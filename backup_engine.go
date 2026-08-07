package main

// This file owns SQL import/export and full-database backup operations.
// Keeping transfer code here lets the HTTP server remain focused on routing and UI APIs.

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type backupRequest struct {
	Schema string `json:"schema"`
}

type backupJob struct {
	mu         sync.RWMutex
	ID         string
	Schema     string
	StartedAt  time.Time
	Finished   bool
	Success    bool
	Message    string
	Path       string
	TablesDone int
	TableTotal int
	Bytes      int64
}

// importJob stores a durable, cancellable SQL import running in the background.
// Compressed files are streamed directly into the parser; their progress is an
// estimate based on compressed bytes read, so no expanded SQL file is created.
type importJob struct {
	mu                 sync.RWMutex
	ID                 string
	Schema             string
	Filename           string
	InputPath          string
	CompatibilityMode  bool
	StartedAt          time.Time
	Finished           bool
	Success            bool
	Cancelled          bool
	Stage              string
	Message            string
	ProcessedBytes     int64
	TotalBytes         int64
	ProgressEstimated  bool
	StatementsExecuted int64
	cancel             context.CancelFunc
}

type importProgressReader struct {
	reader io.Reader
	job    *importJob
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(p)
	}
}

func (r *importProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.job.mu.Lock()
		r.job.ProcessedBytes += int64(n)
		if r.job.TotalBytes > 0 {
			label := "已处理"
			if r.job.ProgressEstimated {
				label = "已读取压缩包"
			}
			r.job.Message = fmt.Sprintf("正在导入 SQL… %s %s / %s", label, formatImportBytes(r.job.ProcessedBytes), formatImportBytes(r.job.TotalBytes))
		}
		r.job.mu.Unlock()
	}
	return n, err
}

const (
	fastImportBatchStatements = 500
	fastImportBatchBytes      = 4 << 20
)

type importBatchFlusher interface {
	Flush() error
}

// fastImportExecutor batches only ordinary data-changing statements. DDL,
// explicit transactions, and all other SQL stay on the original sequential
// path so import semantics remain predictable.
type fastImportExecutor struct {
	ctx        context.Context
	conn       *sql.Conn
	job        *importJob
	statements []string
	bytes      int
	inScriptTx bool
}

func newFastImportExecutor(ctx context.Context, conn *sql.Conn, job *importJob) *fastImportExecutor {
	return &fastImportExecutor{ctx: ctx, conn: conn, job: job, statements: make([]string, 0, fastImportBatchStatements)}
}

func (e *fastImportExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if e.inScriptTx || !isFastImportStatement(query) {
		if err := e.Flush(); err != nil {
			return nil, err
		}
		result, err := e.conn.ExecContext(ctx, query, args...)
		if err == nil {
			recordImportedStatements(e.job, 1)
		}
		if isImportTransactionStart(query) {
			e.inScriptTx = true
		} else if isImportTransactionEnd(query) {
			e.inScriptTx = false
		}
		return result, err
	}
	e.statements = append(e.statements, query)
	e.bytes += len(query)
	if len(e.statements) >= fastImportBatchStatements || e.bytes >= fastImportBatchBytes {
		return nil, e.Flush()
	}
	return nil, nil
}

func (e *fastImportExecutor) Flush() error {
	if len(e.statements) == 0 {
		return nil
	}
	var batch strings.Builder
	batch.Grow(e.bytes + 48)
	batch.WriteString("START TRANSACTION;\n")
	for _, statement := range e.statements {
		batch.WriteString(statement)
		batch.WriteString(";\n")
	}
	batch.WriteString("COMMIT")
	if _, err := e.conn.ExecContext(e.ctx, batch.String()); err != nil {
		return fmt.Errorf("执行快速导入批次失败: %w", err)
	}
	recordImportedStatements(e.job, int64(len(e.statements)))
	e.statements = e.statements[:0]
	e.bytes = 0
	return nil
}

func recordImportedStatements(job *importJob, count int64) {
	job.mu.Lock()
	job.StatementsExecuted += count
	job.Message = fmt.Sprintf("正在执行 SQL… 已完成 %d 条语句（快速批量导入）", job.StatementsExecuted)
	job.mu.Unlock()
}

func importStatementFirstWord(statement string) string {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return ""
	}
	if index := strings.IndexAny(statement, " \t\r\n"); index >= 0 {
		return strings.ToUpper(statement[:index])
	}
	return strings.ToUpper(statement)
}

func isFastImportStatement(statement string) bool {
	switch importStatementFirstWord(statement) {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	default:
		return false
	}
}

func isImportTransactionStart(statement string) bool {
	word := importStatementFirstWord(statement)
	return word == "BEGIN" || word == "START"
}

func isImportTransactionEnd(statement string) bool {
	word := importStatementFirstWord(statement)
	return word == "COMMIT" || word == "ROLLBACK"
}

func formatImportBytes(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%d B", value)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	amount := float64(value) / 1024
	unit := 0
	for amount >= 1024 && unit < len(units)-1 {
		amount /= 1024
		unit++
	}
	return fmt.Sprintf("%.2f %s", amount, units[unit])
}

func (s *server) importSQL(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.startImport(w, r)
	case http.MethodGet:
		if r.URL.Query().Get("events") == "1" {
			s.streamImportProgress(w, r)
			return
		}
		s.importStatus(w, r)
	case http.MethodDelete:
		s.cancelImport(w, r)
	default:
		methodNotAllowed(w)
	}
}

func (s *server) startImport(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "导入文件无效")
		return
	}
	schema, err := requiredIdentifier(strings.TrimSpace(r.FormValue("schema")), "数据库")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "请选择 .sql 或 .sql.gz 文件")
		return
	}
	defer file.Close()
	lowerName := strings.ToLower(header.Filename)
	if !strings.HasSuffix(lowerName, ".sql") && !strings.HasSuffix(lowerName, ".sql.gz") && !strings.HasSuffix(lowerName, ".gz") {
		writeError(w, http.StatusBadRequest, "仅支持 .sql 或 .sql.gz 文件")
		return
	}
	if _, err := s.currentDB(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	input, err := os.CreateTemp("", "mysql-manage-import-*.upload")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法保存导入文件: "+err.Error())
		return
	}
	inputPath := input.Name()
	copied, copyErr := io.Copy(input, file)
	closeErr := input.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(inputPath)
		if copyErr != nil {
			writeError(w, http.StatusInternalServerError, "保存导入文件失败: "+copyErr.Error())
		} else {
			writeError(w, http.StatusInternalServerError, "保存导入文件失败: "+closeErr.Error())
		}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &importJob{
		ID:                fmt.Sprintf("import-%d", time.Now().UnixNano()),
		Schema:            schema,
		Filename:          header.Filename,
		InputPath:         inputPath,
		CompatibilityMode: r.FormValue("compatibilityMode") == "1",
		StartedAt:         time.Now(),
		Stage:             "queued",
		Message:           fmt.Sprintf("文件已接收（%s），等待导入…", formatImportBytes(copied)),
		cancel:            cancel,
	}
	s.importMu.Lock()
	s.importJobs[job.ID] = job
	s.importMu.Unlock()
	go s.runImport(ctx, job)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": job.ID})
}

func (s *server) importJob(id string) *importJob {
	s.importMu.RLock()
	job := s.importJobs[id]
	s.importMu.RUnlock()
	return job
}

func importJobSnapshot(job *importJob) map[string]any {
	job.mu.RLock()
	defer job.mu.RUnlock()
	return map[string]any{
		"id": job.ID, "schema": job.Schema, "filename": job.Filename,
		"finished": job.Finished, "success": job.Success, "cancelled": job.Cancelled,
		"stage": job.Stage, "message": job.Message, "processedBytes": job.ProcessedBytes,
		"totalBytes": job.TotalBytes, "progressEstimated": job.ProgressEstimated, "statementsExecuted": job.StatementsExecuted,
	}
}

func (s *server) importStatus(w http.ResponseWriter, r *http.Request) {
	job := s.importJob(strings.TrimSpace(r.URL.Query().Get("id")))
	if job == nil {
		writeError(w, http.StatusNotFound, "找不到导入任务")
		return
	}
	writeJSON(w, http.StatusOK, importJobSnapshot(job))
}

func (s *server) streamImportProgress(w http.ResponseWriter, r *http.Request) {
	job := s.importJob(strings.TrimSpace(r.URL.Query().Get("id")))
	if job == nil {
		writeError(w, http.StatusNotFound, "找不到导入任务")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "当前服务不支持实时进度")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(350 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := importJobSnapshot(job)
		payload, _ := json.Marshal(snapshot)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		flusher.Flush()
		if finished, _ := snapshot["finished"].(bool); finished {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *server) cancelImport(w http.ResponseWriter, r *http.Request) {
	job := s.importJob(strings.TrimSpace(r.URL.Query().Get("id")))
	if job == nil {
		writeError(w, http.StatusNotFound, "找不到导入任务")
		return
	}
	job.mu.Lock()
	if job.Finished {
		job.mu.Unlock()
		writeError(w, http.StatusConflict, "导入任务已经结束")
		return
	}
	job.Stage, job.Message = "cancelling", "正在取消导入…"
	cancel := job.cancel
	job.mu.Unlock()
	cancel()
	writeJSON(w, http.StatusAccepted, map[string]string{"message": "正在取消导入"})
}

func (s *server) runImport(ctx context.Context, job *importJob) {
	defer func() { _ = os.Remove(job.InputPath) }()
	input, err := os.Open(job.InputPath)
	if err != nil {
		s.finishImport(job, false, false, "无法打开导入文件: "+err.Error())
		return
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		s.finishImport(job, false, false, "无法读取导入文件: "+err.Error())
		return
	}
	compressed := strings.HasSuffix(strings.ToLower(job.Filename), ".sql.gz") || strings.HasSuffix(strings.ToLower(job.Filename), ".gz")
	job.mu.Lock()
	job.Stage, job.Message, job.TotalBytes, job.ProcessedBytes, job.ProgressEstimated = "importing", "正在连接 MySQL…", info.Size(), 0, compressed
	job.mu.Unlock()
	reader := io.Reader(&importProgressReader{reader: input, job: job})
	if compressed {
		gzipReader, gzipErr := gzip.NewReader(reader)
		if gzipErr != nil {
			s.finishImport(job, false, false, "无法解压 .sql.gz 文件: "+gzipErr.Error())
			return
		}
		defer gzipReader.Close()
		reader = gzipReader
	}
	db, err := s.currentDB()
	if err != nil {
		s.finishImport(job, false, false, err.Error())
		return
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		s.finishImport(job, false, ctx.Err() != nil, "无法连接 MySQL: "+err.Error())
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(job.Schema)); err != nil {
		s.finishImport(job, false, ctx.Err() != nil, "无法选择目标数据库: "+err.Error())
		return
	}
	if job.CompatibilityMode {
		restore, modeErr := enableImportCompatibilityMode(ctx, conn)
		if modeErr != nil {
			s.finishImport(job, false, ctx.Err() != nil, "无法启用兼容导入: "+modeErr.Error())
			return
		}
		defer restore()
	}
	err = executeSQLScript(ctx, newFastImportExecutor(ctx, conn, job), &contextReader{ctx: ctx, reader: reader}, job.Schema)
	if err != nil {
		s.finishImport(job, false, ctx.Err() != nil, "导入失败（MySQL 的建库、建表等 DDL 会立即生效，不能自动整体回滚）: "+err.Error())
		return
	}
	s.clearMetadataCache()
	s.finishImport(job, true, false, "导入完成")
}

func (s *server) finishImport(job *importJob, success, cancelled bool, message string) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.Finished, job.Success, job.Cancelled = true, success, cancelled
	if cancelled {
		job.Stage, job.Message = "cancelled", "导入已取消"
	} else if success {
		job.Stage, job.Message, job.ProcessedBytes = "completed", message, job.TotalBytes
	} else {
		job.Stage, job.Message = "failed", message
	}
}

func enableImportCompatibilityMode(ctx context.Context, conn *sql.Conn) (func(), error) {
	var original string
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&original); err != nil {
		return nil, err
	}
	parts := strings.Split(original, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		mode := strings.TrimSpace(part)
		if strings.EqualFold(mode, "STRICT_TRANS_TABLES") || strings.EqualFold(mode, "STRICT_ALL_TABLES") {
			continue
		}
		filtered = append(filtered, mode)
	}
	compatibilityMode := strings.Join(filtered, ",")
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ?", compatibilityMode); err != nil {
		return nil, err
	}
	return func() {
		if _, err := conn.ExecContext(context.Background(), "SET SESSION sql_mode = ?", original); err != nil {
			log.Printf("failed to restore SQL mode after import: %v", err)
		}
	}, nil
}

type sqlScriptParser struct {
	ctx           context.Context
	executor      sqlStatementExecutor
	targetSchema  string
	sourceSchema  string
	delimiter     string
	statement     strings.Builder
	quote, closed byte
	escaped       bool
	lineComment   bool
	blockComment  bool
	previous      byte
}

type sqlStatementExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (p *sqlScriptParser) canChangeDelimiter() bool {
	return p.quote == 0 && p.closed == 0 && !p.escaped && !p.lineComment && !p.blockComment && sqlScriptTriviaOnly(p.statement.String())
}

func (p *sqlScriptParser) resetStatement() {
	p.statement.Reset()
	p.quote, p.closed, p.escaped, p.lineComment, p.blockComment, p.previous = 0, 0, false, false, false, 0
}

// sqlScriptTriviaOnly reports whether text contains only whitespace and ordinary
// comments. MySQL executable comments (/*! ... */) are deliberately retained.
func sqlScriptTriviaOnly(text string) bool {
	for index := 0; index < len(text); {
		if text[index] == ' ' || text[index] == '\t' || text[index] == '\r' || text[index] == '\n' {
			index++
			continue
		}
		if text[index] == '#' || (text[index] == '-' && index+1 < len(text) && text[index+1] == '-') {
			if next := strings.IndexByte(text[index:], '\n'); next >= 0 {
				index += next + 1
				continue
			}
			return true
		}
		if text[index] == '/' && index+1 < len(text) && text[index+1] == '*' {
			if index+2 < len(text) && text[index+2] == '!' {
				return false
			}
			next := strings.Index(text[index+2:], "*/")
			if next < 0 {
				return false
			}
			index += next + 4
			continue
		}
		return false
	}
	return true
}

func sqlDelimiterDirective(line string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "DELIMITER") || len(fields[1]) > 16 || strings.ContainsAny(fields[1], " \t\r\n") {
		return "", false
	}
	return fields[1], true
}

func executeSQLScript(ctx context.Context, executor sqlStatementExecutor, reader io.Reader, targetSchema string) error {
	parser := &sqlScriptParser{ctx: ctx, executor: executor, targetSchema: targetSchema, delimiter: ";"}
	buffered := bufio.NewReaderSize(reader, 64*1024)
	for {
		line, err := buffered.ReadString('\n')
		if len(line) > 0 {
			if delimiter, ok := sqlDelimiterDirective(line); ok && parser.canChangeDelimiter() {
				parser.resetStatement()
				parser.delimiter = delimiter
				continue
			}
			if err := parser.write(line); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if err := parser.execute(false); err != nil {
		return err
	}
	if flusher, ok := executor.(importBatchFlusher); ok {
		return flusher.Flush()
	}
	return nil
}

func (p *sqlScriptParser) write(text string) error {
	for i := 0; i < len(text); i++ {
		char := text[i]
		p.statement.WriteByte(char)
		if p.lineComment {
			if char == '\n' {
				p.lineComment = false
			}
			p.previous = char
			continue
		}
		if p.blockComment {
			if p.previous == '*' && char == '/' {
				p.blockComment = false
			}
			p.previous = char
			continue
		}
		if p.quote != 0 {
			if p.escaped {
				p.escaped = false
			} else if char == '\\' && p.quote != '`' {
				p.escaped = true
			} else if char == p.quote {
				p.closed, p.quote = char, 0
			}
			p.previous = char
			continue
		}
		if p.closed != 0 {
			if char == p.closed {
				p.quote = char
				p.closed = 0
				p.previous = char
				continue
			}
			p.closed = 0
		}
		if char == '\'' || char == '"' || char == '`' {
			p.quote, p.previous = char, char
			continue
		}
		if char == '#' || (p.previous == '-' && char == '-') {
			p.lineComment = true
			p.previous = char
			continue
		}
		if p.previous == '/' && char == '*' {
			p.blockComment = true
			p.previous = char
			continue
		}
		p.previous = char
		if strings.HasSuffix(p.statement.String(), p.delimiter) {
			if err := p.execute(true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *sqlScriptParser) execute(hasDelimiter bool) error {
	statement := p.statement.String()
	if hasDelimiter {
		statement = strings.TrimSuffix(statement, p.delimiter)
	}
	statement = strings.TrimSpace(statement)
	p.statement.Reset()
	p.quote, p.closed, p.escaped, p.lineComment, p.blockComment, p.previous = 0, 0, false, false, false, 0
	if statement == "" {
		return nil
	}
	if importDatabaseStatement.MatchString(statement) {
		return nil
	}
	if matches := importUseStatement.FindStringSubmatch(statement); matches != nil {
		sourceSchema := matches[1]
		if sourceSchema == "" {
			sourceSchema = matches[2]
		}
		p.sourceSchema = strings.ReplaceAll(sourceSchema, "``", "`")
		return nil
	}
	if p.targetSchema != "" && p.sourceSchema != "" && !strings.EqualFold(p.sourceSchema, p.targetSchema) {
		statement = strings.ReplaceAll(statement, quoteIdentifier(p.sourceSchema)+".", quoteIdentifier(p.targetSchema)+".")
	}
	_, err := p.executor.ExecContext(p.ctx, statement)
	if err != nil {
		return fmt.Errorf("执行 %s 失败: %w", importStatementLabel(statement), err)
	}
	return nil
}

func importStatementLabel(statement string) string {
	fields := strings.Fields(statement)
	if len(fields) == 0 {
		return "空语句"
	}
	limit := 3
	if len(fields) < limit {
		limit = len(fields)
	}
	return strings.Join(fields[:limit], " ")
}
func (s *server) exportSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, table, meta, err := s.requestMeta(r)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	filters, args, err := buildFilters(r.URL.Query(), meta)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	includeData := r.URL.Query().Get("data") != "0"
	tx, err := db.BeginTx(r.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		writeDBError(w, err)
		return
	}
	transactionDone := false
	defer func() {
		if !transactionDone {
			_ = tx.Rollback()
		}
	}()
	var tableType string
	if err := tx.QueryRow(`SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, schema, table).Scan(&tableType); err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "找不到指定的数据表")
		} else {
			writeDBError(w, err)
		}
		return
	}
	if tableType != "BASE TABLE" {
		writeError(w, http.StatusBadRequest, "逻辑导出仅支持基础表")
		return
	}
	filename := table + "_" + time.Now().Format("20060102_150405") + ".sql"
	file, path, err := createExportFile(filename)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法创建导出文件: "+err.Error())
		return
	}
	completed := false
	defer func() {
		if !completed {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	var ignored, createSQL string
	if err := tx.QueryRow("SHOW CREATE TABLE "+qualify(schema, table)).Scan(&ignored, &createSQL); err != nil {
		writeDBError(w, err)
		return
	}
	if _, err := fmt.Fprintf(file, "-- Exported by MySQL Manager at %s\n-- Native logical export: structure from SHOW CREATE TABLE, data in 1000-row pages\n\n/*!40101 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n\nDROP TABLE IF EXISTS %s;\n%s;\n\n", time.Now().Format(time.RFC3339), quoteIdentifier(table), strings.TrimSuffix(createSQL, ";")); err != nil {
		writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
		return
	}
	if includeData {
		const batchSize = 1000
		var offset int64
		orderBy := ""
		if len(meta.PrimaryKeys) > 0 {
			keys := make([]string, len(meta.PrimaryKeys))
			for i, key := range meta.PrimaryKeys {
				keys[i] = quoteIdentifier(key) + " ASC"
			}
			orderBy = " ORDER BY " + strings.Join(keys, ",")
		}
		var quotedNames []string
		var jsonColumns []bool
		for {
			pageArgs := append([]any{}, args...)
			pageArgs = append(pageArgs, batchSize, offset)
			rows, err := tx.Query("SELECT * FROM "+qualify(schema, table)+filters+orderBy+" LIMIT ? OFFSET ?", pageArgs...)
			if err != nil {
				writeDBError(w, err)
				return
			}
			colNames, err := rows.Columns()
			if err == nil && quotedNames == nil {
				columnTypes, typeErr := rows.ColumnTypes()
				if typeErr != nil {
					err = typeErr
				} else {
					quotedNames, jsonColumns = make([]string, len(colNames)), make([]bool, len(colNames))
					for i, name := range colNames {
						quotedNames[i] = quoteIdentifier(name)
						jsonColumns[i] = strings.EqualFold(columnTypes[i].DatabaseTypeName(), "JSON")
					}
				}
			}
			if err != nil {
				_ = rows.Close()
				writeDBError(w, err)
				return
			}
			batchRows := 0
			for rows.Next() {
				values, refs := make([]any, len(colNames)), make([]any, len(colNames))
				for i := range values {
					refs[i] = &values[i]
				}
				if err := rows.Scan(refs...); err != nil {
					_ = rows.Close()
					writeDBError(w, err)
					return
				}
				literals := make([]string, len(values))
				for i, value := range values {
					literals[i] = sqlLiteralForColumn(value, jsonColumns[i])
				}
				if _, err := fmt.Fprintf(file, "INSERT INTO %s (%s) VALUES (%s);\n", quoteIdentifier(table), strings.Join(quotedNames, ","), strings.Join(literals, ",")); err != nil {
					_ = rows.Close()
					writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
					return
				}
				batchRows++
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				writeDBError(w, err)
				return
			}
			if err := rows.Close(); err != nil {
				writeDBError(w, err)
				return
			}
			if batchRows < batchSize {
				break
			}
			offset += int64(batchRows)
		}
	}
	if _, err := fmt.Fprint(file, "\n/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n/*!40101 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n"); err != nil {
		writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		writeDBError(w, err)
		return
	}
	transactionDone = true
	completed = finishExportFile(w, file, path)
	_ = meta
}

func (s *server) backup(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var request backupRequest
		if err := decodeJSON(r, &request); err != nil {
			writeError(w, 400, "备份请求无效")
			return
		}
		schema, err := requiredIdentifier(request.Schema, "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		db, err := s.currentDB()
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		var tableTotal int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE'`, schema).Scan(&tableTotal); err != nil {
			writeDBError(w, err)
			return
		}
		job := &backupJob{ID: fmt.Sprintf("backup-%d", time.Now().UnixNano()), Schema: schema, StartedAt: time.Now(), Message: "正在创建一致性快照…", TableTotal: tableTotal}
		s.backupMu.Lock()
		s.backupJobs[job.ID] = job
		s.backupMu.Unlock()
		go s.runBackup(job, db)
		writeJSON(w, http.StatusAccepted, map[string]string{"id": job.ID})
	case http.MethodGet:
		if r.URL.Query().Get("download") == "1" {
			s.downloadBackup(w, r)
			return
		}
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		s.backupMu.RLock()
		job := s.backupJobs[id]
		s.backupMu.RUnlock()
		if job == nil {
			writeError(w, 404, "找不到备份任务")
			return
		}
		job.mu.RLock()
		result := map[string]any{"id": job.ID, "schema": job.Schema, "finished": job.Finished, "success": job.Success, "message": job.Message, "path": job.Path, "tablesDone": job.TablesDone, "tableTotal": job.TableTotal, "bytes": job.Bytes}
		job.mu.RUnlock()
		writeJSON(w, 200, result)
	default:
		methodNotAllowed(w)
	}
}

func (s *server) downloadBackup(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	s.backupMu.RLock()
	job := s.backupJobs[id]
	s.backupMu.RUnlock()
	if job == nil {
		writeError(w, http.StatusNotFound, "找不到备份任务")
		return
	}
	job.mu.RLock()
	finished, success, path := job.Finished, job.Success, job.Path
	job.mu.RUnlock()
	if !finished || !success || path == "" {
		writeError(w, http.StatusConflict, "备份文件尚未准备完成")
		return
	}
	if _, err := os.Stat(path); err != nil {
		writeError(w, http.StatusNotFound, "备份文件不可用: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "备份完成", "path": path, "filename": filepath.Base(path)})
}

func (s *server) runBackup(job *backupJob, db *sql.DB) {
	stamp := job.StartedAt.Format("20060102-150405")
	file, finalPath, err := createExportFile(job.Schema + "-" + stamp + ".sql.gz")
	if err != nil {
		s.finishBackup(job, false, "无法创建备份文件: "+err.Error(), "")
		return
	}
	gzipWriter := gzip.NewWriter(file)
	writer := bufio.NewWriterSize(gzipWriter, 128*1024)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err == nil {
		err = dumpDatabaseSQL(tx, job, writer)
	}
	if err == nil {
		err = tx.Commit()
	} else if tx != nil {
		_ = tx.Rollback()
	}
	flushErr := writer.Flush()
	closeErr := gzipWriter.Close()
	fileErr := file.Close()
	if err == nil {
		err = flushErr
	}
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = fileErr
	}
	if err != nil {
		_ = os.Remove(finalPath)
		s.finishBackup(job, false, "数据库导出失败: "+err.Error(), "")
		return
	}
	info, _ := os.Stat(finalPath)
	if info != nil {
		job.mu.Lock()
		job.Bytes = info.Size()
		job.mu.Unlock()
	}
	s.finishBackup(job, true, "备份完成", finalPath)
}

type databaseObject struct{ Name, Type string }

func dumpDatabaseSQL(tx *sql.Tx, job *backupJob, writer *bufio.Writer) error {
	if _, err := fmt.Fprintf(writer, "-- Exported by MySQL Manager at %s\n-- Native Go streaming backup\n\n", time.Now().Format(time.RFC3339)); err != nil {
		return err
	}
	createDatabase, err := showCreateSQL(tx, "SHOW CREATE DATABASE "+quoteIdentifier(job.Schema))
	if err != nil {
		return err
	}
	createDatabase = createDatabaseIfNotExists(createDatabase)
	if _, err := fmt.Fprintf(writer, "%s;\nUSE %s;\n\n", strings.TrimSuffix(createDatabase, ";"), quoteIdentifier(job.Schema)); err != nil {
		return err
	}
	rows, err := tx.Query(`SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_TYPE DESC, TABLE_NAME`, job.Schema)
	if err != nil {
		return err
	}
	objects := make([]databaseObject, 0)
	for rows.Next() {
		var object databaseObject
		if err := rows.Scan(&object.Name, &object.Type); err != nil {
			rows.Close()
			return err
		}
		objects = append(objects, object)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, object := range objects {
		if object.Type != "BASE TABLE" {
			continue
		}
		job.mu.Lock()
		job.Message = fmt.Sprintf("正在导出第 %d/%d 张表：%s", job.TablesDone+1, job.TableTotal, object.Name)
		job.mu.Unlock()
		if err := dumpTableSQL(tx, job.Schema, object.Name, writer); err != nil {
			return err
		}
		job.mu.Lock()
		job.TablesDone++
		job.mu.Unlock()
	}
	for _, object := range objects {
		if object.Type != "VIEW" {
			continue
		}
		createSQL, err := showCreateSQL(tx, "SHOW CREATE TABLE "+qualify(job.Schema, object.Name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "DROP VIEW IF EXISTS %s;\n%s;\n\n", qualify(job.Schema, object.Name), strings.TrimSuffix(createSQL, ";")); err != nil {
			return err
		}
	}
	return dumpDatabaseProgrammables(tx, job.Schema, writer)
}

func createDatabaseIfNotExists(createSQL string) string {
	statement := strings.TrimSpace(createSQL)
	const prefix = "CREATE DATABASE"
	if strings.HasPrefix(strings.ToUpper(statement), prefix) {
		rest := strings.TrimSpace(statement[len(prefix):])
		if !strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
			return prefix + " IF NOT EXISTS " + rest
		}
	}
	return statement
}

func dumpTableSQL(tx *sql.Tx, schema, table string, writer *bufio.Writer) error {
	createSQL, err := showCreateSQL(tx, "SHOW CREATE TABLE "+qualify(schema, table))
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "DROP TABLE IF EXISTS %s;\n%s;\n", qualify(schema, table), strings.TrimSuffix(createSQL, ";")); err != nil {
		return err
	}
	rows, err := tx.Query("SELECT * FROM " + qualify(schema, table))
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return err
	}
	quoted := make([]string, len(columns))
	jsonColumns := make([]bool, len(columns))
	for index, column := range columns {
		quoted[index] = quoteIdentifier(column)
		jsonColumns[index] = strings.EqualFold(columnTypes[index].DatabaseTypeName(), "JSON")
	}
	for rows.Next() {
		values, refs := make([]any, len(columns)), make([]any, len(columns))
		for index := range values {
			refs[index] = &values[index]
		}
		if err := rows.Scan(refs...); err != nil {
			rows.Close()
			return err
		}
		literals := make([]string, len(values))
		for index, value := range values {
			literals[index] = sqlLiteralForColumn(value, jsonColumns[index])
		}
		if _, err := fmt.Fprintf(writer, "INSERT INTO %s (%s) VALUES (%s);\n", quoteIdentifier(table), strings.Join(quoted, ","), strings.Join(literals, ",")); err != nil {
			rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	_, err = writer.WriteString("\n")
	return err
}

func showCreateSQL(tx *sql.Tx, query string) (string, error) {
	rows, err := tx.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if !rows.Next() {
		return "", errors.New("未返回建表语句")
	}
	values, refs := make([]any, len(columns)), make([]any, len(columns))
	for index := range values {
		refs[index] = &values[index]
	}
	if err := rows.Scan(refs...); err != nil {
		return "", err
	}
	for index, name := range columns {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "create") || strings.Contains(lower, "statement") {
			return fmt.Sprint(jsonValue(values[index])), nil
		}
	}
	return "", errors.New("无法读取创建语句")
}

func dumpDatabaseProgrammables(tx *sql.Tx, schema string, writer *bufio.Writer) error {
	routines, err := tx.Query(`SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=? ORDER BY ROUTINE_TYPE, ROUTINE_NAME`, schema)
	if err != nil {
		return err
	}
	routineObjects := make([]databaseObject, 0)
	for routines.Next() {
		var object databaseObject
		if err := routines.Scan(&object.Name, &object.Type); err != nil {
			routines.Close()
			return err
		}
		routineObjects = append(routineObjects, object)
	}
	if err := routines.Close(); err != nil {
		return err
	}
	for _, object := range routineObjects {
		createSQL, err := showCreateSQL(tx, "SHOW CREATE "+object.Type+" "+qualify(schema, object.Name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "DROP %s IF EXISTS %s;\nDELIMITER ;;\n%s;;\nDELIMITER ;\n\n", object.Type, qualify(schema, object.Name), strings.TrimSuffix(createSQL, ";")); err != nil {
			return err
		}
	}
	triggers, err := tx.Query(`SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? ORDER BY TRIGGER_NAME`, schema)
	if err != nil {
		return err
	}
	triggerNames := make([]string, 0)
	for triggers.Next() {
		var name string
		if err := triggers.Scan(&name); err != nil {
			triggers.Close()
			return err
		}
		triggerNames = append(triggerNames, name)
	}
	if err := triggers.Close(); err != nil {
		return err
	}
	for _, name := range triggerNames {
		createSQL, err := showCreateSQL(tx, "SHOW CREATE TRIGGER "+qualify(schema, name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "DROP TRIGGER IF EXISTS %s;\nDELIMITER ;;\n%s;;\nDELIMITER ;\n\n", qualify(schema, name), strings.TrimSuffix(createSQL, ";")); err != nil {
			return err
		}
	}
	events, err := tx.Query(`SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA=? ORDER BY EVENT_NAME`, schema)
	if err != nil {
		return err
	}
	eventNames := make([]string, 0)
	for events.Next() {
		var name string
		if err := events.Scan(&name); err != nil {
			events.Close()
			return err
		}
		eventNames = append(eventNames, name)
	}
	if err := events.Close(); err != nil {
		return err
	}
	for _, name := range eventNames {
		createSQL, err := showCreateSQL(tx, "SHOW CREATE EVENT "+qualify(schema, name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "DROP EVENT IF EXISTS %s;\nDELIMITER ;;\n%s;;\nDELIMITER ;\n\n", qualify(schema, name), strings.TrimSuffix(createSQL, ";")); err != nil {
			return err
		}
	}
	return nil
}

func backupDirectory() (string, error) {
	return os.MkdirTemp("", "mysql-manage-backup-")
}

func (s *server) finishBackup(job *backupJob, success bool, message, path string) {
	job.mu.Lock()
	job.Finished, job.Success, job.Message, job.Path = true, success, message, path
	if success {
		job.TablesDone = job.TableTotal
	}
	schema := job.Schema
	job.mu.Unlock()
	if success {
		log.Printf("backup completed: schema=%s path=%s", schema, path)
	} else {
		log.Printf("backup failed: schema=%s error=%s", schema, message)
	}
}
