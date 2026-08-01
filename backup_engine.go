package main

// This file owns SQL import/export and full-database backup operations.
// Keeping transfer code here lets the HTTP server remain focused on routing and UI APIs.

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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

func (s *server) importSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, 400, "导入文件无效")
		return
	}
	schema := strings.TrimSpace(r.FormValue("schema"))
	compatibilityMode := r.FormValue("compatibilityMode") == "1"
	if schema == "" {
		writeError(w, 400, "请选择导入目标数据库")
		return
	}
	var schemaErr error
	schema, schemaErr = requiredIdentifier(schema, "数据库")
	if schemaErr != nil {
		writeError(w, 400, schemaErr.Error())
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, 400, "请选择 .sql 或 .sql.gz 文件")
		return
	}
	defer file.Close()
	reader := io.Reader(file)
	var gzipReader *gzip.Reader
	if strings.HasSuffix(strings.ToLower(header.Filename), ".sql.gz") || strings.HasSuffix(strings.ToLower(header.Filename), ".gz") {
		gzipReader, err = gzip.NewReader(reader)
		if err != nil {
			writeError(w, 400, "无法解压 .sql.gz 文件: "+err.Error())
			return
		}
		defer gzipReader.Close()
		reader = gzipReader
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	conn, err := db.Conn(r.Context())
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(r.Context(), "USE "+quoteIdentifier(schema)); err != nil {
		writeDBError(w, err)
		return
	}
	if compatibilityMode {
		restore, err := enableImportCompatibilityMode(r.Context(), conn)
		if err != nil {
			writeDBError(w, err)
			return
		}
		defer restore()
		log.Printf("import compatibility mode enabled: file=%s", header.Filename)
	}
	if err := executeSQLScript(r.Context(), conn, reader, schema); err != nil {
		writeError(w, 500, "导入失败（MySQL 的建库、建表等 DDL 会立即生效，不能自动整体回滚）: "+err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"message": "SQL 已执行完成"})
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

func executeSQLScript(ctx context.Context, executor sqlStatementExecutor, reader io.Reader, targetSchema string) error {
	parser := &sqlScriptParser{ctx: ctx, executor: executor, targetSchema: targetSchema, delimiter: ";"}
	buffered := bufio.NewReaderSize(reader, 64*1024)
	for {
		line, err := buffered.ReadString('\n')
		if len(line) > 0 {
			if parser.statement.Len() == 0 {
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) == 2 && strings.EqualFold(fields[0], "DELIMITER") && len(fields[1]) <= 16 && !strings.ContainsAny(fields[1], " \t\r\n") {
					parser.delimiter = fields[1]
					continue
				}
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
	return parser.execute(false)
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
	filename := schema + "_" + table + ".sql"
	w.Header().Set("Content-Type", "application/sql; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(filename))
	_, _ = fmt.Fprintf(w, "-- Exported by MySQL Manager at %s\n\n", time.Now().Format(time.RFC3339))
	var ignored, createSQL string
	if err := db.QueryRow("SHOW CREATE TABLE "+qualify(schema, table)).Scan(&ignored, &createSQL); err != nil {
		writeDBError(w, err)
		return
	}
	_, _ = fmt.Fprintf(w, "DROP TABLE IF EXISTS %s;\n%s;\n\n", quoteIdentifier(table), createSQL)
	if !includeData {
		return
	}
	rows, err := db.Query("SELECT * FROM "+qualify(schema, table)+filters, args...)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	colNames, err := rows.Columns()
	if err != nil {
		writeDBError(w, err)
		return
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		writeDBError(w, err)
		return
	}
	quotedNames := make([]string, len(colNames))
	jsonColumns := make([]bool, len(colNames))
	for i, name := range colNames {
		quotedNames[i] = quoteIdentifier(name)
		jsonColumns[i] = strings.EqualFold(columnTypes[i].DatabaseTypeName(), "JSON")
	}
	for rows.Next() {
		values := make([]any, len(colNames))
		refs := make([]any, len(colNames))
		for i := range values {
			refs[i] = &values[i]
		}
		if err := rows.Scan(refs...); err != nil {
			writeDBError(w, err)
			return
		}
		literals := make([]string, len(values))
		for i, value := range values {
			literals[i] = sqlLiteralForColumn(value, jsonColumns[i])
		}
		_, _ = fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n", quoteIdentifier(table), strings.Join(quotedNames, ","), strings.Join(literals, ","))
	}
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
	file, err := os.Open(path)
	if err != nil {
		writeError(w, http.StatusNotFound, "备份文件不可用: "+err.Error())
		return
	}
	defer file.Close()
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("failed to remove temporary backup file %s: %v", path, err)
		}
		if err := os.Remove(filepath.Dir(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("failed to remove temporary backup directory %s: %v", filepath.Dir(path), err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法读取备份文件: "+err.Error())
		return
	}
	filename := filepath.Base(path)
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(filename))
	http.ServeContent(w, r, filename, info.ModTime(), file)
}

func (s *server) runBackup(job *backupJob, db *sql.DB) {
	directory, err := backupDirectory()
	if err != nil {
		s.finishBackup(job, false, "无法创建备份目录: "+err.Error(), "")
		return
	}
	stamp := job.StartedAt.Format("20060102-150405")
	finalPath := filepath.Join(directory, job.Schema+"-"+stamp+".sql.gz")
	partPath := finalPath + ".part"
	file, err := os.Create(partPath)
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
		_ = os.Remove(partPath)
		s.finishBackup(job, false, "数据库导出失败: "+err.Error(), "")
		return
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		_ = os.Remove(partPath)
		s.finishBackup(job, false, "备份文件保存失败: "+err.Error(), "")
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
