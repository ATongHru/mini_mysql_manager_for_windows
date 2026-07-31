package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed web/index.html
var webFiles embed.FS

var defaultPassword = ""
const noDefaultPasswordMarker = "__MYSQL_MANAGER_NO_DEFAULT_PASSWORD__"

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)
var sqlCompletionTail = regexp.MustCompile("(?i)([`A-Za-z0-9_$]+)\\s*\\.\\s*([A-Za-z0-9_$]*)$")
var sqlTableReference = regexp.MustCompile("(?is)\\b(?:from|join)\\s+((?:`[^`]+`|[A-Za-z0-9_$]+)(?:\\s*\\.\\s*(?:`[^`]+`|[A-Za-z0-9_$]+))?)(?:\\s+(?:as\\s+)?([A-Za-z_][A-Za-z0-9_$]*))?")
var sqlWordTail = regexp.MustCompile("(?i)([A-Za-z_][A-Za-z0-9_$]*)$")

type server struct {
	mu              sync.RWMutex
	db              *sql.DB
	host            string
	port            string
	user            string
	passwordDefault string
}

type column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Primary  bool   `json:"primary"`
}

type tableMeta struct {
	Columns     []column
	PrimaryKeys []string
}

type rowRequest struct {
	Schema     string         `json:"schema"`
	Table      string         `json:"table"`
	PrimaryKey map[string]any `json:"primaryKey"`
	Data       map[string]any `json:"data"`
}

type connectionRequest struct {
	Host string `json:"host"`
	Port string `json:"port"`
	User string `json:"user"`
	Password string `json:"password"`
}

type sqlRequest struct {
	Schema string `json:"schema"`
	SQL string `json:"sql"`
	Limit int `json:"limit"`
	Offset int `json:"offset"`
}

type sqlCompletionRequest struct {
	Schema string `json:"schema"`
	SQL string `json:"sql"`
}

type completionSuggestion struct {
	Value string `json:"value"`
	Kind string `json:"kind"`
}

func main() {
	addr := flag.String("addr", env("MYSQL_MANAGER_ADDR", "127.0.0.1:8080"), "web listen address")
	host := flag.String("mysql-host", env("MYSQL_HOST", ""), "initial MySQL host")
	port := flag.String("mysql-port", env("MYSQL_PORT", "3306"), "initial MySQL port")
	user := flag.String("mysql-user", env("MYSQL_USER", "root"), "initial MySQL user")
	password := flag.String("mysql-password", env("MYSQL_PASSWORD", defaultPassword), "initial MySQL password")
	openBrowser := flag.Bool("open-browser", true, "open the local web page after startup")
	flag.Parse()
	if *password == noDefaultPasswordMarker { *password = "" }

	s := &server{host: *host, port: *port, user: *user, passwordDefault: *password}
	if strings.TrimSpace(*host) != "" {
		if db, err := openDatabase(*host, *port, *user, *password); err != nil {
			log.Printf("initial MySQL connection unavailable: %v", err)
		} else {
			s.db = db
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.health)
	mux.HandleFunc("/api/connection", s.connection)
	mux.HandleFunc("/api/connect", s.connect)
	mux.HandleFunc("/api/sql", s.executeSQL)
	mux.HandleFunc("/api/sql-complete", s.sqlComplete)
	mux.HandleFunc("/api/databases", s.databases)
	mux.HandleFunc("/api/tables", s.tables)
	mux.HandleFunc("/api/rows", s.rows)
	mux.HandleFunc("/api/row", s.row)
	mux.HandleFunc("/api/import", s.importSQL)
	mux.HandleFunc("/api/export", s.exportSQL)
	mux.HandleFunc("/", s.index)
	listener, err := net.Listen("tcp", *addr)
	if err != nil { log.Fatalf("cannot listen on %s: %v", *addr, err) }
	localURL := "http://" + *addr
	log.Printf("MySQL Manager is running at %s", localURL)
	log.Printf("Close this console window to stop the local web service.")
	if *openBrowser { go openLocalBrowser(localURL) }
	log.Fatal((&http.Server{Handler: recoverMiddleware(mux)}).Serve(listener))
}

func openLocalBrowser(localURL string) {
	if err := exec.Command("rundll32.exe", "url.dll,FileProtocolHandler", localURL).Start(); err != nil {
		log.Printf("could not open browser automatically: %v", err)
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}
func getenv(key string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSuffix(""+getEnv(key), "\n"), "\r"))
}
func getEnv(key string) string { return os.Getenv(key) }

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("panic: %v", v)
				writeError(w, http.StatusInternalServerError, "服务器内部错误")
			}
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	db, err := s.currentDB()
	if err != nil { writeJSON(w, 200, map[string]bool{"ok": false}); return }
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	writeJSON(w, 200, map[string]bool{"ok": db.PingContext(ctx) == nil})
}

func (s *server) connection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { methodNotAllowed(w); return }
	s.mu.RLock()
	host, port, user, connected := s.host, s.port, s.user, s.db != nil
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]any{"connected": connected, "host": host, "port": port, "user": user, "passwordDefault": s.passwordDefault})
}

func (s *server) connect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { methodNotAllowed(w); return }
	var request connectionRequest
	if err := decodeJSON(r, &request); err != nil { writeError(w, 400, "连接参数无效: "+err.Error()); return }
	request.Host, request.Port, request.User = strings.TrimSpace(request.Host), strings.TrimSpace(request.Port), strings.TrimSpace(request.User)
	if request.Host == "" || strings.ContainsAny(request.Host, "\r\n\x00") { writeError(w, 400, "请输入有效的 MySQL IP 或主机名"); return }
	if request.User == "" || strings.ContainsAny(request.User, "\r\n\x00") { writeError(w, 400, "请输入有效的用户名"); return }
	port, err := strconv.Atoi(request.Port)
	if err != nil || port < 1 || port > 65535 { writeError(w, 400, "端口必须是 1 到 65535 之间的数字"); return }
	db, err := openDatabase(request.Host, request.Port, request.User, request.Password)
	if err != nil { writeError(w, 400, "无法连接 MySQL: "+err.Error()); return }
	s.mu.Lock()
	old := s.db
	s.db, s.host, s.port, s.user = db, request.Host, request.Port, request.User
	s.mu.Unlock()
	if old != nil { _ = old.Close() }
	writeJSON(w, 200, map[string]any{"message": "MySQL 连接成功", "host": request.Host, "port": request.Port, "user": request.User})
}

func openDatabase(host, port, user, password string) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, password, "tcp", net.JoinHostPort(host, port)
	cfg.ParseTime, cfg.MultiStatements, cfg.Loc = true, true, time.Local
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 15*time.Second, 15*time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil { return nil, err }
	db.SetConnMaxLifetime(3*time.Minute)
	db.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil { _ = db.Close(); return nil, err }
	return db, nil
}

func (s *server) currentDB() (*sql.DB, error) {
	s.mu.RLock(); db := s.db; s.mu.RUnlock()
	if db == nil { return nil, errors.New("请先在网页中连接 MySQL") }
	return db, nil
}

func (s *server) databases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			writeDBError(w, err)
			return
		}
		databases = append(databases, name)
	}
	writeJSON(w, 200, map[string]any{"databases": databases})
}
func (s *server) tables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	rows, err := db.Query(`SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME`, schema)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	list := make([]map[string]string, 0)
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			writeDBError(w, err)
			return
		}
		list = append(list, map[string]string{"name": name, "type": kind})
	}
	writeJSON(w, 200, map[string]any{"tables": list})
}
func (s *server) rows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { methodNotAllowed(w); return }
	schema, table, meta, err := s.requestMeta(r)
	if err != nil { writeError(w, 400, err.Error()); return }
	q := r.URL.Query()
	limit := clampInt(q.Get("limit"), 100, 1, 500)
	offset := clampInt(q.Get("offset"), 0, 0, 100000000)
	where, args, err := buildFilters(q, meta)
	if err != nil { writeError(w, 400, err.Error()); return }
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	sort, err := buildSort(q, meta)
	if err != nil { writeError(w, 400, err.Error()); return }

	// Cursor paging avoids COUNT(*) and costly deep OFFSET scans on tables with primary keys.
	cursorMode := sort == "" && len(meta.PrimaryKeys) > 0
	if cursorMode {
		order := make([]string, len(meta.PrimaryKeys))
		for i, key := range meta.PrimaryKeys { order[i] = quoteIdentifier(key)+" ASC" }
		sort = " ORDER BY " + strings.Join(order, ",")
		if token := strings.TrimSpace(q.Get("cursor")); token != "" {
			values, err := decodeCursor(token, len(meta.PrimaryKeys))
			if err != nil { writeError(w, 400, "分页游标无效"); return }
			seek, seekArgs := primaryCursorCondition(meta.PrimaryKeys, values)
			where = appendWhere(where, seek)
			args = append(args, seekArgs...)
		}
	}
	qualified := qualify(schema, table)
	query := "SELECT * FROM " + qualified + where + sort + " LIMIT ?"
	args = append(args, limit+1)
	if !cursorMode { query += " OFFSET ?"; args = append(args, offset) }
	result, err := db.Query(query, args...)
	if err != nil { writeDBError(w, err); return }
	defer result.Close()
	columnNames, err := result.Columns()
	if err != nil { writeDBError(w, err); return }
	keyIndexes := make([]int, len(meta.PrimaryKeys))
	for i, key := range meta.PrimaryKeys {
		keyIndexes[i] = -1
		for index, name := range columnNames { if name == key { keyIndexes[i] = index; break } }
	}
	data := make([]map[string]any, 0, limit)
	var lastKeyValues []any
	hasMore := false
	for result.Next() {
		values := make([]any, len(columnNames)); pointers := make([]any, len(columnNames))
		for i := range values { pointers[i] = &values[i] }
		if err := result.Scan(pointers...); err != nil { writeDBError(w, err); return }
		if len(data) == limit { hasMore = true; break }
		item := make(map[string]any, len(columnNames))
		for i, name := range columnNames { item[name] = jsonValue(values[i]) }
		if cursorMode {
			lastKeyValues = make([]any, len(keyIndexes))
			for i, index := range keyIndexes {
				if index < 0 { writeError(w, 500, "无法读取主键字段"); return }
				lastKeyValues[i] = jsonValue(values[index])
			}
		}
		data = append(data, item)
	}
	if err := result.Err(); err != nil { writeDBError(w, err); return }
	nextCursor := ""
	if cursorMode && hasMore && len(lastKeyValues) > 0 {
		nextCursor, err = encodeCursor(lastKeyValues)
		if err != nil { writeError(w, 500, "无法创建分页游标"); return }
	}
	writeJSON(w, 200, map[string]any{"columns": meta.Columns, "primaryKeys": meta.PrimaryKeys, "rows": data, "limit": limit, "offset": offset, "hasMore": hasMore, "cursorMode": cursorMode, "nextCursor": nextCursor})
}
func (s *server) row(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	var payload rowRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, 400, "请求数据无效: "+err.Error())
		return
	}
	schema, err := requiredIdentifier(payload.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(payload.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	meta, err := s.getMeta(schema, table)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	allowed := map[string]bool{}
	for _, col := range meta.Columns {
		allowed[col.Name] = true
	}
	locatorKeys := meta.PrimaryKeys
	if len(locatorKeys) == 0 {
		locatorKeys = make([]string, 0, len(meta.Columns))
		for _, col := range meta.Columns {
			locatorKeys = append(locatorKeys, col.Name)
		}
	}
	where, whereArgs, err := primaryWhere(locatorKeys, payload.PrimaryKey, allowed)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if r.Method == http.MethodDelete {
		res, err := db.Exec("DELETE FROM "+qualify(schema, table)+where, whereArgs...)
		if err != nil {
			writeDBError(w, err)
			return
		}
		n, _ := res.RowsAffected()
		writeJSON(w, 200, map[string]any{"deleted": n})
		return
	}
	if len(payload.Data) == 0 {
		writeError(w, 400, "没有要保存的字段")
		return
	}
	setParts := make([]string, 0, len(payload.Data))
	args := make([]any, 0, len(payload.Data)+len(whereArgs))
	for name, value := range payload.Data {
		if !allowed[name] {
			writeError(w, 400, "字段不存在: "+name)
			return
		}
		setParts = append(setParts, quoteIdentifier(name)+"=?")
		args = append(args, value)
	}
	args = append(args, whereArgs...)
	res, err := db.Exec("UPDATE "+qualify(schema, table)+" SET "+strings.Join(setParts, ",")+where, args...)
	if err != nil {
		writeDBError(w, err)
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, 200, map[string]any{"updated": n})
}
func (s *server) importSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 50<<20)
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		writeError(w, 400, "导入文件无效或超过 50 MB")
		return
	}
	schema := strings.TrimSpace(r.FormValue("schema"))
	if schema != "" {
		var err error
		schema, err = requiredIdentifier(schema, "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, 400, "请选择 .sql 文件")
		return
	}
	defer file.Close()
	script, err := io.ReadAll(io.LimitReader(file, 50<<20))
	if err != nil {
		writeError(w, 400, "无法读取文件")
		return
	}
	if len(strings.TrimSpace(string(script))) == 0 {
		writeError(w, 400, "SQL 文件为空")
		return
	}
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	conn, err := db.Conn(r.Context())
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer conn.Close()
	if schema != "" {
		if _, err := conn.ExecContext(r.Context(), "USE "+quoteIdentifier(schema)); err != nil {
			writeDBError(w, err)
			return
		}
	}
	if _, err := conn.ExecContext(r.Context(), string(script)); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"message": "SQL 已执行完成"})
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
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
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
	rows, err := db.Query("SELECT * FROM " + qualify(schema, table))
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
	quotedNames := make([]string, len(colNames))
	for i, name := range colNames {
		quotedNames[i] = quoteIdentifier(name)
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
			literals[i] = sqlLiteral(value)
		}
		_, _ = fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n", quoteIdentifier(table), strings.Join(quotedNames, ","), strings.Join(literals, ","))
	}
	_ = meta
}

func (s *server) executeSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { methodNotAllowed(w); return }
	var request sqlRequest
	if err := decodeJSON(r, &request); err != nil { writeError(w, 400, "SQL 请求无效: "+err.Error()); return }
	statement := strings.TrimSpace(request.SQL)
	if statement == "" { writeError(w, 400, "请输入 SQL"); return }
	if len(statement) > 2<<20 { writeError(w, 400, "SQL 不能超过 2 MB"); return }
	if request.Schema != "" { var err error; request.Schema, err = requiredIdentifier(request.Schema, "数据库"); if err != nil { writeError(w, 400, err.Error()); return } }
	db, err := s.currentDB()
	if err != nil { writeError(w, 400, err.Error()); return }
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second); defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil { writeDBError(w, err); return }
	defer conn.Close()
	if request.Schema != "" { if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(request.Schema)); err != nil { writeDBError(w, err); return } }
	if !sqlReturnsRows(statement) {
		result, err := conn.ExecContext(ctx, statement)
		if err != nil { writeDBError(w, err); return }
		affected, _ := result.RowsAffected()
		writeJSON(w, 200, map[string]any{"message": "SQL 执行成功", "affectedRows": affected})
		return
	}
	limit := request.Limit
	if limit <= 0 { limit = 100 }
	if limit > 500 { limit = 500 }
	offset := request.Offset
	if offset < 0 { offset = 0 }
	query := strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	serverPaged := !regexp.MustCompile(`(?i)\blimit\b`).MatchString(query) && !regexp.MustCompile(`(?i)\bfor\s+update\b`).MatchString(query)
	var rows *sql.Rows
	if serverPaged {
		rows, err = conn.QueryContext(ctx, query+" LIMIT ? OFFSET ?", limit+1, offset)
	} else {
		rows, err = conn.QueryContext(ctx, statement)
	}
	if err != nil { writeDBError(w, err); return }
	defer rows.Close()
	columns, err := rows.Columns(); if err != nil { writeDBError(w, err); return }
	data := make([]map[string]any, 0, limit)
	hasMore, skipped := false, 0
	for rows.Next() {
		values, refs := make([]any, len(columns)), make([]any, len(columns))
		for i := range values { refs[i] = &values[i] }
		if err := rows.Scan(refs...); err != nil { writeDBError(w, err); return }
		if !serverPaged && skipped < offset { skipped++; continue }
		if len(data) >= limit { hasMore = true; break }
		item := make(map[string]any, len(columns))
		for i, name := range columns { item[name] = jsonValue(values[i]) }
		data = append(data, item)
	}
	if err := rows.Err(); err != nil { writeDBError(w, err); return }
	writeJSON(w, 200, map[string]any{"message": "查询执行成功", "columns": columns, "rows": data, "hasMore": hasMore, "limit": limit, "offset": offset})
}

func (s *server) sqlComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { methodNotAllowed(w); return }
	var request sqlCompletionRequest
	if err := decodeJSON(r, &request); err != nil { writeError(w, 400, "补全请求无效"); return }
	request.Schema = strings.TrimSpace(request.Schema)
	if request.Schema == "" { writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}}); return }
	var err error
	request.Schema, err = requiredIdentifier(request.Schema, "数据库")
	if err != nil { writeError(w, 400, err.Error()); return }
	db, err := s.currentDB(); if err != nil { writeError(w, 400, err.Error()); return }
	if match := sqlCompletionTail.FindStringSubmatch(request.SQL); len(match) == 3 {
		qualifier, prefix := cleanSQLIdentifier(match[1]), match[2]
		schema, table := resolveSQLReference(request.SQL, request.Schema, qualifier)
		if qualifier == "" || schema == "" || table == "" { writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}}); return }
		rows, err := db.Query(`SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME LIKE ? ORDER BY COLUMN_NAME LIMIT 50`, schema, table, prefix+"%")
		if err != nil { writeDBError(w, err); return }
		defer rows.Close()
		suggestions := make([]completionSuggestion, 0)
		for rows.Next() { var name string; if err := rows.Scan(&name); err != nil { writeDBError(w, err); return }; suggestions = append(suggestions, completionSuggestion{Value: qualifier+"."+name, Kind: "字段"}) }
		writeJSON(w, 200, map[string]any{"suggestions": suggestions}); return
	}
	match := sqlWordTail.FindStringSubmatch(request.SQL)
	if len(match) != 2 { writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}}); return }
	prefix := match[1]
	rows, err := db.Query(`SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME LIKE ? ORDER BY TABLE_NAME LIMIT 50`, request.Schema, prefix+"%")
	if err != nil { writeDBError(w, err); return }
	defer rows.Close()
	suggestions := make([]completionSuggestion, 0)
	for rows.Next() { var name string; if err := rows.Scan(&name); err != nil { writeDBError(w, err); return }; suggestions = append(suggestions, completionSuggestion{Value: name, Kind: "表"}) }
	for _, keyword := range []string{"SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "ON", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "AS", "AND", "OR", "NOT", "IN", "LIKE", "IS NULL"} {
		if strings.HasPrefix(strings.ToLower(keyword), strings.ToLower(prefix)) { suggestions = append(suggestions, completionSuggestion{Value: keyword, Kind: "关键字"}) }
	}
	sort.Slice(suggestions, func(i, j int) bool { if suggestions[i].Kind != suggestions[j].Kind { return suggestions[i].Kind < suggestions[j].Kind }; return strings.ToLower(suggestions[i].Value) < strings.ToLower(suggestions[j].Value) })
	writeJSON(w, 200, map[string]any{"suggestions": suggestions})
}

func sqlReturnsRows(statement string) bool {
	text := strings.TrimSpace(statement)
	for {
		if strings.HasPrefix(text, "--") { if newline := strings.IndexByte(text, '\n'); newline >= 0 { text = strings.TrimSpace(text[newline+1:]); continue } }
		if strings.HasPrefix(text, "/*") { if end := strings.Index(text, "*/"); end >= 0 { text = strings.TrimSpace(text[end+2:]); continue } }
		break
	}
	fields := strings.Fields(text)
	if len(fields) == 0 { return false }
	keyword := strings.ToUpper(fields[0])
	switch keyword { case "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH": return true }
	return false
}

func cleanSQLIdentifier(value string) string { return strings.Trim(strings.TrimSpace(value), "`") }

func resolveSQLReference(sqlText, defaultSchema, qualifier string) (string, string) {
	reserved := map[string]bool{"where": true, "join": true, "left": true, "right": true, "inner": true, "outer": true, "on": true, "group": true, "order": true, "limit": true, "having": true, "union": true}
	references := map[string][2]string{}
	for _, match := range sqlTableReference.FindAllStringSubmatch(sqlText, -1) {
		reference, alias := strings.TrimSpace(match[1]), strings.ToLower(strings.TrimSpace(match[2]))
		parts := strings.Split(strings.ReplaceAll(reference, "`", ""), ".")
		schema, table := defaultSchema, ""
		if len(parts) == 1 { table = parts[0] } else if len(parts) == 2 { schema, table = parts[0], parts[1] }
		if !identifierPattern.MatchString(schema) || !identifierPattern.MatchString(table) { continue }
		pair := [2]string{schema, table}
		references[strings.ToLower(table)] = pair
		if alias != "" && !reserved[alias] { references[alias] = pair }
	}
	if reference, ok := references[strings.ToLower(qualifier)]; ok { return reference[0], reference[1] }
	if identifierPattern.MatchString(qualifier) { return defaultSchema, qualifier }
	return "", ""
}

func (s *server) requestMeta(r *http.Request) (string, string, tableMeta, error) {
	schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
	if err != nil {
		return "", "", tableMeta{}, err
	}
	table, err := requiredIdentifier(r.URL.Query().Get("table"), "表")
	if err != nil {
		return "", "", tableMeta{}, err
	}
	meta, err := s.getMeta(schema, table)
	if err != nil {
		return "", "", tableMeta{}, err
	}
	return schema, table, meta, nil
}
func (s *server) getMeta(schema, table string) (tableMeta, error) {
	db, err := s.currentDB()
	if err != nil { return tableMeta{}, err }
	rows, err := db.Query(`SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	defer rows.Close()
	meta := tableMeta{Columns: make([]column, 0), PrimaryKeys: make([]string, 0)}
	for rows.Next() {
		var c column
		var nullable, key string
		if err := rows.Scan(&c.Name, &c.Type, &nullable, &key); err != nil {
			return tableMeta{}, err
		}
		c.Nullable, c.Primary = nullable == "YES", key == "PRI"
		meta.Columns = append(meta.Columns, c)
		if c.Primary {
			meta.PrimaryKeys = append(meta.PrimaryKeys, c.Name)
		}
	}
	if err := rows.Err(); err != nil {
		return tableMeta{}, err
	}
	if len(meta.Columns) == 0 {
		return tableMeta{}, errors.New("找不到指定的表")
	}
	return meta, nil
}
type filterCondition struct {
	Column string `json:"column"`
	Operator string `json:"operator"`
	Value string `json:"value"`
}

func buildFilters(q url.Values, meta tableMeta) (string, []any, error) {
	conditions := make([]filterCondition, 0)
	if raw := strings.TrimSpace(q.Get("filters")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &conditions); err != nil { return "", nil, errors.New("多字段筛选条件格式无效") }
		if len(conditions) > 8 { return "", nil, errors.New("最多支持 8 个筛选条件") }
	} else if name := strings.TrimSpace(q.Get("filterColumn")); name != "" {
		conditions = append(conditions, filterCondition{Column: name, Operator: q.Get("filterOperator"), Value: q.Get("filterValue")})
	}
	if len(conditions) == 0 { return "", nil, nil }
	allowed := make(map[string]bool, len(meta.Columns))
	for _, col := range meta.Columns { allowed[col.Name] = true }
	parts, args := make([]string, 0, len(conditions)), make([]any, 0, len(conditions))
	for _, condition := range conditions {
		name, op, value := strings.TrimSpace(condition.Column), condition.Operator, condition.Value
		if name == "" { return "", nil, errors.New("筛选字段不能为空") }
		if op == "" { op = "contains" }
		if op != "is_null" && op != "not_null" && strings.TrimSpace(value) == "" { continue }
		if !allowed[name] { return "", nil, errors.New("筛选字段不存在: "+name) }
		field := quoteIdentifier(name)
		switch op {
		case "equals": parts, args = append(parts, field+"=?"), append(args, value)
		case "starts_with": parts, args = append(parts, field+" LIKE ?"), append(args, value+"%")
		case "gt": parts, args = append(parts, field+">?"), append(args, value)
		case "lt": parts, args = append(parts, field+"<?"), append(args, value)
		case "is_null": parts = append(parts, field+" IS NULL")
		case "not_null": parts = append(parts, field+" IS NOT NULL")
		case "contains": parts, args = append(parts, field+" LIKE ?"), append(args, "%"+value+"%")
		default: return "", nil, errors.New("不支持的筛选条件")
		}
	}
	if len(parts) == 0 { return "", nil, nil }
	return " WHERE "+strings.Join(parts, " AND "), args, nil
}
func appendWhere(where, condition string) string {
	if condition == "" { return where }
	if where == "" { return " WHERE " + condition }
	return where + " AND " + condition
}

func primaryCursorCondition(keys []string, values []any) (string, []any) {
	parts, args := make([]string, 0, len(keys)), make([]any, 0, len(keys)*(len(keys)+1)/2)
	for i, key := range keys {
		prefix := make([]string, 0, i+1)
		for j := 0; j < i; j++ { prefix, args = append(prefix, quoteIdentifier(keys[j])+"=?"), append(args, values[j]) }
		prefix, args = append(prefix, quoteIdentifier(key)+">?"), append(args, values[i])
		parts = append(parts, "("+strings.Join(prefix, " AND ")+")")
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func encodeCursor(values []any) (string, error) {
	payload, err := json.Marshal(values)
	if err != nil { return "", err }
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(token string, expected int) ([]any, error) {
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil { return nil, err }
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var values []any
	if err := decoder.Decode(&values); err != nil || len(values) != expected { return nil, errors.New("invalid cursor") }
	for i, value := range values {
		if value == nil { return nil, errors.New("invalid cursor") }
		if number, ok := value.(json.Number); ok { values[i] = number.String() }
	}
	return values, nil
}

func buildSort(q url.Values, meta tableMeta) (string, error) {
	columnName := strings.TrimSpace(q.Get("sortColumn"))
	if columnName == "" { return "", nil }
	found := false
	for _, column := range meta.Columns { if column.Name == columnName { found = true; break } }
	if !found { return "", errors.New("排序字段不存在") }
	direction := strings.ToUpper(strings.TrimSpace(q.Get("sortDirection")))
	if direction != "ASC" && direction != "DESC" { return "", errors.New("排序方向无效") }
	return " ORDER BY "+quoteIdentifier(columnName)+" "+direction, nil
}

func primaryWhere(keys []string, provided map[string]any, allowed map[string]bool) (string, []any, error) {
	parts, args := make([]string, 0, len(keys)), make([]any, 0, len(keys))
	for _, key := range keys {
		value, ok := provided[key]
		if !ok {
			return "", nil, errors.New("缺少主键字段: " + key)
		}
		if !allowed[key] {
			return "", nil, errors.New("主键字段无效")
		}
		if value == nil {
			parts = append(parts, quoteIdentifier(key)+" IS NULL")
			continue
		}
		parts, args = append(parts, quoteIdentifier(key)+"=?"), append(args, value)
	}
	return " WHERE " + strings.Join(parts, " AND "), args, nil
}
func requiredIdentifier(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if !identifierPattern.MatchString(value) {
		return "", fmt.Errorf("%s名称无效", label)
	}
	return value, nil
}
func quoteIdentifier(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }
func qualify(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}
func clampInt(value string, fallback, min, max int) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}
func jsonValue(value any) any {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return value
}
func sqlLiteral(value any) string {
	if value == nil {
		return "NULL"
	}
	if bytes, ok := value.([]byte); ok {
		return "X'" + hex.EncodeToString(bytes) + "'"
	}
	switch v := value.(type) {
	case time.Time:
		return quoteSQL(v.Format("2006-01-02 15:04:05.999999"))
	case int64, float64, bool:
		return fmt.Sprint(v)
	default:
		return quoteSQL(fmt.Sprint(v))
	}
}
func quoteSQL(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "'", "\\'", "\x00", "\\0", "\n", "\\n", "\r", "\\r", "\x1a", "\\Z")
	return "'" + replacer.Replace(value) + "'"
}
func decodeJSON(r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	return decoder.Decode(destination)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeDBError(w http.ResponseWriter, err error) {
	log.Printf("database error: %v", err)
	writeError(w, 500, "数据库操作失败: "+err.Error())
}
func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "不支持的请求方法")
}
