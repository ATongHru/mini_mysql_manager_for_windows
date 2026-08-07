package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/go-sql-driver/mysql"
)

//go:embed web/index.html web/favicon.svg web/favicon.ico
var webFiles embed.FS

var defaultPassword = ""

const noDefaultPasswordMarker = "__MYSQL_MANAGER_NO_DEFAULT_PASSWORD__"

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)
var sqlCompletionTail = regexp.MustCompile("(?i)([`A-Za-z0-9_$]+)\\s*\\.\\s*([A-Za-z0-9_$]*)$")
var sqlTableReference = regexp.MustCompile("(?is)\\b(?:from|join)\\s+((?:`[^`]+`|[A-Za-z0-9_$]+)(?:\\s*\\.\\s*(?:`[^`]+`|[A-Za-z0-9_$]+))?)(?:\\s+(?:as\\s+)?([A-Za-z_][A-Za-z0-9_$]*))?")
var sqlWordTail = regexp.MustCompile("(?i)([A-Za-z_][A-Za-z0-9_$]*)$")
var importUseStatement = regexp.MustCompile("(?is)^\\s*USE\\s+(?:`((?:``|[^`])+)`|([A-Za-z0-9_$]+))\\s*$")
var importDatabaseStatement = regexp.MustCompile("(?is)^(?:\\s*(?:--[^\\n]*(?:\\n|$)|#[^\\n]*(?:\\n|$)|/\\*.*?\\*/))*\\s*(?:CREATE|DROP|ALTER)\\s+(?:DATABASE|SCHEMA)\\b")

type server struct {
	mu                sync.RWMutex
	db                *sql.DB
	host              string
	port              string
	user              string
	password          string
	params            string
	passwordDefault   string
	backupMu          sync.RWMutex
	backupJobs        map[string]*backupJob
	importMu          sync.RWMutex
	importJobs        map[string]*importJob
	configMu          sync.RWMutex
	config            appConfig
	configPath        string
	resourceMu        sync.Mutex
	lastCPUTime       uint64
	lastCPUAt         time.Time
	metadataMu        sync.RWMutex
	metadata          metadataCache
	transactionMu     sync.Mutex
	activeTransaction *sql.Tx
	transactionAt     time.Time
}

type column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	Primary       bool   `json:"primary"`
	AutoIncrement bool   `json:"autoIncrement"`
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

type transactionRequest struct {
	Action string `json:"action"`
}

// statementExecutor is deliberately shared by sql.DB and sql.Tx.  When a
// table transaction is active, every grid read and record mutation uses this
// executor so the browser can immediately see its own uncommitted changes.
type statementExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type connectionRequest struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Params   string `json:"params"`
}

type sqlRequest struct {
	Schema   string `json:"schema"`
	SQL      string `json:"sql"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
	Password string `json:"password"`
}

type sqlExportRequest struct {
	Schema  string   `json:"schema"`
	SQL     string   `json:"sql"`
	Columns []string `json:"columns"`
}

type passwordRequest struct {
	Password string `json:"password"`
}

type sqlCompletionRequest struct {
	Schema string `json:"schema"`
	SQL    string `json:"sql"`
}

type batchDeleteRequest struct {
	Schema      string           `json:"schema"`
	Table       string           `json:"table"`
	PrimaryKeys []map[string]any `json:"primaryKeys"`
}

type createTableColumn struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	Primary       bool   `json:"primary"`
	AutoIncrement bool   `json:"autoIncrement"`
	Comment       string `json:"comment"`
}

type createTableRequest struct {
	Schema    string              `json:"schema"`
	Table     string              `json:"table"`
	Columns   []createTableColumn `json:"columns"`
	Engine    string              `json:"engine"`
	Charset   string              `json:"charset"`
	Collation string              `json:"collation"`
	Comment   string              `json:"comment"`
}

type columnTypeRequest struct {
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Column   string `json:"column"`
	NewName  string `json:"newName"`
	Type     string `json:"type"`
	Nullable *bool  `json:"nullable"`
	Primary  *bool  `json:"primary"`
	Force    bool   `json:"force"`
}

type createDatabaseRequest struct {
	Name      string `json:"name"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
}

type savedConnection struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Params   string `json:"params"`
}

type savedSQLQuery struct {
	Name           string `json:"name"`
	Schema         string `json:"schema"`
	SQL            string `json:"sql"`
	ConnectionName string `json:"connectionName"`
	ConnectionHost string `json:"connectionHost"`
	ConnectionPort string `json:"connectionPort"`
	ConnectionUser string `json:"connectionUser"`
}

type appConfig struct {
	Theme        string            `json:"theme"`
	Connections  []savedConnection `json:"connections"`
	SavedQueries []savedSQLQuery   `json:"savedQueries"`
}

type createIndexRequest struct {
	Schema   string   `json:"schema"`
	Table    string   `json:"table"`
	Name     string   `json:"name"`
	Type     string   `json:"type"`
	Columns  []string `json:"columns"`
	Password string   `json:"password"`
}

type tableColumnRequest struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	AutoIncrement bool   `json:"autoIncrement"`
	Password      string `json:"password"`
}

type tableActionRequest struct {
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Action   string `json:"action"`
	NewName  string `json:"newName"`
	Password string `json:"password"`
}

type completionSuggestion struct {
	Value string `json:"value"`
	Kind  string `json:"kind"`
}

type databasePermission struct {
	Database   string   `json:"database"`
	Privileges []string `json:"privileges"`
}

type permissionSnapshot struct {
	Databases        []databasePermission `json:"databases"`
	GlobalPrivileges []string             `json:"globalPrivileges"`
}

// cachedTable is the server-side copy of the object browser's table metadata.
// It deliberately contains only lightweight metadata, never table rows.
type cachedTable struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	RecordCount      *int64 `json:"recordCount,omitempty"`
	RecordCountExact bool   `json:"recordCountExact"`
}

type metadataCache struct {
	Databases   []string
	Tables      map[string][]cachedTable
	Permissions *permissionSnapshot
}

func main() {
	addr := flag.String("addr", env("MYSQL_MANAGER_ADDR", "127.0.0.1:52013"), "web listen address")
	host := flag.String("mysql-host", env("MYSQL_HOST", ""), "initial MySQL host")
	port := flag.String("mysql-port", env("MYSQL_PORT", "3306"), "initial MySQL port")
	user := flag.String("mysql-user", env("MYSQL_USER", "root"), "initial MySQL user")
	password := flag.String("mysql-password", env("MYSQL_PASSWORD", defaultPassword), "initial MySQL password")
	openBrowser := flag.Bool("open-browser", true, "open the local web page after startup")
	flag.Parse()
	if *password == noDefaultPasswordMarker {
		*password = ""
	}

	configPath := applicationConfigPath()
	config := loadAppConfig(configPath)
	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		if err := saveAppConfig(configPath, config); err != nil {
			log.Printf("无法创建配置文件 %s: %v", configPath, err)
		}
	}
	s := &server{host: *host, port: *port, user: *user, password: *password, passwordDefault: *password, backupJobs: make(map[string]*backupJob), importJobs: make(map[string]*importJob), config: config, configPath: configPath, metadata: metadataCache{Tables: make(map[string][]cachedTable)}}
	if strings.TrimSpace(*host) != "" {
		if db, err := openDatabase(*host, *port, *user, *password, ""); err != nil {
			log.Printf("initial MySQL connection unavailable: %v", err)
		} else {
			s.db = db
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.health)
	mux.HandleFunc("/api/status", s.status)
	mux.HandleFunc("/api/resources", s.resources)
	mux.HandleFunc("/api/settings", s.settings)
	mux.HandleFunc("/api/connection", s.connection)
	mux.HandleFunc("/api/metadata", s.metadataSnapshot)
	mux.HandleFunc("/api/connect", s.connect)
	mux.HandleFunc("/api/test-connection", s.testConnection)
	mux.HandleFunc("/api/disconnect", s.disconnect)
	mux.HandleFunc("/api/transaction", s.transaction)
	mux.HandleFunc("/api/sql", s.executeSQL)
	mux.HandleFunc("/api/sql-export", s.exportSQLQuery)
	mux.HandleFunc("/api/sql-complete", s.sqlComplete)
	mux.HandleFunc("/api/databases", s.databases)
	mux.HandleFunc("/api/database-options", s.databaseOptions)
	mux.HandleFunc("/api/database", s.database)
	mux.HandleFunc("/api/tables", s.tables)
	mux.HandleFunc("/api/table-count", s.tableCount)
	mux.HandleFunc("/api/rows", s.rows)
	mux.HandleFunc("/api/row", s.row)
	mux.HandleFunc("/api/rows/delete", s.deleteRows)
	mux.HandleFunc("/api/table", s.table)
	mux.HandleFunc("/api/import", s.importSQL)
	mux.HandleFunc("/api/export", s.exportSQL)
	mux.HandleFunc("/api/column-type", s.columnType)
	mux.HandleFunc("/api/column", s.tableColumn)
	mux.HandleFunc("/api/table-action", s.tableAction)
	mux.HandleFunc("/api/index", s.tableIndex)
	mux.HandleFunc("/api/table-create", s.tableCreateSQL)
	mux.HandleFunc("/api/table-info", s.tableInfo)
	mux.HandleFunc("/api/backup", s.backup)
	mux.HandleFunc("/favicon.svg", s.favicon)
	mux.HandleFunc("/favicon.ico", s.faviconICO)
	mux.HandleFunc("/", s.index)
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", *addr, err)
	}
	localURL := "http://" + *addr
	log.Printf("Mini MySQL Manager for Windows 已启动。")
	log.Printf("请在浏览器访问：%s", localURL)
	log.Printf("关闭此终端窗口将停止服务。")
	if *openBrowser {
		go openLocalBrowser(localURL)
	}
	log.Fatal((&http.Server{Handler: recoverMiddleware(mux)}).Serve(listener))
}

func openLocalBrowser(localURL string) {
	// When the default browser is not already running, launching the URL through
	// the http protocol handler (cmd start / rundll32 url.dll) makes a
	// cold-started browser open both its configured startup/home page and the
	// requested local page. For Chromium-based browsers (Edge, Chrome, ...) we
	// launch the executable directly with --app=<url>, which opens the page in a
	// standalone window and does not trigger the startup pages.
	if exe, ok := defaultBrowserExe(); ok && isChromiumBrowser(exe) {
		if err := exec.Command(exe, "--app="+localURL).Start(); err == nil {
			return
		}
	}
	// Fallback: let the Windows shell open the URL.
	if err := exec.Command("cmd.exe", "/c", "start", "", localURL).Start(); err != nil {
		log.Printf("could not open browser automatically: %v", err)
	}
}

// defaultBrowserExe resolves the executable path of the default HTTP browser
// from the Windows registry. It returns ok=false when the default browser
// cannot be determined.
func defaultBrowserExe() (string, bool) {
	progID := regQueryString(`HKCU\Software\Microsoft\Windows\Shell\Associations\UrlAssociations\http\UserChoice`, "ProgId")
	if progID == "" {
		return "", false
	}
	command := regQueryString(`HKCR\`+progID+`\shell\open\command`, "")
	if command == "" {
		return "", false
	}
	return extractExePath(command), true
}

// extractExePath pulls the leading executable path out of a registry shell
// command template such as `"C:\...\msedge.exe" --single-argument %1`.
func extractExePath(command string) string {
	command = strings.TrimSpace(command)
	if strings.HasPrefix(command, `"`) {
		if end := strings.Index(command[1:], `"`); end >= 0 {
			return command[1 : 1+end]
		}
	}
	if idx := strings.IndexByte(command, ' '); idx >= 0 {
		return command[:idx]
	}
	return command
}

// isChromiumBrowser reports whether the executable belongs to a Chromium-based
// browser that understands the --app=<url> flag.
func isChromiumBrowser(exePath string) bool {
	switch strings.ToLower(filepath.Base(exePath)) {
	case "msedge.exe", "chrome.exe", "chromium.exe", "brave.exe", "vivaldi.exe":
		return true
	}
	return false
}

// regQueryString reads a REG_SZ value from the Windows registry via reg.exe.
// An empty name queries the default value. It returns "" when the value cannot
// be read.
func regQueryString(key, name string) string {
	args := []string{"query", key}
	if name == "" {
		args = append(args, "/ve")
	} else {
		args = append(args, "/v", name)
	}
	out, err := exec.Command("reg", args...).Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if idx := strings.Index(line, "REG_SZ"); idx >= 0 {
			return strings.TrimSpace(line[idx+len("REG_SZ"):])
		}
	}
	return ""
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

func applicationConfigPath() string {
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		return filepath.Join(home, ".mini-manage", "mysql-manage.json")
	}
	return "mysql-manage.json"
}

// createExportFile reserves a new file beside the running EXE. Exporting is a
// local desktop operation, so keeping the result next to the application makes
// it discoverable without relying on the browser's download configuration.
func createExportFile(filename string) (*os.File, string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, "", fmt.Errorf("无法确定程序目录: %w", err)
	}
	directory := filepath.Dir(executable)
	extension := filepath.Ext(filename)
	base := strings.TrimSuffix(filename, extension)
	for index := 0; ; index++ {
		candidate := filename
		if index > 0 {
			candidate = fmt.Sprintf("%s-%d%s", base, index, extension)
		}
		path := filepath.Join(directory, candidate)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return file, path, nil
	}
}

func finishExportFile(w http.ResponseWriter, file *os.File, path string) bool {
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		writeError(w, http.StatusInternalServerError, "导出文件保存失败: "+err.Error())
		return false
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "导出完成", "path": path, "filename": filepath.Base(path)})
	return true
}

func loadAppConfig(path string) appConfig {
	config := appConfig{Theme: "light", Connections: []savedConnection{}, SavedQueries: []savedSQLQuery{}}
	data, err := os.ReadFile(path)
	if err != nil {
		return config
	}
	if err := json.Unmarshal(data, &config); err != nil || (config.Theme != "light" && config.Theme != "dark") {
		return appConfig{Theme: "light", Connections: []savedConnection{}, SavedQueries: []savedSQLQuery{}}
	}
	if config.Connections == nil {
		config.Connections = []savedConnection{}
	}
	if config.SavedQueries == nil {
		config.SavedQueries = []savedSQLQuery{}
	}
	return config
}

func saveAppConfig(path string, config appConfig) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

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
func (s *server) favicon(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	data, err := webFiles.ReadFile("web/favicon.svg")
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(data)
}
func (s *server) faviconICO(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	data, err := webFiles.ReadFile("web/favicon.ico")
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(data)
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	db, err := s.currentDB()
	if err != nil {
		writeJSON(w, 200, map[string]bool{"ok": false})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	writeJSON(w, 200, map[string]bool{"ok": db.PingContext(ctx) == nil})
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	s.mu.RLock()
	connected := s.db != nil
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]bool{"online": true, "connected": connected})
}

type winFiletime struct{ LowDateTime, HighDateTime uint32 }

type processMemoryCountersEx struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
	PrivateUsage               uintptr
}

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var (
	kernel32DLL              = syscall.NewLazyDLL("kernel32.dll")
	getProcessTimesProc      = kernel32DLL.NewProc("GetProcessTimes")
	globalMemoryStatusExProc = kernel32DLL.NewProc("GlobalMemoryStatusEx")
	psapiDLL                 = syscall.NewLazyDLL("psapi.dll")
	getProcessMemoryInfoProc = psapiDLL.NewProc("GetProcessMemoryInfo")
)

func processCPUTime() (uint64, error) {
	var creation, exit, kernel, user winFiletime
	result, _, callErr := getProcessTimesProc.Call(^uintptr(0), uintptr(unsafe.Pointer(&creation)), uintptr(unsafe.Pointer(&exit)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user)))
	if result == 0 {
		return 0, callErr
	}
	toUint64 := func(value winFiletime) uint64 { return uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime) }
	return toUint64(kernel) + toUint64(user), nil
}

func processMemoryUsage() (uint64, uint64, error) {
	counters := processMemoryCountersEx{CB: uint32(unsafe.Sizeof(processMemoryCountersEx{}))}
	result, _, callErr := getProcessMemoryInfoProc.Call(^uintptr(0), uintptr(unsafe.Pointer(&counters)), uintptr(counters.CB))
	if result == 0 {
		return 0, 0, callErr
	}
	memory := memoryStatusEx{Length: uint32(unsafe.Sizeof(memoryStatusEx{}))}
	result, _, callErr = globalMemoryStatusExProc.Call(uintptr(unsafe.Pointer(&memory)))
	if result == 0 {
		return 0, 0, callErr
	}
	return uint64(counters.WorkingSetSize), memory.TotalPhys, nil
}

func (s *server) resources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	now := time.Now()
	cpuTime, cpuErr := processCPUTime()
	workingSet, totalMemory, memoryErr := processMemoryUsage()
	var cpuPercent float64
	s.resourceMu.Lock()
	if cpuErr == nil && !s.lastCPUAt.IsZero() {
		elapsed := now.Sub(s.lastCPUAt).Seconds()
		if elapsed > 0 {
			cpuPercent = float64(cpuTime-s.lastCPUTime) / 10000000 / elapsed / float64(runtime.NumCPU()) * 100
			if cpuPercent < 0 {
				cpuPercent = 0
			}
		}
	}
	if cpuErr == nil {
		s.lastCPUTime, s.lastCPUAt = cpuTime, now
	}
	s.resourceMu.Unlock()
	var memoryPercent float64
	if memoryErr == nil && totalMemory > 0 {
		memoryPercent = float64(workingSet) / float64(totalMemory) * 100
	}
	writeJSON(w, 200, map[string]any{"backendCPUPercent": cpuPercent, "backendMemoryPercent": memoryPercent, "backendMemoryMB": float64(workingSet) / (1024 * 1024)})
}

func validConfigName(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len([]rune(value)) <= 64 && !strings.ContainsAny(value, "\r\n\x00")
}

func validateAppConfig(config appConfig) error {
	if config.Theme != "light" && config.Theme != "dark" {
		return errors.New("不支持的界面主题")
	}
	if len(config.Connections) > 50 || len(config.SavedQueries) > 100 {
		return errors.New("保存项目数量超过限制")
	}
	seenConnections := map[string]bool{}
	for _, item := range config.Connections {
		key := strings.ToLower(strings.TrimSpace(item.Name))
		if !validConfigName(item.Name) || seenConnections[key] || strings.TrimSpace(item.Host) == "" || strings.TrimSpace(item.User) == "" {
			return errors.New("保存的连接信息无效或名称重复")
		}
		seenConnections[key] = true
	}
	seenQueries := map[string]bool{}
	for _, item := range config.SavedQueries {
		key := strings.ToLower(strings.TrimSpace(item.Name))
		if !validConfigName(item.Name) || seenQueries[key] || strings.TrimSpace(item.SQL) == "" || len(item.SQL) > 2<<20 {
			return errors.New("保存的 SQL 无效或名称重复")
		}
		seenQueries[key] = true
	}
	return nil
}

func (s *server) settings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.configMu.RLock()
		config := s.config
		s.configMu.RUnlock()
		writeJSON(w, 200, config)
		return
	}
	if r.Method != http.MethodPut {
		methodNotAllowed(w)
		return
	}
	var config appConfig
	if err := decodeJSON(r, &config); err != nil {
		writeError(w, 400, "设置数据无效")
		return
	}
	s.configMu.Lock()
	if config.Connections == nil {
		config.Connections = s.config.Connections
	}
	if config.SavedQueries == nil {
		config.SavedQueries = s.config.SavedQueries
	}
	if err := validateAppConfig(config); err != nil {
		s.configMu.Unlock()
		writeError(w, 400, err.Error())
		return
	}
	err := saveAppConfig(s.configPath, config)
	if err == nil {
		s.config = config
	}
	s.configMu.Unlock()
	if err != nil {
		writeError(w, 500, "无法保存设置: "+err.Error())
		return
	}
	writeJSON(w, 200, config)
}

func (s *server) connection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	s.mu.RLock()
	host, port, user, params, db := s.host, s.port, s.user, s.params, s.db
	s.mu.RUnlock()
	response := map[string]any{"connected": db != nil, "host": host, "port": port, "user": user, "params": params, "passwordDefault": s.passwordDefault}
	if db != nil {
		permissions, ok := s.cachedPermissions()
		if !ok {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			var err error
			permissions, err = databasePermissions(ctx, db)
			cancel()
			if err == nil {
				s.cachePermissions(permissions)
				ok = true
			}
		}
		if ok {
			response["permissions"] = permissions
		}
	}
	writeJSON(w, 200, response)
}

func cloneCachedTables(source []cachedTable) []cachedTable {
	result := make([]cachedTable, len(source))
	for index, item := range source {
		result[index] = item
		if item.RecordCount != nil {
			count := *item.RecordCount
			result[index].RecordCount = &count
		}
	}
	return result
}

func (s *server) cachePermissions(permissions permissionSnapshot) {
	databaseNames := make([]string, 0, len(permissions.Databases))
	for _, item := range permissions.Databases {
		databaseNames = append(databaseNames, item.Database)
	}
	s.metadataMu.Lock()
	s.metadata.Permissions = &permissions
	s.metadata.Databases = databaseNames
	s.metadata.Tables = make(map[string][]cachedTable)
	s.metadataMu.Unlock()
}

func (s *server) cachedPermissions() (permissionSnapshot, bool) {
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if s.metadata.Permissions == nil {
		return permissionSnapshot{}, false
	}
	return *s.metadata.Permissions, true
}

func (s *server) cacheDatabases(databases []string) {
	s.metadataMu.Lock()
	s.metadata.Databases = append([]string(nil), databases...)
	if s.metadata.Tables == nil {
		s.metadata.Tables = make(map[string][]cachedTable)
	}
	s.metadataMu.Unlock()
}

func (s *server) cachedDatabases() ([]string, bool) {
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	if s.metadata.Databases == nil {
		return nil, false
	}
	return append([]string(nil), s.metadata.Databases...), true
}

func (s *server) cacheTables(schema string, tables []cachedTable) {
	s.metadataMu.Lock()
	if s.metadata.Tables == nil {
		s.metadata.Tables = make(map[string][]cachedTable)
	}
	s.metadata.Tables[schema] = cloneCachedTables(tables)
	s.metadataMu.Unlock()
}

func (s *server) cachedTables(schema string) ([]cachedTable, bool) {
	s.metadataMu.RLock()
	defer s.metadataMu.RUnlock()
	tables, ok := s.metadata.Tables[schema]
	return cloneCachedTables(tables), ok
}

func (s *server) cacheTableCount(schema, table string, count int64) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	for index := range s.metadata.Tables[schema] {
		item := &s.metadata.Tables[schema][index]
		if item.Name == table {
			item.RecordCount = &count
			item.RecordCountExact = true
			return
		}
	}
}

func (s *server) invalidateCachedTableCount(schema, table string) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	for index := range s.metadata.Tables[schema] {
		item := &s.metadata.Tables[schema][index]
		if item.Name == table {
			item.RecordCount = nil
			item.RecordCountExact = false
			return
		}
	}
}

func (s *server) clearMetadataCache() {
	s.metadataMu.Lock()
	s.metadata = metadataCache{Tables: make(map[string][]cachedTable)}
	s.metadataMu.Unlock()
}

// metadataSnapshot returns the cached object-browser data to a freshly loaded
// page. The cache survives browser refreshes while the local manager is running.
func (s *server) metadataSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	s.metadataMu.RLock()
	databases := append([]string(nil), s.metadata.Databases...)
	tables := make(map[string][]cachedTable, len(s.metadata.Tables))
	for schema, items := range s.metadata.Tables {
		tables[schema] = cloneCachedTables(items)
	}
	var permissions *permissionSnapshot
	if s.metadata.Permissions != nil {
		copy := *s.metadata.Permissions
		permissions = &copy
	}
	s.metadataMu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"databases": databases, "tables": tables, "permissions": permissions})
}

func normalizeConnectionRequest(request *connectionRequest) error {
	request.Host, request.Port, request.User = strings.TrimSpace(request.Host), strings.TrimSpace(request.Port), strings.TrimSpace(request.User)
	if request.Host == "" || strings.ContainsAny(request.Host, "\r\n\x00") {
		return errors.New("请输入有效的 MySQL IP 或主机名")
	}
	if request.User == "" || strings.ContainsAny(request.User, "\r\n\x00") {
		return errors.New("请输入有效的用户名")
	}
	port, err := strconv.Atoi(request.Port)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("端口必须是 1 到 65535 之间的数字")
	}
	normalized, err := normalizeMySQLParams(request.Params)
	if err != nil {
		return err
	}
	request.Params = normalized
	return nil
}

func normalizeMySQLParams(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimLeft(raw, "?& \t")
	if raw == "" {
		return "", nil
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return "", errors.New("连接参数格式无效，请使用 key=value&key2=value2")
	}
	normalized := url.Values{}
	for key, valuesForKey := range values {
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, "\r\n\x00") || len(valuesForKey) == 0 {
			return "", errors.New("连接参数名称无效")
		}
		// url.ParseQuery groups duplicate keys; retain only the last copied value.
		normalized.Set(key, valuesForKey[len(valuesForKey)-1])
	}
	return normalized.Encode(), nil
}

func (s *server) testConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request connectionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "连接参数无效: "+err.Error())
		return
	}
	if err := normalizeConnectionRequest(&request); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	probe, err := openDatabase(request.Host, request.Port, request.User, request.Password, request.Params)
	if err != nil {
		writeError(w, 400, "无法连接 MySQL: "+err.Error())
		return
	}
	_ = probe.Close()
	writeJSON(w, 200, map[string]any{"message": "连接测试成功", "params": request.Params})
}

func (s *server) connect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request connectionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "连接参数无效: "+err.Error())
		return
	}
	if err := normalizeConnectionRequest(&request); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := openDatabase(request.Host, request.Port, request.User, request.Password, request.Params)
	if err != nil {
		writeError(w, 400, "无法连接 MySQL: "+err.Error())
		return
	}
	permissions, err := databasePermissions(r.Context(), db)
	if err != nil {
		_ = db.Close()
		writeDBError(w, err)
		return
	}
	s.rollbackActiveTransaction()
	s.mu.Lock()
	old := s.db
	s.db, s.host, s.port, s.user, s.password, s.params = db, request.Host, request.Port, request.User, request.Password, request.Params
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	s.cachePermissions(permissions)
	writeJSON(w, 200, map[string]any{"message": "MySQL 连接成功", "host": request.Host, "port": request.Port, "user": request.User, "params": request.Params, "permissions": permissions})
}

func (s *server) disconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	s.rollbackActiveTransaction()
	s.mu.Lock()
	db := s.db
	s.db, s.password, s.params = nil, "", ""
	s.mu.Unlock()
	if db != nil {
		_ = db.Close()
	}
	s.clearMetadataCache()
	writeJSON(w, 200, map[string]string{"message": "已退出 MySQL 连接"})
}

func openDatabase(host, port, user, password, rawParams string) (*sql.DB, error) {
	params, err := normalizeMySQLParams(rawParams)
	if err != nil {
		return nil, err
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, password, "tcp", net.JoinHostPort(host, port)
	cfg.ParseTime, cfg.MultiStatements, cfg.Loc = true, true, time.Local
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 15*time.Second, 15*time.Second
	if params != "" {
		values, _ := url.ParseQuery(params)
		cfg.Params = make(map[string]string, len(values))
		for key, valuesForKey := range values {
			cfg.Params[key] = valuesForKey[0]
		}
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func databasePermissions(ctx context.Context, db *sql.DB) (permissionSnapshot, error) {
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return permissionSnapshot{}, err
	}
	databaseNames := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return permissionSnapshot{}, err
		}
		databaseNames = append(databaseNames, name)
	}
	if err := rows.Close(); err != nil {
		return permissionSnapshot{}, err
	}
	permissions := make(map[string]map[string]bool, len(databaseNames))
	originalNames := make(map[string]string, len(databaseNames))
	for _, name := range databaseNames {
		key := strings.ToLower(name)
		permissions[key] = map[string]bool{}
		originalNames[key] = name
	}
	global := map[string]bool{}
	grantRows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err == nil {
		for grantRows.Next() {
			var grant string
			if err := grantRows.Scan(&grant); err != nil {
				continue
			}
			applyGrantPrivileges(grant, permissions, global)
		}
		_ = grantRows.Close()
	}
	// SCHEMA_PRIVILEGES covers database-scoped grants that may not be present as
	// an easily parseable database pattern in SHOW GRANTS.
	schemaRows, err := db.QueryContext(ctx, "SELECT TABLE_SCHEMA, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES")
	if err == nil {
		for schemaRows.Next() {
			var schema, privilege string
			if err := schemaRows.Scan(&schema, &privilege); err == nil {
				if set, exists := permissions[strings.ToLower(schema)]; exists {
					set[strings.ToUpper(privilege)] = true
				}
			}
		}
		_ = schemaRows.Close()
	}
	for _, set := range permissions {
		for privilege := range global {
			set[privilege] = true
		}
	}
	snapshot := permissionSnapshot{Databases: make([]databasePermission, 0, len(databaseNames)), GlobalPrivileges: sortedPrivilegeNames(global)}
	for _, name := range databaseNames {
		key := strings.ToLower(name)
		snapshot.Databases = append(snapshot.Databases, databasePermission{Database: originalNames[key], Privileges: sortedPrivilegeNames(permissions[key])})
	}
	return snapshot, nil
}

func applyGrantPrivileges(grant string, databasePrivileges map[string]map[string]bool, global map[string]bool) {
	upper := strings.ToUpper(grant)
	if !strings.HasPrefix(upper, "GRANT ") {
		return
	}
	onIndex, toIndex := strings.Index(upper, " ON "), strings.Index(upper, " TO ")
	if onIndex < 0 || toIndex <= onIndex {
		return
	}
	privilegeNames := parsePrivilegeNames(grant[len("GRANT "):onIndex])
	scope := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(grant[onIndex+len(" ON "):toIndex]), "`", ""))
	if scope == "*.*" {
		for _, privilege := range privilegeNames {
			global[privilege] = true
		}
		return
	}
	if !strings.HasSuffix(scope, ".*") {
		return
	}
	if privileges, exists := databasePrivileges[strings.TrimSuffix(scope, ".*")]; exists {
		for _, privilege := range privilegeNames {
			privileges[privilege] = true
		}
	}
}

func parsePrivilegeNames(text string) []string {
	text = strings.TrimSpace(strings.ToUpper(text))
	if strings.Contains(text, "ALL PRIVILEGES") || text == "ALL" {
		return []string{"ALL PRIVILEGES"}
	}
	parts := strings.Split(text, ",")
	privileges := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := strings.TrimSpace(part); name != "" {
			privileges = append(privileges, name)
		}
	}
	return privileges
}

func sortedPrivilegeNames(privileges map[string]bool) []string {
	values := make([]string, 0, len(privileges))
	for privilege := range privileges {
		values = append(values, privilege)
	}
	sort.Strings(values)
	return values
}

func (s *server) currentDB() (*sql.DB, error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return nil, errors.New("请先在网页中连接 MySQL")
	}
	return db, nil
}

func (s *server) activeExecutor(db *sql.DB) (statementExecutor, bool, func()) {
	s.transactionMu.Lock()
	if s.activeTransaction != nil {
		if !transactionUsable(s.activeTransaction) {
			s.activeTransaction, s.transactionAt = nil, time.Time{}
			s.transactionMu.Unlock()
			return db, false, func() {}
		}
		return s.activeTransaction, true, s.transactionMu.Unlock
	}
	s.transactionMu.Unlock()
	return db, false, func() {}
}

func transactionUsable(tx *sql.Tx) bool {
	var probe int
	return tx != nil && tx.QueryRow("SELECT 1").Scan(&probe) == nil
}

func (s *server) rollbackActiveTransaction() {
	s.transactionMu.Lock()
	tx := s.activeTransaction
	s.activeTransaction, s.transactionAt = nil, time.Time{}
	s.transactionMu.Unlock()
	if tx != nil {
		_ = tx.Rollback()
	}
}

func (s *server) transaction(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.transactionMu.Lock()
		if s.activeTransaction != nil && !transactionUsable(s.activeTransaction) {
			s.activeTransaction, s.transactionAt = nil, time.Time{}
		}
		active, startedAt := s.activeTransaction != nil, s.transactionAt
		s.transactionMu.Unlock()
		response := map[string]any{"active": active}
		if active {
			response["startedAt"] = startedAt.Format(time.RFC3339)
		}
		writeJSON(w, http.StatusOK, response)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request transactionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "事务请求无效")
		return
	}
	action := strings.ToLower(strings.TrimSpace(request.Action))
	switch action {
	case "start":
		db, err := s.currentDB()
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.transactionMu.Lock()
		if s.activeTransaction != nil && !transactionUsable(s.activeTransaction) {
			s.activeTransaction, s.transactionAt = nil, time.Time{}
		}
		if s.activeTransaction != nil {
			s.transactionMu.Unlock()
			writeError(w, http.StatusConflict, "当前已有正在进行的事务")
			return
		}
		tx, err := db.BeginTx(r.Context(), nil)
		if err == nil {
			s.activeTransaction, s.transactionAt = tx, time.Now()
		}
		s.transactionMu.Unlock()
		if err != nil {
			writeDBError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"active": true, "message": "事务已开始"})
	case "commit", "rollback":
		s.transactionMu.Lock()
		tx := s.activeTransaction
		if tx == nil {
			s.transactionMu.Unlock()
			writeError(w, http.StatusBadRequest, "当前没有正在进行的事务")
			return
		}
		var err error
		if action == "commit" {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		s.activeTransaction, s.transactionAt = nil, time.Time{}
		s.transactionMu.Unlock()
		if err != nil {
			writeDBError(w, err)
			return
		}
		message := "事务已提交"
		if action == "rollback" {
			message = "事务已回滚"
		}
		writeJSON(w, http.StatusOK, map[string]any{"active": false, "message": message})
	default:
		writeError(w, http.StatusBadRequest, "不支持的事务操作")
	}
}

// verifyCurrentPassword opens a short-lived connection so destructive operations
// are protected by the password currently configured for this MySQL connection.
func (s *server) verifyCurrentPassword(password string) error {
	s.mu.RLock()
	host, port, user, currentPassword, params, connected := s.host, s.port, s.user, s.password, s.params, s.db != nil
	s.mu.RUnlock()
	if !connected {
		return errors.New("请先在网页中连接 MySQL")
	}
	if password == "" && currentPassword != "" {
		return errors.New("请输入当前 MySQL 连接密码以继续")
	}
	probe, err := openDatabase(host, port, user, password, params)
	if err != nil {
		return errors.New("当前 MySQL 连接密码验证失败")
	}
	return probe.Close()
}

func (s *server) databases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if databases, ok := s.cachedDatabases(); ok {
		writeJSON(w, http.StatusOK, map[string]any{"databases": databases})
		return
	}
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
	s.cacheDatabases(databases)
	writeJSON(w, 200, map[string]any{"databases": databases})
}

func (s *server) databaseOptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	charsetRows, err := db.Query(`SELECT CHARACTER_SET_NAME, DEFAULT_COLLATE_NAME FROM information_schema.CHARACTER_SETS ORDER BY CHARACTER_SET_NAME`)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer charsetRows.Close()
	charsets := make([]map[string]string, 0)
	for charsetRows.Next() {
		var name, defaultCollation string
		if err := charsetRows.Scan(&name, &defaultCollation); err != nil {
			writeDBError(w, err)
			return
		}
		charsets = append(charsets, map[string]string{"name": name, "defaultCollation": defaultCollation})
	}
	if err := charsetRows.Err(); err != nil {
		writeDBError(w, err)
		return
	}
	collationRows, err := db.Query(`SELECT CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLLATIONS ORDER BY CHARACTER_SET_NAME, COLLATION_NAME`)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer collationRows.Close()
	collations := make([]map[string]string, 0)
	for collationRows.Next() {
		var charset, name string
		if err := collationRows.Scan(&charset, &name); err != nil {
			writeDBError(w, err)
			return
		}
		collations = append(collations, map[string]string{"charset": charset, "name": name})
	}
	if err := collationRows.Err(); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"charsets": charsets, "collations": collations})
}

func (s *server) database(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		name, err := requiredIdentifier(r.URL.Query().Get("name"), "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if map[string]bool{"mysql": true, "information_schema": true, "performance_schema": true, "sys": true}[strings.ToLower(name)] {
			writeError(w, 400, "不能删除 MySQL 系统数据库")
			return
		}
		var payload passwordRequest
		if err := decodeJSON(r, &payload); err != nil {
			writeError(w, 400, "请重新输入当前 MySQL 连接密码")
			return
		}
		if err := s.verifyCurrentPassword(payload.Password); err != nil {
			writeError(w, 401, err.Error())
			return
		}
		db, err := s.currentDB()
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if _, err := db.Exec("DROP DATABASE " + quoteIdentifier(name)); err != nil {
			writeDBError(w, err)
			return
		}
		s.clearMetadataCache()
		writeJSON(w, 200, map[string]string{"message": "数据库已删除"})
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request createDatabaseRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "创建数据库请求无效")
		return
	}
	name, err := requiredIdentifier(request.Name, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	charset, err := requiredIdentifier(request.Charset, "字符集")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	collation, err := requiredIdentifier(request.Collation, "排序规则")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLLATIONS WHERE CHARACTER_SET_NAME=? AND COLLATION_NAME=?`, charset, collation).Scan(&exists); err != nil {
		writeDBError(w, err)
		return
	}
	if exists == 0 {
		writeError(w, 400, "排序规则不属于所选字符集")
		return
	}
	if _, err := db.Exec("CREATE DATABASE " + quoteIdentifier(name) + " CHARACTER SET " + quoteIdentifier(charset) + " COLLATE " + quoteIdentifier(collation)); err != nil {
		writeDBError(w, err)
		return
	}
	s.clearMetadataCache()
	writeJSON(w, 201, map[string]string{"message": "数据库已创建", "name": name})
}
func (s *server) tableCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(r.URL.Query().Get("table"), "数据表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if tables, ok := s.cachedTables(schema); ok {
		for _, item := range tables {
			if item.Name == table && item.RecordCount != nil && item.RecordCountExact {
				writeJSON(w, http.StatusOK, map[string]any{"recordCount": *item.RecordCount, "recordCountExact": true})
				return
			}
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualify(schema, table)).Scan(&count); err != nil {
		writeDBError(w, err)
		return
	}
	s.cacheTableCount(schema, table, count)
	writeJSON(w, http.StatusOK, map[string]any{"recordCount": count, "recordCountExact": true})
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
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	exactCounts := r.URL.Query().Get("includeCounts") == "1"
	if cached, ok := s.cachedTables(schema); ok {
		allExact := true
		for _, item := range cached {
			if item.Type == "BASE TABLE" && (!item.RecordCountExact || item.RecordCount == nil) {
				allExact = false
				break
			}
		}
		if !exactCounts || allExact {
			writeJSON(w, http.StatusOK, map[string]any{"tables": cached})
			return
		}
	}
	rows, err := db.QueryContext(r.Context(), `SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME`, schema)
	if err != nil {
		writeDBError(w, err)
		return
	}
	type listedTable struct {
		name      string
		kind      string
		estimated sql.NullInt64
	}
	entries := make([]listedTable, 0)
	for rows.Next() {
		var entry listedTable
		if err := rows.Scan(&entry.name, &entry.kind, &entry.estimated); err != nil {
			_ = rows.Close()
			writeDBError(w, err)
			return
		}
		entries = append(entries, entry)
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

	list := make([]cachedTable, len(entries))
	countIndexes := make([]int, 0)
	for index, entry := range entries {
		item := cachedTable{Name: entry.name, Type: entry.kind, RecordCountExact: entry.kind != "BASE TABLE"}
		if entry.estimated.Valid {
			count := entry.estimated.Int64
			item.RecordCount = &count
		}
		list[index] = item
		if exactCounts && entry.kind == "BASE TABLE" {
			countIndexes = append(countIndexes, index)
		}
	}
	if exactCounts && len(countIndexes) > 0 {
		// Bound the number of simultaneous COUNT(*) queries so large schemas refresh
		// quickly without opening an unbounded number of MySQL connections.
		workerCount := len(countIndexes)
		if workerCount > 6 {
			workerCount = 6
		}
		type countResult struct {
			index int
			count int64
			err   error
		}
		jobs := make(chan int)
		results := make(chan countResult)
		var workers sync.WaitGroup
		for worker := 0; worker < workerCount; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for index := range jobs {
					var count int64
					err := db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM "+qualify(schema, entries[index].name)).Scan(&count)
					results <- countResult{index: index, count: count, err: err}
				}
			}()
		}
		go func() {
			for _, index := range countIndexes {
				jobs <- index
			}
			close(jobs)
		}()
		go func() {
			workers.Wait()
			close(results)
		}()
		for result := range results {
			if result.err != nil {
				continue // Keep the TABLE_ROWS estimate already populated above.
			}
			count := result.count
			list[result.index].RecordCount = &count
			list[result.index].RecordCountExact = true
		}
	}
	s.cacheTables(schema, list)
	writeJSON(w, 200, map[string]any{"tables": list})
}

func (s *server) table(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		name, err := requiredIdentifier(r.URL.Query().Get("table"), "表")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		var payload passwordRequest
		if err := decodeJSON(r, &payload); err != nil {
			writeError(w, 400, "请重新输入当前 MySQL 连接密码")
			return
		}
		if err := s.verifyCurrentPassword(payload.Password); err != nil {
			writeError(w, 401, err.Error())
			return
		}
		db, err := s.currentDB()
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if _, err := db.Exec("DROP TABLE " + qualify(schema, name)); err != nil {
			writeDBError(w, err)
			return
		}
		s.clearMetadataCache()
		writeJSON(w, 200, map[string]string{"message": "数据表已删除"})
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request createTableRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "创建表请求无效")
		return
	}
	schema, err := requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	name, err := requiredIdentifier(request.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if len(request.Columns) == 0 || len(request.Columns) > 128 {
		writeError(w, 400, "字段数量必须在 1 到 128 之间")
		return
	}
	definitions, primaryKeys, seen := make([]string, 0, len(request.Columns)+1), make([]string, 0), map[string]bool{}
	for _, column := range request.Columns {
		columnName, err := requiredIdentifier(column.Name, "字段")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if seen[columnName] {
			writeError(w, 400, "字段名称重复: "+columnName)
			return
		}
		seen[columnName] = true
		columnType, err := validMySQLColumnType(column.Type)
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		definition := quoteIdentifier(columnName) + " " + columnType
		if column.Nullable && !column.Primary && !column.AutoIncrement {
			definition += " NULL"
		} else {
			definition += " NOT NULL"
		}
		if column.AutoIncrement {
			definition += " AUTO_INCREMENT"
		}
		if len(column.Comment) > 1024 {
			writeError(w, 400, "字段注释不能超过 1024 个字符")
			return
		}
		if comment := strings.TrimSpace(column.Comment); comment != "" {
			definition += " COMMENT " + quoteSQL(comment)
		}
		definitions = append(definitions, definition)
		if column.Primary {
			primaryKeys = append(primaryKeys, quoteIdentifier(columnName))
		}
	}
	if len(primaryKeys) > 0 {
		definitions = append(definitions, "PRIMARY KEY ("+strings.Join(primaryKeys, ",")+")")
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	statement := "CREATE TABLE " + qualify(schema, name) + " (" + strings.Join(definitions, ",") + ")"
	if engine := strings.TrimSpace(request.Engine); engine != "" {
		engine, err = requiredIdentifier(engine, "存储引擎")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		statement += " ENGINE=" + quoteIdentifier(engine)
	}
	if charset := strings.TrimSpace(request.Charset); charset != "" {
		charset, err = requiredIdentifier(charset, "字符集")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		statement += " DEFAULT CHARACTER SET " + quoteIdentifier(charset)
	}
	if collation := strings.TrimSpace(request.Collation); collation != "" {
		collation, err = requiredIdentifier(collation, "排序规则")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		statement += " COLLATE " + quoteIdentifier(collation)
	}
	if len(request.Comment) > 2048 {
		writeError(w, 400, "表注释不能超过 2048 个字符")
		return
	}
	if comment := strings.TrimSpace(request.Comment); comment != "" {
		statement += " COMMENT=" + quoteSQL(comment)
	}
	if _, err := db.Exec(statement); err != nil {
		writeDBError(w, err)
		return
	}
	s.clearMetadataCache()
	writeJSON(w, 201, map[string]string{"message": "数据表已创建", "name": name})
}
func countIndexHint(db *sql.DB, schema, table string, q url.Values) string {
	required := map[string]bool{}
	var filters []filterCondition
	if raw := strings.TrimSpace(q.Get("filters")); raw != "" {
		_ = json.Unmarshal([]byte(raw), &filters)
	} else if name := strings.TrimSpace(q.Get("filterColumn")); name != "" {
		filters = []filterCondition{{Column: name}}
	}
	for _, filter := range filters {
		if name := strings.TrimSpace(filter.Column); name != "" {
			required[name] = true
		}
	}
	rows, err := db.Query(`SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, schema, table)
	if err != nil {
		return ""
	}
	defer rows.Close()
	indexes := map[string]map[string]bool{}
	widths := map[string]int{}
	for rows.Next() {
		var name, column string
		if rows.Scan(&name, &column) != nil {
			return ""
		}
		if indexes[name] == nil {
			indexes[name] = map[string]bool{}
		}
		indexes[name][column] = true
		widths[name]++
	}
	best := ""
	bestWidth := int(^uint(0) >> 1)
	for name, columns := range indexes {
		covers := true
		for column := range required {
			if !columns[column] {
				covers = false
				break
			}
		}
		if covers && widths[name] < bestWidth {
			best, bestWidth = name, widths[name]
		}
	}
	if best == "" {
		return ""
	}
	return " USE INDEX (" + quoteIdentifier(best) + ")"
}

func (s *server) rows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, table, meta, err := s.requestMeta(r)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	q := r.URL.Query()
	limit := clampInt(q.Get("limit"), 100, 1, 500)
	offset := clampInt(q.Get("offset"), 0, 0, 100000000)
	where, args, err := buildFilters(q, meta)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	filterWhere := where
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	executor, _, release := s.activeExecutor(db)
	defer release()
	sort, err := buildSort(q, meta)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	qualified := qualify(schema, table)
	var total int64
	if err := executor.QueryRow("SELECT COUNT(*) FROM "+qualified+countIndexHint(db, schema, table, q)+where, args...).Scan(&total); err != nil {
		writeDBError(w, err)
		return
	}

	// Cursor paging avoids costly deep OFFSET scans on tables with primary keys.
	cursorMode := sort == "" && len(meta.PrimaryKeys) > 0
	if cursorMode {
		order := make([]string, len(meta.PrimaryKeys))
		for i, key := range meta.PrimaryKeys {
			order[i] = quoteIdentifier(key) + " ASC"
		}
		sort = " ORDER BY " + strings.Join(order, ",")
		if token := strings.TrimSpace(q.Get("cursor")); token != "" {
			values, err := decodeCursor(token, len(meta.PrimaryKeys))
			if err != nil {
				writeError(w, 400, "分页游标无效")
				return
			}
			seek, seekArgs := primaryCursorCondition(meta.PrimaryKeys, values)
			where = appendWhere(where, seek)
			args = append(args, seekArgs...)
		}
	}
	query := "SELECT * FROM " + qualified + where + sort + " LIMIT ?"
	args = append(args, limit+1)
	if !cursorMode {
		query += " OFFSET ?"
		args = append(args, offset)
	}
	queryPreview := ""
	if filterWhere != "" {
		queryPreview = query
		for _, arg := range args {
			queryPreview = strings.Replace(queryPreview, "?", sqlLiteral(arg), 1)
		}
	}
	result, err := executor.Query(query, args...)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer result.Close()
	columnNames, err := result.Columns()
	if err != nil {
		writeDBError(w, err)
		return
	}
	keyIndexes := make([]int, len(meta.PrimaryKeys))
	for i, key := range meta.PrimaryKeys {
		keyIndexes[i] = -1
		for index, name := range columnNames {
			if name == key {
				keyIndexes[i] = index
				break
			}
		}
	}
	data := make([]map[string]any, 0, limit)
	var lastKeyValues []any
	hasMore := false
	for result.Next() {
		values := make([]any, len(columnNames))
		pointers := make([]any, len(columnNames))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := result.Scan(pointers...); err != nil {
			writeDBError(w, err)
			return
		}
		if len(data) == limit {
			hasMore = true
			break
		}
		item := make(map[string]any, len(columnNames))
		for i, name := range columnNames {
			item[name] = jsonValue(values[i])
		}
		if cursorMode {
			lastKeyValues = make([]any, len(keyIndexes))
			for i, index := range keyIndexes {
				if index < 0 {
					writeError(w, 500, "无法读取主键字段")
					return
				}
				lastKeyValues[i] = jsonValue(values[index])
			}
		}
		data = append(data, item)
	}
	if err := result.Err(); err != nil {
		writeDBError(w, err)
		return
	}
	nextCursor := ""
	if cursorMode && hasMore && len(lastKeyValues) > 0 {
		nextCursor, err = encodeCursor(lastKeyValues)
		if err != nil {
			writeError(w, 500, "无法创建分页游标")
			return
		}
	}
	writeJSON(w, 200, map[string]any{"columns": meta.Columns, "primaryKeys": meta.PrimaryKeys, "rows": data, "limit": limit, "offset": offset, "total": total, "hasMore": hasMore, "cursorMode": cursorMode, "nextCursor": nextCursor, "queryPreview": queryPreview})
}
func (s *server) row(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodDelete {
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
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	executor, _, release := s.activeExecutor(db)
	defer release()
	allowed := map[string]bool{}
	for _, col := range meta.Columns {
		allowed[col.Name] = true
	}
	if r.Method == http.MethodPost {
		columns, values, placeholders := make([]string, 0, len(payload.Data)), make([]any, 0, len(payload.Data)), make([]string, 0, len(payload.Data))
		for _, col := range meta.Columns {
			value, present := payload.Data[col.Name]
			if !present {
				continue
			}
			columns = append(columns, quoteIdentifier(col.Name))
			values = append(values, value)
			placeholders = append(placeholders, "?")
		}
		for name := range payload.Data {
			if !allowed[name] {
				writeError(w, 400, "字段不存在: "+name)
				return
			}
		}
		statement := "INSERT INTO " + qualify(schema, table)
		if len(columns) == 0 {
			statement += " () VALUES ()"
		} else {
			statement += " (" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")"
		}
		result, err := executor.ExecContext(r.Context(), statement, values...)
		if err != nil {
			writeDBError(w, err)
			return
		}
		id, _ := result.LastInsertId()
		s.invalidateCachedTableCount(schema, table)
		writeJSON(w, http.StatusCreated, map[string]any{"message": "记录已新增", "lastInsertId": id})
		return
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
		res, err := executor.Exec("DELETE FROM "+qualify(schema, table)+where, whereArgs...)
		if err != nil {
			writeDBError(w, err)
			return
		}
		n, _ := res.RowsAffected()
		s.invalidateCachedTableCount(schema, table)
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
	res, err := executor.Exec("UPDATE "+qualify(schema, table)+" SET "+strings.Join(setParts, ",")+where, args...)
	if err != nil {
		writeDBError(w, err)
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, 200, map[string]any{"updated": n})
}

func (s *server) deleteRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var payload batchDeleteRequest
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, 400, "批量删除请求无效")
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
	if len(payload.PrimaryKeys) == 0 || len(payload.PrimaryKeys) > 500 {
		writeError(w, 400, "请选择 1 到 500 条记录")
		return
	}
	meta, err := s.getMeta(schema, table)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	allowed := map[string]bool{}
	for _, col := range meta.Columns {
		allowed[col.Name] = true
	}
	keys := meta.PrimaryKeys
	if len(keys) == 0 {
		keys = make([]string, 0, len(meta.Columns))
		for _, col := range meta.Columns {
			keys = append(keys, col.Name)
		}
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	executor, transactionActive, release := s.activeExecutor(db)
	defer release()
	var localTx *sql.Tx
	if !transactionActive {
		localTx, err = db.BeginTx(r.Context(), nil)
		if err != nil {
			writeDBError(w, err)
			return
		}
		executor = localTx
	}
	var deleted int64
	for _, primaryKey := range payload.PrimaryKeys {
		where, args, err := primaryWhere(keys, primaryKey, allowed)
		if err != nil {
			if localTx != nil {
				_ = localTx.Rollback()
			}
			writeError(w, 400, err.Error())
			return
		}
		result, err := executor.Exec("DELETE FROM "+qualify(schema, table)+where, args...)
		if err != nil {
			if localTx != nil {
				_ = localTx.Rollback()
			}
			writeDBError(w, err)
			return
		}
		count, _ := result.RowsAffected()
		deleted += count
	}
	if localTx != nil {
		if err := localTx.Commit(); err != nil {
			_ = localTx.Rollback()
			writeDBError(w, err)
			return
		}
	}
	s.invalidateCachedTableCount(schema, table)
	writeJSON(w, 200, map[string]any{"deleted": deleted})
}

var mysqlColumnTypeNames = map[string]bool{
	"BIT": true, "BOOL": true, "BOOLEAN": true, "TINYINT": true, "SMALLINT": true, "MEDIUMINT": true, "INT": true, "INTEGER": true, "BIGINT": true,
	"DECIMAL": true, "DEC": true, "NUMERIC": true, "FIXED": true, "FLOAT": true, "DOUBLE": true, "REAL": true,
	"DATE": true, "DATETIME": true, "TIMESTAMP": true, "TIME": true, "YEAR": true,
	"CHAR": true, "VARCHAR": true, "BINARY": true, "VARBINARY": true, "TINYBLOB": true, "BLOB": true, "MEDIUMBLOB": true, "LONGBLOB": true,
	"TINYTEXT": true, "TEXT": true, "MEDIUMTEXT": true, "LONGTEXT": true, "ENUM": true, "SET": true, "JSON": true,
	"GEOMETRY": true, "POINT": true, "LINESTRING": true, "POLYGON": true, "MULTIPOINT": true, "MULTILINESTRING": true, "MULTIPOLYGON": true, "GEOMETRYCOLLECTION": true,
}

func validMySQLColumnType(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 256 || strings.ContainsAny(value, ";\r\n\\") || strings.Contains(value, "--") || strings.Contains(value, "/*") || strings.Contains(value, "*/") {
		return "", errors.New("字段类型格式无效")
	}
	base := strings.ToUpper(strings.Fields(value)[0])
	if index := strings.IndexByte(base, '('); index >= 0 {
		base = base[:index]
	}
	if !mysqlColumnTypeNames[base] {
		return "", errors.New("不支持的 MySQL 字段类型")
	}
	for _, char := range value {
		if !(char == '(' || char == ')' || char == ',' || char == '\'' || char == ' ' || char == '\t' || char == '_' || (char >= '0' && char <= '9') || (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z')) {
			return "", errors.New("字段类型只能包含 MySQL 类型、参数和修饰符")
		}
	}
	return value, nil
}

func isCharacterColumnType(value string) bool {
	base := strings.ToUpper(strings.Fields(value)[0])
	if index := strings.IndexByte(base, '('); index >= 0 {
		base = base[:index]
	}
	return map[string]bool{"CHAR": true, "VARCHAR": true, "TINYTEXT": true, "TEXT": true, "MEDIUMTEXT": true, "LONGTEXT": true, "ENUM": true, "SET": true}[base]
}

func (s *server) columnType(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request columnTypeRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "字段修改请求无效")
		return
	}
	schema, err := requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(request.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	columnName, err := requiredIdentifier(request.Column, "字段")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	columnType, err := validMySQLColumnType(request.Type)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	meta, err := s.getMeta(schema, table)
	if err != nil {
		writeDBError(w, err)
		return
	}
	var currentType, nullable, extra, characterSet, collation, comment string
	var defaultValue sql.NullString
	err = db.QueryRow(`SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COALESCE(CHARACTER_SET_NAME,''), COALESCE(COLLATION_NAME,''), COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, schema, table, columnName).Scan(&currentType, &nullable, &defaultValue, &extra, &characterSet, &collation, &comment)
	if err == sql.ErrNoRows {
		writeError(w, 404, "找不到指定字段")
		return
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	currentNullable := nullable == "YES"
	targetNullable := currentNullable
	if request.Nullable != nil {
		targetNullable = *request.Nullable
	}
	targetPrimary := false
	for _, item := range meta.Columns {
		if item.Name == columnName {
			targetPrimary = item.Primary
			break
		}
	}
	if request.Primary != nil {
		targetPrimary = *request.Primary
	}
	if targetPrimary {
		targetNullable = false
	}
	targetName := columnName
	if strings.TrimSpace(request.NewName) != "" {
		targetName, err = requiredIdentifier(request.NewName, "新字段")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	nameChanged := targetName != columnName
	typeChanged := !strings.EqualFold(strings.TrimSpace(currentType), strings.TrimSpace(columnType))
	if !nameChanged && !typeChanged && targetNullable == currentNullable && request.Primary == nil {
		writeJSON(w, 200, map[string]string{"message": "字段定义未发生变化"})
		return
	}
	if typeChanged && !request.Force {
		var count int64
		if err := db.QueryRow("SELECT COUNT(*) FROM " + qualify(schema, table)).Scan(&count); err != nil {
			writeDBError(w, err)
			return
		}
		if count > 0 {
			message := fmt.Sprintf("该字段所在表已有 %d 条历史数据。转换为 %s 可能因数据不匹配而截断、失败或改变数据，请确认后继续。", count, columnType)
			writeJSON(w, http.StatusConflict, map[string]any{"warning": true, "rowCount": count, "message": message, "error": message})
			return
		}
	}
	if strings.Contains(strings.ToLower(extra), "generated") {
		writeError(w, 400, "暂不支持修改生成列的定义")
		return
	}
	definition := columnType
	if targetNullable {
		definition += " NULL"
	} else {
		definition += " NOT NULL"
	}
	if defaultValue.Valid {
		definition += " DEFAULT " + columnDefaultLiteral(defaultValue.String)
	}
	if strings.Contains(strings.ToLower(extra), "auto_increment") {
		definition += " AUTO_INCREMENT"
	}
	if strings.Contains(strings.ToLower(extra), "on update current_timestamp") {
		definition += " ON UPDATE CURRENT_TIMESTAMP"
	}
	if isCharacterColumnType(columnType) && characterSet != "" {
		definition += " CHARACTER SET " + quoteIdentifier(characterSet)
	}
	if isCharacterColumnType(columnType) && collation != "" {
		definition += " COLLATE " + quoteIdentifier(collation)
	}
	if comment != "" {
		definition += " COMMENT " + quoteSQL(comment)
	}
	if nameChanged || typeChanged || targetNullable != currentNullable {
		statement := "ALTER TABLE " + qualify(schema, table)
		if nameChanged {
			statement += " CHANGE COLUMN " + quoteIdentifier(columnName) + " " + quoteIdentifier(targetName) + " " + definition
		} else {
			statement += " MODIFY COLUMN " + quoteIdentifier(columnName) + " " + definition
		}
		if _, err := db.Exec(statement); err != nil {
			writeDBError(w, err)
			return
		}
	}
	if request.Primary != nil {
		keys := make([]string, 0, len(meta.PrimaryKeys)+1)
		for _, key := range meta.PrimaryKeys {
			if key != columnName {
				keys = append(keys, key)
			}
		}
		if targetPrimary {
			keys = append(keys, targetName)
		}
		currentKeys := strings.Join(meta.PrimaryKeys, "\x00")
		targetKeys := strings.Join(keys, "\x00")
		if currentKeys != targetKeys {
			statement := "ALTER TABLE " + qualify(schema, table)
			if len(meta.PrimaryKeys) > 0 {
				statement += " DROP PRIMARY KEY"
			}
			if len(keys) > 0 {
				quoted := make([]string, len(keys))
				for i, key := range keys {
					quoted[i] = quoteIdentifier(key)
				}
				if len(meta.PrimaryKeys) > 0 {
					statement += ","
				}
				statement += " ADD PRIMARY KEY (" + strings.Join(quoted, ",") + ")"
			}
			if _, err := db.Exec(statement); err != nil {
				writeDBError(w, err)
				return
			}
		}
	}
	message := "字段定义已更新"
	if nameChanged {
		message = "字段已重命名并保存"
	}
	s.clearMetadataCache()
	writeJSON(w, 200, map[string]string{"message": message, "name": targetName})
}

func (s *server) tableAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request tableActionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "表操作请求无效")
		return
	}
	schema, err := requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(request.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	switch strings.ToLower(strings.TrimSpace(request.Action)) {
	case "truncate":
		if _, err := db.Exec("TRUNCATE TABLE " + qualify(schema, table)); err != nil {
			writeDBError(w, err)
			return
		}
		s.invalidateCachedTableCount(schema, table)
		writeJSON(w, 200, map[string]string{"message": "数据表已清空"})
	case "rename":
		newName, err := requiredIdentifier(request.NewName, "新表")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if newName == table {
			writeError(w, 400, "新表名不能与原表名相同")
			return
		}
		if _, err := db.Exec("RENAME TABLE " + qualify(schema, table) + " TO " + qualify(schema, newName)); err != nil {
			writeDBError(w, err)
			return
		}
		s.clearMetadataCache()
		writeJSON(w, 200, map[string]string{"message": "数据表已重命名", "name": newName})
	default:
		writeError(w, 400, "不支持的表操作")
	}
}

func (s *server) tableColumn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	var request tableColumnRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "字段操作请求无效")
		return
	}
	schema, err := requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(request.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	name, err := requiredIdentifier(request.Name, "字段")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if r.Method == http.MethodDelete {
		if _, err := db.Exec("ALTER TABLE " + qualify(schema, table) + " DROP COLUMN " + quoteIdentifier(name)); err != nil {
			writeDBError(w, err)
			return
		}
		writeJSON(w, 200, map[string]string{"message": "字段已删除"})
		return
	}
	columnType, err := validMySQLColumnType(request.Type)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	definition := quoteIdentifier(name) + " " + columnType
	if request.Nullable && !request.AutoIncrement {
		definition += " NULL"
	} else {
		definition += " NOT NULL"
	}
	if request.AutoIncrement {
		definition += " AUTO_INCREMENT"
	}
	if _, err := db.Exec("ALTER TABLE " + qualify(schema, table) + " ADD COLUMN " + definition); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 201, map[string]string{"message": "字段已新增"})
}

func (s *server) tableCreateSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(r.URL.Query().Get("table"), "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	var ignored, createSQL string
	if err := db.QueryRow("SHOW CREATE TABLE "+qualify(schema, table)).Scan(&ignored, &createSQL); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"createSQL": createSQL})
}

func (s *server) tableInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	table, err := requiredIdentifier(r.URL.Query().Get("table"), "表")
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	executor, _, release := s.activeExecutor(db)
	defer release()
	var rowCount int64
	if err := executor.QueryRow("SELECT COUNT(*) FROM " + qualify(schema, table)).Scan(&rowCount); err != nil {
		writeDBError(w, err)
		return
	}
	info := struct {
		Engine, AutoIncrement, RowFormat, UpdateTime, CreateTime, CheckTime string
		IndexLength, DataLength, MaxDataLength, DataFree                    int64
		Collation, CreateOptions, Comment                                   string
	}{}
	err = db.QueryRow(`SELECT COALESCE(ENGINE,''), COALESCE(AUTO_INCREMENT,0), COALESCE(ROW_FORMAT,''),
		COALESCE(DATE_FORMAT(UPDATE_TIME,'%Y-%m-%d %H:%i:%s'),''), COALESCE(DATE_FORMAT(CREATE_TIME,'%Y-%m-%d %H:%i:%s'),''), COALESCE(DATE_FORMAT(CHECK_TIME,'%Y-%m-%d %H:%i:%s'),''),
		COALESCE(INDEX_LENGTH,0), COALESCE(DATA_LENGTH,0), COALESCE(MAX_DATA_LENGTH,0), COALESCE(DATA_FREE,0), COALESCE(TABLE_COLLATION,''), COALESCE(CREATE_OPTIONS,''), COALESCE(TABLE_COMMENT,'')
		FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, schema, table).Scan(
		&info.Engine, &info.AutoIncrement, &info.RowFormat, &info.UpdateTime, &info.CreateTime, &info.CheckTime,
		&info.IndexLength, &info.DataLength, &info.MaxDataLength, &info.DataFree, &info.Collation, &info.CreateOptions, &info.Comment,
	)
	if err == sql.ErrNoRows {
		writeError(w, http.StatusNotFound, "找不到指定的数据表")
		return
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema": schema, "table": table, "rows": rowCount,
		"engine": info.Engine, "autoIncrement": info.AutoIncrement, "rowFormat": info.RowFormat,
		"updateTime": info.UpdateTime, "createTime": info.CreateTime, "checkTime": info.CheckTime,
		"indexLength": info.IndexLength, "dataLength": info.DataLength, "maxDataLength": info.MaxDataLength, "dataFree": info.DataFree,
		"collation": info.Collation, "createOptions": info.CreateOptions, "comment": info.Comment,
	})
}

func (s *server) tableIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		schema, err := requiredIdentifier(r.URL.Query().Get("schema"), "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		table, err := requiredIdentifier(r.URL.Query().Get("table"), "表")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		db, err := s.currentDB()
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		rows, err := db.Query(`SELECT INDEX_NAME, CASE WHEN NON_UNIQUE=0 THEN 'UNIQUE' ELSE INDEX_TYPE END, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME<>'PRIMARY' GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE ORDER BY INDEX_NAME`, schema, table)
		if err != nil {
			writeDBError(w, err)
			return
		}
		defer rows.Close()
		indexes := make([]map[string]string, 0)
		for rows.Next() {
			var name, kind, columns string
			if err := rows.Scan(&name, &kind, &columns); err != nil {
				writeDBError(w, err)
				return
			}
			indexes = append(indexes, map[string]string{"name": name, "type": kind, "columns": columns})
		}
		if err := rows.Err(); err != nil {
			writeDBError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"indexes": indexes})
		return
	}
	if r.Method == http.MethodDelete {
		var request createIndexRequest
		if err := decodeJSON(r, &request); err != nil {
			writeError(w, 400, "删除索引请求无效")
			return
		}
		schema, err := requiredIdentifier(request.Schema, "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		table, err := requiredIdentifier(request.Table, "表")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		name, err := requiredIdentifier(request.Name, "索引")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if strings.EqualFold(name, "PRIMARY") {
			writeError(w, 400, "请通过设计表功能管理主键")
			return
		}
		db, err := s.currentDB()
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
		if _, err := db.Exec("ALTER TABLE " + qualify(schema, table) + " DROP INDEX " + quoteIdentifier(name)); err != nil {
			writeDBError(w, err)
			return
		}
		writeJSON(w, 200, map[string]string{"message": "索引已删除"})
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request createIndexRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "创建索引请求无效")
		return
	}
	schema, err := requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	table, err := requiredIdentifier(request.Table, "表")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	name, err := requiredIdentifier(request.Name, "索引")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	kind := strings.ToUpper(strings.TrimSpace(request.Type))
	if kind == "" {
		kind = "INDEX"
	}
	if !map[string]bool{"INDEX": true, "UNIQUE": true, "FULLTEXT": true, "SPATIAL": true}[kind] {
		writeError(w, 400, "不支持的索引类型")
		return
	}
	if len(request.Columns) == 0 || len(request.Columns) > 16 {
		writeError(w, 400, "请选择 1 到 16 个索引字段")
		return
	}
	meta, err := s.getMeta(schema, table)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	allowed := map[string]bool{}
	for _, column := range meta.Columns {
		allowed[column.Name] = true
	}
	columns := make([]string, 0, len(request.Columns))
	seen := map[string]bool{}
	for _, value := range request.Columns {
		column, err := requiredIdentifier(value, "索引字段")
		if err != nil || !allowed[column] || seen[column] {
			writeError(w, 400, "索引字段无效或重复")
			return
		}
		seen[column] = true
		columns = append(columns, quoteIdentifier(column))
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if _, err := db.Exec("ALTER TABLE " + qualify(schema, table) + " ADD " + kind + " " + quoteIdentifier(name) + " (" + strings.Join(columns, ",") + ")"); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 201, map[string]string{"message": "索引已创建"})
}

func columnDefaultLiteral(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	if lower == "current_timestamp" || lower == "current_timestamp()" || strings.HasPrefix(lower, "current_timestamp(") {
		return value
	}
	return quoteSQL(value)
}

func (s *server) executeSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request sqlRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "SQL 请求无效: "+err.Error())
		return
	}
	statement := strings.TrimSpace(request.SQL)
	if statement == "" {
		writeError(w, 400, "请输入 SQL")
		return
	}
	if len(statement) > 2<<20 {
		writeError(w, 400, "SQL 不能超过 2 MB")
		return
	}
	if request.Schema != "" {
		var err error
		request.Schema, err = requiredIdentifier(request.Schema, "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer conn.Close()
	if request.Schema != "" {
		if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(request.Schema)); err != nil {
			writeDBError(w, err)
			return
		}
	}
	startedAt := time.Now()
	if !sqlReturnsRows(statement) {
		result, err := conn.ExecContext(ctx, statement)
		if err != nil {
			writeDBError(w, err)
			return
		}
		affected, _ := result.RowsAffected()
		s.clearMetadataCache()
		writeJSON(w, 200, map[string]any{"message": "SQL 执行成功", "affectedRows": affected, "durationMs": float64(time.Since(startedAt).Microseconds()) / 1000})
		return
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset := request.Offset
	if offset < 0 {
		offset = 0
	}
	query := strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	serverPaged := !regexp.MustCompile(`(?i)\blimit\b`).MatchString(query) && !regexp.MustCompile(`(?i)\bfor\s+update\b`).MatchString(query)
	total := int64(-1)
	if serverPaged {
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+query+") AS `__mysql_manager_count`").Scan(&total); err != nil {
			total = -1
		}
	}
	var rows *sql.Rows
	if serverPaged {
		rows, err = conn.QueryContext(ctx, query+" LIMIT ? OFFSET ?", limit+1, offset)
	} else {
		rows, err = conn.QueryContext(ctx, statement)
	}
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		writeDBError(w, err)
		return
	}
	data := make([]map[string]any, 0, limit)
	hasMore, skipped := false, 0
	for rows.Next() {
		values, refs := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			refs[i] = &values[i]
		}
		if err := rows.Scan(refs...); err != nil {
			writeDBError(w, err)
			return
		}
		if !serverPaged && skipped < offset {
			skipped++
			continue
		}
		if len(data) >= limit {
			hasMore = true
			break
		}
		item := make(map[string]any, len(columns))
		for i, name := range columns {
			item[name] = jsonValue(values[i])
		}
		data = append(data, item)
	}
	if err := rows.Err(); err != nil {
		writeDBError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"message": "查询执行成功", "columns": columns, "rows": data, "hasMore": hasMore, "limit": limit, "offset": offset, "total": total, "durationMs": float64(time.Since(startedAt).Microseconds()) / 1000})
}

func csvCell(value any) string {
	value = jsonValue(value)
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func (s *server) exportSQLQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request sqlExportRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "SQL 导出请求无效")
		return
	}
	statement := strings.TrimSpace(request.SQL)
	if statement == "" || !sqlReturnsRows(statement) {
		writeError(w, 400, "只能导出返回记录的查询 SQL")
		return
	}
	if len(statement) > 2<<20 {
		writeError(w, 400, "SQL 不能超过 2 MB")
		return
	}
	query := strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	if strings.Contains(query, ";") {
		writeError(w, 400, "导出 SQL 仅支持单条查询语句")
		return
	}
	if request.Schema != "" {
		var err error
		request.Schema, err = requiredIdentifier(request.Schema, "数据库")
		if err != nil {
			writeError(w, 400, err.Error())
			return
		}
	}
	if len(request.Columns) == 0 {
		writeError(w, 400, "请至少选择一个导出字段")
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer conn.Close()
	if request.Schema != "" {
		if _, err := conn.ExecContext(ctx, "USE "+quoteIdentifier(request.Schema)); err != nil {
			writeDBError(w, err)
			return
		}
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	allColumns, err := rows.Columns()
	if err != nil {
		writeDBError(w, err)
		return
	}
	indexes := make(map[string]int, len(allColumns))
	for index, name := range allColumns {
		if _, exists := indexes[name]; !exists {
			indexes[name] = index
		}
	}
	selectedIndexes := make([]int, 0, len(request.Columns))
	selectedColumns := make([]string, 0, len(request.Columns))
	seen := map[string]bool{}
	for _, name := range request.Columns {
		index, exists := indexes[name]
		if !exists {
			writeError(w, 400, "导出字段不存在: "+name)
			return
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		selectedIndexes, selectedColumns = append(selectedIndexes, index), append(selectedColumns, name)
	}
	filename := "sql_query_" + time.Now().Format("20060102_150405") + ".csv"
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
	if _, err := file.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
		return
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(selectedColumns); err != nil {
		writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
		return
	}
	for rows.Next() {
		values, refs := make([]any, len(allColumns)), make([]any, len(allColumns))
		for i := range values {
			refs[i] = &values[i]
		}
		if err := rows.Scan(refs...); err != nil {
			writeDBError(w, err)
			return
		}
		record := make([]string, len(selectedIndexes))
		for i, index := range selectedIndexes {
			record[i] = csvCell(values[index])
		}
		if err := writer.Write(record); err != nil {
			writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		writeError(w, http.StatusInternalServerError, "写入导出文件失败: "+err.Error())
		return
	}
	if err := rows.Err(); err != nil {
		writeDBError(w, err)
		return
	}
	completed = finishExportFile(w, file, path)
}

func (s *server) sqlComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request sqlCompletionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, 400, "补全请求无效")
		return
	}
	request.Schema = strings.TrimSpace(request.Schema)
	if request.Schema == "" {
		writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}})
		return
	}
	var err error
	request.Schema, err = requiredIdentifier(request.Schema, "数据库")
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	db, err := s.currentDB()
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if match := sqlCompletionTail.FindStringSubmatch(request.SQL); len(match) == 3 {
		qualifier, prefix := cleanSQLIdentifier(match[1]), match[2]
		schema, table := resolveSQLReference(request.SQL, request.Schema, qualifier)
		if qualifier == "" || schema == "" || table == "" {
			writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}})
			return
		}
		rows, err := db.Query(`SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME LIKE ? ORDER BY COLUMN_NAME LIMIT 50`, schema, table, prefix+"%")
		if err != nil {
			writeDBError(w, err)
			return
		}
		defer rows.Close()
		suggestions := make([]completionSuggestion, 0)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				writeDBError(w, err)
				return
			}
			suggestions = append(suggestions, completionSuggestion{Value: qualifier + "." + name, Kind: "字段"})
		}
		writeJSON(w, 200, map[string]any{"suggestions": suggestions})
		return
	}
	match := sqlWordTail.FindStringSubmatch(request.SQL)
	if len(match) != 2 {
		writeJSON(w, 200, map[string]any{"suggestions": []completionSuggestion{}})
		return
	}
	prefix := match[1]
	rows, err := db.Query(`SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME LIMIT 500`, request.Schema)
	if err != nil {
		writeDBError(w, err)
		return
	}
	defer rows.Close()
	suggestions := make([]completionSuggestion, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			writeDBError(w, err)
			return
		}
		if tableNameMatchesCompletion(name, prefix) {
			suggestions = append(suggestions, completionSuggestion{Value: name, Kind: "表"})
		}
	}
	for _, keyword := range []string{"SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "ON", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE", "ALTER TABLE", "DROP TABLE", "AS", "AND", "OR", "NOT", "IN", "LIKE", "IS NULL"} {
		if strings.HasPrefix(strings.ToLower(keyword), strings.ToLower(prefix)) {
			suggestions = append(suggestions, completionSuggestion{Value: keyword, Kind: "关键字"})
		}
	}
	sort.Slice(suggestions, func(i, j int) bool {
		if suggestions[i].Kind != suggestions[j].Kind {
			return suggestions[i].Kind < suggestions[j].Kind
		}
		return strings.ToLower(suggestions[i].Value) < strings.ToLower(suggestions[j].Value)
	})
	writeJSON(w, 200, map[string]any{"suggestions": suggestions})
}

func tableNameMatchesCompletion(name, prefix string) bool {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	if prefix == "" || strings.HasPrefix(strings.ToLower(name), prefix) {
		return true
	}
	initials := make([]rune, 0, len(name))
	var previous rune
	for index, current := range []rune(name) {
		if index == 0 || previous == '_' || previous == '-' || (previous >= 'a' && previous <= 'z' && current >= 'A' && current <= 'Z') {
			initials = append(initials, current)
		}
		previous = current
	}
	return strings.HasPrefix(strings.ToLower(string(initials)), prefix)
}

func sqlReturnsRows(statement string) bool {
	text := strings.TrimSpace(statement)
	for {
		if strings.HasPrefix(text, "--") {
			if newline := strings.IndexByte(text, '\n'); newline >= 0 {
				text = strings.TrimSpace(text[newline+1:])
				continue
			}
		}
		if strings.HasPrefix(text, "/*") {
			if end := strings.Index(text, "*/"); end >= 0 {
				text = strings.TrimSpace(text[end+2:])
				continue
			}
		}
		break
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return false
	}
	keyword := strings.ToUpper(fields[0])
	switch keyword {
	case "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH":
		return true
	}
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
		if len(parts) == 1 {
			table = parts[0]
		} else if len(parts) == 2 {
			schema, table = parts[0], parts[1]
		}
		if !identifierPattern.MatchString(schema) || !identifierPattern.MatchString(table) {
			continue
		}
		pair := [2]string{schema, table}
		references[strings.ToLower(table)] = pair
		if alias != "" && !reserved[alias] {
			references[alias] = pair
		}
	}
	if reference, ok := references[strings.ToLower(qualifier)]; ok {
		return reference[0], reference[1]
	}
	if identifierPattern.MatchString(qualifier) {
		return defaultSchema, qualifier
	}
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
	if err != nil {
		return tableMeta{}, err
	}
	rows, err := db.Query(`SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return tableMeta{}, err
	}
	defer rows.Close()
	meta := tableMeta{Columns: make([]column, 0), PrimaryKeys: make([]string, 0)}
	for rows.Next() {
		var c column
		var nullable, key, extra string
		if err := rows.Scan(&c.Name, &c.Type, &nullable, &key, &extra); err != nil {
			return tableMeta{}, err
		}
		c.Nullable, c.Primary = nullable == "YES", key == "PRI"
		c.AutoIncrement = strings.Contains(strings.ToLower(extra), "auto_increment")
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
	Column   string `json:"column"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

func buildFilters(q url.Values, meta tableMeta) (string, []any, error) {
	conditions := make([]filterCondition, 0)
	if raw := strings.TrimSpace(q.Get("filters")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &conditions); err != nil {
			return "", nil, errors.New("多字段筛选条件格式无效")
		}
		if len(conditions) > 8 {
			return "", nil, errors.New("最多支持 8 个筛选条件")
		}
	} else if name := strings.TrimSpace(q.Get("filterColumn")); name != "" {
		conditions = append(conditions, filterCondition{Column: name, Operator: q.Get("filterOperator"), Value: q.Get("filterValue")})
	}
	if len(conditions) == 0 {
		return "", nil, nil
	}
	allowed := make(map[string]bool, len(meta.Columns))
	for _, col := range meta.Columns {
		allowed[col.Name] = true
	}
	parts, args := make([]string, 0, len(conditions)), make([]any, 0, len(conditions))
	for _, condition := range conditions {
		name, op, value := strings.TrimSpace(condition.Column), condition.Operator, condition.Value
		if name == "" {
			return "", nil, errors.New("筛选字段不能为空")
		}
		if op == "" {
			op = "contains"
		}
		if op != "is_null" && op != "not_null" && strings.TrimSpace(value) == "" {
			continue
		}
		if !allowed[name] {
			return "", nil, errors.New("筛选字段不存在: " + name)
		}
		field := quoteIdentifier(name)
		switch op {
		case "equals":
			parts, args = append(parts, field+"=?"), append(args, value)
		case "starts_with":
			parts, args = append(parts, field+" LIKE ?"), append(args, value+"%")
		case "gt":
			parts, args = append(parts, field+">?"), append(args, value)
		case "lt":
			parts, args = append(parts, field+"<?"), append(args, value)
		case "is_null":
			parts = append(parts, field+" IS NULL")
		case "not_null":
			parts = append(parts, field+" IS NOT NULL")
		case "contains":
			parts, args = append(parts, field+" LIKE ?"), append(args, "%"+value+"%")
		default:
			return "", nil, errors.New("不支持的筛选条件")
		}
	}
	if len(parts) == 0 {
		return "", nil, nil
	}
	logic := strings.ToUpper(strings.TrimSpace(q.Get("filterLogic")))
	if logic == "" {
		logic = "AND"
	}
	if logic != "AND" && logic != "OR" {
		return "", nil, errors.New("筛选条件关系无效")
	}
	return " WHERE " + strings.Join(parts, " "+logic+" "), args, nil
}
func appendWhere(where, condition string) string {
	if condition == "" {
		return where
	}
	if where == "" {
		return " WHERE " + condition
	}
	return where + " AND " + condition
}

func primaryCursorCondition(keys []string, values []any) (string, []any) {
	parts, args := make([]string, 0, len(keys)), make([]any, 0, len(keys)*(len(keys)+1)/2)
	for i, key := range keys {
		prefix := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			prefix, args = append(prefix, quoteIdentifier(keys[j])+"=?"), append(args, values[j])
		}
		prefix, args = append(prefix, quoteIdentifier(key)+">?"), append(args, values[i])
		parts = append(parts, "("+strings.Join(prefix, " AND ")+")")
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func encodeCursor(values []any) (string, error) {
	payload, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(token string, expected int) ([]any, error) {
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var values []any
	if err := decoder.Decode(&values); err != nil || len(values) != expected {
		return nil, errors.New("invalid cursor")
	}
	for i, value := range values {
		if value == nil {
			return nil, errors.New("invalid cursor")
		}
		if number, ok := value.(json.Number); ok {
			values[i] = number.String()
		}
	}
	return values, nil
}

func buildSort(q url.Values, meta tableMeta) (string, error) {
	columnNames := q["sortColumn"]
	directions := q["sortDirection"]
	if len(columnNames) == 0 || (len(columnNames) == 1 && strings.TrimSpace(columnNames[0]) == "") {
		return "", nil
	}
	if len(columnNames) != len(directions) {
		return "", errors.New("排序规则无效")
	}
	if len(columnNames) > 8 {
		return "", errors.New("排序规则不能超过 8 条")
	}
	available := make(map[string]bool, len(meta.Columns))
	for _, column := range meta.Columns {
		available[column.Name] = true
	}
	seen := make(map[string]bool, len(columnNames))
	parts := make([]string, 0, len(columnNames))
	for index, rawName := range columnNames {
		columnName := strings.TrimSpace(rawName)
		if !available[columnName] {
			return "", errors.New("排序字段不存在")
		}
		if seen[columnName] {
			return "", errors.New("排序字段不能重复")
		}
		seen[columnName] = true
		direction := strings.ToUpper(strings.TrimSpace(directions[index]))
		if direction != "ASC" && direction != "DESC" {
			return "", errors.New("排序方向无效")
		}
		parts = append(parts, quoteIdentifier(columnName)+" "+direction)
	}
	return " ORDER BY " + strings.Join(parts, ","), nil
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
	return sqlLiteralForColumn(value, false)
}

func sqlLiteralForColumn(value any, jsonColumn bool) string {
	if value == nil {
		return "NULL"
	}
	if bytes, ok := value.([]byte); ok {
		if jsonColumn {
			return quoteSQL(string(bytes))
		}
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
	log.Printf("request failed: status=%d message=%s", status, message)
	writeJSON(w, status, map[string]string{"error": message})
}
func writeDBError(w http.ResponseWriter, err error) {
	log.Printf("database error: %v", err)
	writeError(w, 500, "数据库操作失败: "+err.Error())
}
func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "不支持的请求方法")
}
