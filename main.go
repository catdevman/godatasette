// main.go
package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed templates
var templateFS embed.FS

// App holds application-wide dependencies, like the database connection.
type App struct {
	db        *sql.DB
	templates *template.Template
	dbPath    string
}

// Table represents a single database table.
type Table struct {
	Name       string
	RowCount   int64
	ViewURL    string
	APIDataURL string
}

// PageData is the structure passed to HTML templates.
type PageData struct {
	DBName       string
	Tables       []Table
	CurrentTable string
	Columns      []string
	Rows         [][]interface{}
	Query        string
	Error        string
	CurrentPage  int
	NextPage     int
	PrevPage     int
	HasNextPage  bool
	TotalPages   int
}

const rowsPerPage = 50

func main() {
	// --- Command-Line Flags ---
	dbPath := flag.String("db", "", "Path to the SQLite database file (required)")
	port := flag.Int("port", 8080, "Port to run the web server on")
	flag.Parse()

	if *dbPath == "" {
		log.Println("Error: -db flag is required.")
		flag.Usage()
		os.Exit(1)
	}

	// --- Application Setup ---
	app, err := NewApp(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}
	defer app.db.Close()

	// --- HTTP Server Setup ---
	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleIndex)
	mux.HandleFunc("/table/", app.handleTable)
	mux.HandleFunc("/query", app.handleQuery)

	// API endpoints
	mux.HandleFunc("/api/tables", app.handleAPITables)
	mux.HandleFunc("/api/table/", app.handleAPITableData)
	mux.HandleFunc("/api/query", app.handleAPIQuery)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("Starting GoDB-Explorer for '%s'", filepath.Base(*dbPath))
	log.Printf("Server listening on http://localhost:%d", *port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// NewApp creates and initializes a new App instance.
func NewApp(dbPath string) (*App, error) {
	// Check if the database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file not found at path: %s", dbPath)
	}

	// Connect to the SQLite database
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Parse HTML templates from the embedded filesystem
	templates, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse templates: %w", err)
	}

	return &App{
		db:        db,
		templates: templates,
		dbPath:    dbPath,
	}, nil
}

// --- HTTP Handlers (HTML) ---

// handleIndex displays the homepage with a list of tables.
func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	tables, err := a.getTables()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list tables: %v", err), http.StatusInternalServerError)
		return
	}

	data := PageData{
		DBName: filepath.Base(a.dbPath),
		Tables: tables,
	}
	a.renderTemplate(w, "index.html", data)
}

// handleTable displays data for a specific table with pagination.
func (a *App) handleTable(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, "/table/")
	if tableName == "" {
		http.Error(w, "Table name not specified", http.StatusBadRequest)
		return
	}

	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	columns, rows, totalRows, err := a.getTableData(tableName, page)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch table data: %v", err), http.StatusInternalServerError)
		return
	}

	totalPages := int(totalRows-1)/rowsPerPage + 1
	if totalRows == 0 {
		totalPages = 0
	}

	data := PageData{
		DBName:       filepath.Base(a.dbPath),
		CurrentTable: tableName,
		Columns:      columns,
		Rows:         rows,
		CurrentPage:  page,
		NextPage:     page + 1,
		PrevPage:     page - 1,
		HasNextPage:  page < totalPages,
		TotalPages:   totalPages,
	}

	a.renderTemplate(w, "table.html", data)
}

// handleQuery displays a form for custom SQL and shows results.
func (a *App) handleQuery(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("sql")
	data := PageData{
		DBName: filepath.Base(a.dbPath),
		Query:  query,
	}

	if r.Method == http.MethodPost && query != "" {
		// Basic security: only allow SELECT statements.
		if !strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "SELECT") {
			data.Error = "Only SELECT queries are allowed."
		} else {
			columns, rows, err := a.executeCustomQuery(query)
			if err != nil {
				data.Error = err.Error()
			} else {
				data.Columns = columns
				data.Rows = rows
			}
		}
	}

	a.renderTemplate(w, "query.html", data)
}

// --- HTTP Handlers (JSON API) ---

func (a *App) handleAPITables(w http.ResponseWriter, r *http.Request) {
	tables, err := a.getTables()
	if err != nil {
		a.respondWithError(w, http.StatusInternalServerError, "Failed to get tables")
		return
	}
	a.respondWithJSON(w, http.StatusOK, tables)
}

func (a *App) handleAPITableData(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, "/api/table/")
	page := 1
	if p, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && p > 0 {
		page = p
	}

	columns, rows, totalRows, err := a.getTableData(tableName, page)
	if err != nil {
		a.respondWithError(w, http.StatusInternalServerError, "Failed to get table data")
		return
	}

	response := map[string]interface{}{
		"tableName":   tableName,
		"page":        page,
		"rowsPerPage": rowsPerPage,
		"totalRows":   totalRows,
		"columns":     columns,
		"rows":        rows,
	}
	a.respondWithJSON(w, http.StatusOK, response)
}

func (a *App) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("sql")
	if query == "" {
		a.respondWithError(w, http.StatusBadRequest, "Missing 'sql' query parameter")
		return
	}

	if !strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "SELECT") {
		a.respondWithError(w, http.StatusForbidden, "Only SELECT queries are allowed.")
		return
	}

	columns, rows, err := a.executeCustomQuery(query)
	if err != nil {
		a.respondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Query execution failed: %v", err))
		return
	}

	response := map[string]interface{}{
		"query":   query,
		"columns": columns,
		"rows":    rows,
	}
	a.respondWithJSON(w, http.StatusOK, response)
}

// --- Database Logic ---

// getTables retrieves all user-defined tables from the database.
func (a *App) getTables() ([]Table, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		// Get row count for each table
		var count int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q", name)
		err := a.db.QueryRow(countQuery).Scan(&count)
		if err != nil {
			log.Printf("Could not count rows for table %s: %v", name, err)
			count = -1 // Indicate an error
		}

		tables = append(tables, Table{
			Name:       name,
			RowCount:   count,
			ViewURL:    fmt.Sprintf("/table/%s", name),
			APIDataURL: fmt.Sprintf("/api/table/%s", name),
		})
	}
	return tables, nil
}

// getTableData retrieves paginated data for a given table.
func (a *App) getTableData(tableName string, page int) (columns []string, rows [][]interface{}, totalRows int64, err error) {
	// First, get the total number of rows for pagination
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %q", tableName)
	err = a.db.QueryRow(countQuery).Scan(&totalRows)
	if err != nil {
		return
	}

	// Then, fetch the paginated data
	offset := (page - 1) * rowsPerPage
	query := fmt.Sprintf("SELECT * FROM %q LIMIT %d OFFSET %d", tableName, rowsPerPage, offset)

	columns, rows, err = a.executeCustomQuery(query)
	return
}

// executeCustomQuery runs a given SQL query and returns the results.
func (a *App) executeCustomQuery(query string) ([]string, [][]interface{}, error) {
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results [][]interface{}
	for rows.Next() {
		// Create a slice of empty interfaces to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		// Convert byte slices (BLOBs) and other types to printable strings
		for i, val := range values {
			switch v := val.(type) {
			case []byte:
				values[i] = string(v)
			case time.Time:
				values[i] = v.Format(time.RFC3339)
			case nil:
				values[i] = "NULL"
			}
		}

		results = append(results, values)
	}

	return columns, results, nil
}

// --- Helper Functions ---

func (a *App) renderTemplate(w http.ResponseWriter, tmplName string, data PageData) {
	err := a.templates.ExecuteTemplate(w, tmplName, data)
	if err != nil {
		log.Printf("Error executing template %s: %v", tmplName, err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (a *App) respondWithError(w http.ResponseWriter, code int, message string) {
	a.respondWithJSON(w, code, map[string]string{"error": message})
}

func (a *App) respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "Failed to marshal JSON response"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
