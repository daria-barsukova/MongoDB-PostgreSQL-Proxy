package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/lib/pq"
)

/*
	example: curl -X POST http://localhost:8080/query -H "Content-Type: application/json" -d '{"find": "testing", "filter": {"name": "Daria"}}'
	example: curl -X POST http://localhost:8080/query -H "Content-Type: application/json" -d '{"insert": "testing", "documents": [{"name": "John", "age": "79"}]}'
	example:  curl -X POST http://localhost:8080/query -H "Content-Type: application/json" -d '{"delete": "testing", "filter": {"name": "Helen"}}'
*/

type ProxyServer struct {
	db *sql.DB
}

func main() {
	connStr := "user=user1 password=passwd dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to PostgreSQL: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Db connection error: %v", err)
	}

	log.Println("Successful connection")

	proxy := &ProxyServer{db: db}
	http.HandleFunc("/query", proxy.handleQuery)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (ps *ProxyServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	var mongoQuery map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&mongoQuery); err != nil {
		log.Printf("Error decoding request body: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	switch {
	case mongoQuery["find"] != nil:
		ps.handleFind(w, mongoQuery)
	case mongoQuery["insert"] != nil:
		ps.handleInsert(w, mongoQuery)
	//case mongoQuery["update"] != nil:
	//	ps.handleUpdate(w, mongoQuery)
	case mongoQuery["delete"] != nil:
		ps.handleDelete(w, mongoQuery)
	default:
		http.Error(w, "Invalid request", http.StatusBadRequest)
	}
}

func (ps *ProxyServer) handleFind(w http.ResponseWriter, mongoQuery map[string]interface{}) {
	if find, ok := mongoQuery["find"].(string); ok {
		sqlQuery, err := ps.convertFindToSQL(find, mongoQuery["filter"])
		if err != nil {
			log.Printf("Error converting find query to SQL: %v", err)
			http.Error(w, "Request conversion error", http.StatusInternalServerError)
			return
		}

		log.Printf("Executing SQL query: %s", sqlQuery)
		rows, err := ps.db.Query(sqlQuery)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			http.Error(w, "Request execution error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		result, err := ps.rowsToJSON(rows)
		if err != nil {
			log.Printf("Error processing result: %v", err)
			http.Error(w, "Error processing result", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(result)
	} else {
		http.Error(w, "Invalid find request", http.StatusBadRequest)
	}
}

func (ps *ProxyServer) handleInsert(w http.ResponseWriter, mongoQuery map[string]interface{}) {
	if insert, ok := mongoQuery["insert"].(string); ok {
		sqlQuery, err := ps.convertInsertToSQL(insert, mongoQuery["documents"])
		if err != nil {
			log.Printf("Error converting insert query to SQL: %v", err)
			http.Error(w, "Request conversion error", http.StatusInternalServerError)
			return
		}

		log.Printf("Executing SQL query: %s", sqlQuery)
		_, err = ps.db.Exec(sqlQuery)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			http.Error(w, "Request execution error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "success"}`))
	} else {
		http.Error(w, "Invalid insert request", http.StatusBadRequest)
	}
}

/*
func (ps *ProxyServer) handleUpdate(w http.ResponseWriter, mongoQuery map[string]interface{}) {
	if update, ok := mongoQuery["update"].(string); ok {
		sqlQuery, err := ps.convertUpdateToSQL(update, mongoQuery["filter"], mongoQuery["update"])
		if err != nil {
			log.Printf("Error converting update query to SQL: %v", err)
			http.Error(w, "Request conversion error", http.StatusInternalServerError)
			return
		}

		log.Printf("Executing SQL query: %s", sqlQuery)
		_, err = ps.db.Exec(sqlQuery)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			http.Error(w, "Request execution error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "success"}`))
	} else {
		http.Error(w, "Invalid update request", http.StatusBadRequest)
	}
}
*/

func (ps *ProxyServer) handleDelete(w http.ResponseWriter, mongoQuery map[string]interface{}) {
	if del, ok := mongoQuery["delete"].(string); ok {
		sqlQuery, err := ps.convertDeleteToSQL(del, mongoQuery["filter"])
		if err != nil {
			log.Printf("Error converting delete query to SQL: %v", err)
			http.Error(w, "Request conversion error", http.StatusInternalServerError)
			return
		}

		log.Printf("Executing SQL query: %s", sqlQuery)
		_, err = ps.db.Exec(sqlQuery)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			http.Error(w, "Request execution error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status": "success"}`))
	} else {
		http.Error(w, "Invalid delete request", http.StatusBadRequest)
	}
}

func (ps *ProxyServer) convertFindToSQL(collection string, filter interface{}) (string, error) {
	var where []string
	if filterMap, ok := filter.(map[string]interface{}); ok {
		for key, value := range filterMap {
			where = append(where, fmt.Sprintf("%s = '%v'", key, value))
		}
	}
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", collection)
	if len(where) > 0 {
		sqlQuery += " WHERE " + strings.Join(where, " AND ")
	}
	return sqlQuery, nil
}

func (ps *ProxyServer) convertInsertToSQL(collection string, documents interface{}) (string, error) {
	if docs, ok := documents.([]interface{}); ok && len(docs) > 0 {
		var keys []string
		var values []string
		for key := range docs[0].(map[string]interface{}) {
			keys = append(keys, key)
		}
		for _, doc := range docs {
			var valueList []string
			for _, key := range keys {
				valueList = append(valueList, fmt.Sprintf("'%v'", doc.(map[string]interface{})[key]))
			}
			values = append(values, "("+strings.Join(valueList, ", ")+")")
		}
		sqlQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", collection, strings.Join(keys, ", "), strings.Join(values, ", "))
		return sqlQuery, nil
	}
	return "", fmt.Errorf("incorrect format")
}

/*
func (ps *ProxyServer) convertUpdateToSQL(collection string, filter interface{}, update interface{}) (string, error) {
	var setClauses []string
	if updateMap, ok := update.(map[string]interface{}); ok {
		for key, value := range updateMap {
			setClauses = append(setClauses, fmt.Sprintf("%s = '%v'", key, value))
		}
	}
	sqlQuery := fmt.Sprintf("UPDATE %s SET %s", collection, strings.Join(setClauses, ", "))
	if filterMap, ok := filter.(map[string]interface{}); ok {
		var whereClauses []string
		for key, value := range filterMap {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", key, value))
		}
		if len(whereClauses) > 0 {
			sqlQuery += " WHERE " + strings.Join(whereClauses, " AND ")
		}
	}
	return sqlQuery, nil
}
*/

func (ps *ProxyServer) convertDeleteToSQL(collection string, filter interface{}) (string, error) {
	sqlQuery := fmt.Sprintf("DELETE FROM %s", collection)
	if filterMap, ok := filter.(map[string]interface{}); ok {
		var whereClauses []string
		for key, value := range filterMap {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", key, value))
		}
		if len(whereClauses) > 0 {
			sqlQuery += " WHERE " + strings.Join(whereClauses, " AND ")
		}
	}
	return sqlQuery, nil
}

func (ps *ProxyServer) rowsToJSON(rows *sql.Rows) ([]byte, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		row := make(map[string]interface{})
		columnPointers := make([]interface{}, len(columns))
		for i := range columns {
			columnPointers[i] = new(interface{})
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		for i, colName := range columns {
			row[colName] = *(columnPointers[i].(*interface{}))
		}
		result = append(result, row)
	}
	return json.Marshal(result)
}
