package test_integration

import (
	"database/sql"
	"io"
	"net/http"
	"os"
	"testing"

	egdm "github.com/mimiro-io/entity-graph-data-model"
	go_ora "github.com/sijms/go-ora/v2"
)

/**
 * @api {test} POST /datasets/{name}/entities
 *   Test posting batches to the /entities endpoint
 *   In this test we use the "sample" dataset, which is configured in latest only mode.
 *   We add 3 entities to the table, then update one of them, and finally delete one of them. in latest only mode,
 *   existing rows should be mutated.
 */
func TestPostEntitiesLatestOnly(t *testing.T) {
	defer testServer().Stop()

	t.Run("add entities to table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "props": map[string]any{"http://test/prop1": "value1"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()

		resp, err := http.Post(baseURL+"/datasets/sample/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		rows, err := conn.Query("SELECT id,name FROM sample")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, name string
		cnt := 0
		for rows.Next() {
			err := rows.Scan(&id, &name)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			cnt++
		}
		if cnt != 3 {
			t.Fatalf("Expected 3 rows, got %d", cnt)
		}
	})

	t.Run("update entities in table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "props": map[string]any{"http://test/prop1": "value1"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() { ec.WriteEntityGraphJSON(entityWriter); entityWriter.Close() }()

		resp, err := http.Post(baseURL+"/datasets/sample/entities", "application/json", entityReader)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Failed to send request: %v", err)
		}

		ec = egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "props": map[string]any{"http://test/prop1": "value1-changed"}})
		entityReader, entityWriter = io.Pipe()
		go func() { ec.WriteEntityGraphJSON(entityWriter); entityWriter.Close() }()

		resp, err = http.Post(baseURL+"/datasets/sample/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		rows, err := conn.Query("SELECT id,name FROM sample WHERE id = 'http://test/1'")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, name string
		hasNext := rows.Next()
		if !hasNext {
			t.Fatalf("Expected row, got none")
		}
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if name != "value1-changed" {
			t.Fatalf("Expected value1-changed, got %s", name)
		}
		hasNext = rows.Next()
		if hasNext {
			t.Fatalf("Expected no more rows, got one")
		}

		rows2, err := conn.Query("SELECT id,name FROM sample")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows2.Close()
		cnt := 0
		for rows2.Next() {
			err := rows2.Scan(&id, &name)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			cnt++
		}
		if cnt != 3 {
			t.Fatalf("Expected 3 rows, got %d", cnt)
		}
	})

	t.Run("delete entities from table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "props": map[string]any{"http://test/prop1": "value1"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()

		resp, err := http.Post(baseURL+"/datasets/sample/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		ec = egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{
			"id":      "http://test/1",
			"props":   map[string]any{"http://test/prop1": "value1-changed"},
			"deleted": true,
		})
		entityReader, entityWriter = io.Pipe()
		go func() { ec.WriteEntityGraphJSON(entityWriter); entityWriter.Close() }()

		resp, err = http.Post(baseURL+"/datasets/sample/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		rows, err := conn.Query("SELECT id,name FROM sample WHERE id = 'http://test/1'")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		hasNext := rows.Next()
		if hasNext {
			t.Fatalf("Expected row http://test/1 to be deleted")
		}

		rows, err = conn.Query("SELECT id,name FROM sample")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, name string
		cnt := 0
		for rows.Next() {
			err := rows.Scan(&id, &name)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			cnt++
		}
		if cnt != 2 {
			t.Fatalf("Expected 2 remaining rows, got %d", cnt)
		}
	})
}

/* @api {test} POST /datasets/{name}/entities
*   Test posting batches to the /entities endpoint
*   In this test we use the "sample2" dataset, which is configured with the "append_mode=true" option in
*	 integration-test-config.json.
*   We add 3 entities to the table, then update one of them, and finally delete one of them. in append mode,
*   existing rows should NOT be mutated, instead new rows with id duplicates are appended. It is up to the user
*   to manage the duplicates using recorded timestamp and deleted flag.
 */
func TestPostEntitiesAppendMode(t *testing.T) {
	defer testServer().Stop()
	t.Run("add entities to table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "recorded": uint64(1245454545), "props": map[string]any{"http://test/prop1": "value1", "http://test/prop2": 44, "http://test/prop3": 57.5}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()

		resp, err := http.Post(baseURL+"/datasets/sample2/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		rows, err := conn.Query("SELECT * FROM sample2")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, name, recorded, deleted, age, weight any
		cnt := 0
		id1Seen := false
		for rows.Next() {
			err := rows.Scan(&id, &name, &recorded, &deleted, &age, &weight)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			if id == "1" {
				if name != "value1" {
					t.Fatalf("Expected value1, got %s", name)
				}
				if deleted != "0" {
					t.Fatalf("Expected deleted flag to be 0, got %v", deleted)
				}
				if recorded != "1245454545" {
					t.Fatalf("Expected recorded timestamp, got nil")
				}
				if age != "44" {
					t.Fatalf("Expected age 44, got %v", age)
				}
				if weight != 57.5 {
					t.Fatalf("Expected weight 57.5, got %v", weight)
				}
				id1Seen = true
			}
			cnt++
		}
		if !id1Seen {
			t.Fatalf("Expected row with id 1")
		}
		if cnt != 3 {
			t.Fatalf("Expected 3 rows, got %d", cnt)
		}
	})

	t.Run("update entities in table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "recorded": uint64(1245454545), "props": map[string]any{"http://test/prop1": "value1"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()

		resp, err := http.Post(baseURL+"/datasets/sample2/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		ec = egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "recorded": uint64(1345454545), "props": map[string]any{"http://test/prop1": "value1-changed"}})
		entityReader, entityWriter = io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()
		resp, err = http.Post(baseURL+"/datasets/sample2/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}
		rows, err := conn.Query("SELECT id,name FROM sample2 WHERE id = '1'")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, name string
		var valueChangedSeen, valueSeen bool
		hasNext := rows.Next()
		if !hasNext {
			t.Fatalf("Expected row, got none")
		}
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		valueChangedSeen = name == "value1-changed"
		valueSeen = name == "value1"
		hasNext = rows.Next()
		if !hasNext {
			t.Fatalf("Expected more rows")
		}
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		valueChangedSeen = valueChangedSeen || name == "value1-changed"
		valueSeen = valueSeen || name == "value1"
		hasNext = rows.Next()
		if hasNext {
			t.Fatalf("Expected no more rows, got one")
		}

		if !valueChangedSeen {
			t.Fatalf("could not observe value1-changed in db after it was posted")
		}
		if !valueSeen {
			t.Fatalf("could not observe value1 in db after it was updated")
		}

		rows2, err := conn.Query("SELECT id,name FROM sample2")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows2.Close()
		cnt := 0
		for rows2.Next() {
			err := rows2.Scan(&id, &name)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			cnt++
		}
		if cnt != 4 {
			t.Fatalf("Expected 4 rows, got %d", cnt)
		}
	})

	t.Run("delete entities from table", func(t *testing.T) {
		conn := freshTables(t)
		defer conn.Close()

		ec := egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{"id": "http://test/1", "recorded": uint64(1245454545), "props": map[string]any{"http://test/prop1": "value1"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/2", "props": map[string]any{"http://test/prop1": "value2"}})
		ec.AddEntityFromMap(map[string]any{"id": "http://test/3", "props": map[string]any{"http://test/prop1": "value3"}})
		entityReader, entityWriter := io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()

		resp, err := http.Post(baseURL+"/datasets/sample2/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}

		ec = egdm.NewEntityCollection(egdm.NewNamespaceContext())
		ec.AddEntityFromMap(map[string]any{
			"id":       "http://test/1",
			"recorded": uint64(1345454545),
			"deleted":  true,
			"props":    map[string]any{"http://test/prop1": "value1"},
		})
		entityReader, entityWriter = io.Pipe()
		go func() {
			ec.WriteEntityGraphJSON(entityWriter)
			entityWriter.Close()
		}()
		resp, err = http.Post(baseURL+"/datasets/sample2/entities", "application/json", entityReader)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}
		rows, err := conn.Query("SELECT id,deleted FROM sample2 WHERE id = '1'")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows.Close()
		var id, deleted any
		var undeletedSeen, deletedSeen bool
		hasNext := rows.Next()
		if !hasNext {
			t.Fatalf("Expected row, got none")
		}
		err = rows.Scan(&id, &deleted)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		deletedSeen = deleted == "1"
		undeletedSeen = deleted == "0"
		hasNext = rows.Next()
		if !hasNext {
			t.Fatalf("Expected more rows")
		}
		err = rows.Scan(&id, &deleted)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		deletedSeen = deletedSeen || deleted == "1"
		undeletedSeen = undeletedSeen || deleted == "0"
		hasNext = rows.Next()
		if hasNext {
			t.Fatalf("Expected no more rows, got one")
		}

		if !undeletedSeen {
			t.Fatalf("could not observe undeleted variant in db after it was posted")
		}
		if !deletedSeen {
			t.Fatalf("could not observe deleted variant in db after it was updated")
		}

		rows2, err := conn.Query("SELECT id FROM sample2")
		if err != nil {
			t.Fatalf("Failed to query table: %v", err)
		}
		defer rows2.Close()
		cnt := 0
		for rows2.Next() {
			err := rows2.Scan(&id)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			cnt++
		}
		if cnt != 4 {
			t.Fatalf("Expected 4 rows, got %d", cnt)
		}
	})
}

func freshTables(t *testing.T) *sql.DB {
	url := os.Getenv("ORACLE_URL")
	c := sql.OpenDB(go_ora.NewConnector(url))
	c.Exec("DROP TABLE sample")  // ignore errors, table may not exist
	c.Exec("DROP TABLE sample2") // ignore errors, table may not exist
	c.Exec("DROP TABLE sample3") // ignore errors, table may not exist
	_, err := c.Exec("CREATE TABLE sample (id VARCHAR2(100), name VARCHAR2(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = c.Exec("CREATE TABLE sample2 (" +
		"id VARCHAR2(100), " +
		"name VARCHAR2(100), " +
		"recorded NUMBER(16), " +
		"deleted BOOL, " +
		"age NUMBER(5,0), " +
		"weight BINARY_FLOAT" +
		")")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = c.Exec("CREATE TABLE sample3 (id VARCHAR2(100), name VARCHAR2(100))")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	return c
}
