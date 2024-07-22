package test_integration

import (
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"net/http"
	"testing"
)

func TestReadChanges_prefixed(t *testing.T) {
	defer testServer().Stop()
	t.Run("read all changes", func(t *testing.T) {
		primeTables(t)
		resp, err := http.Get(baseURL + "/datasets/sample/changes")
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext())
		ec, err := entityParser.LoadEntityCollection(resp.Body)
		if err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}
		if len(ec.GetEntities()) != 10 {
			t.Fatalf("Expected 10 entities, got %d", len(ec.GetEntities()))
		}
	})
}

func primeTables(t *testing.T) {
	conn := freshTables(t) // reuse table creation from "write" tests
	defer conn.Close()

	// populate "sample" table
	result, err := conn.Exec("INSERT ALL " +
		"    INTO sample (id, name) VALUES ('http://test/1', 'one')" +
		"	INTO sample (id, name) VALUES ('http://test/2', 'two')" +
		"	INTO sample (id, name) VALUES ('http://test/3', 'three')" +
		"	INTO sample (id, name) VALUES ('http://test/4', 'four')" +
		"	INTO sample (id, name) VALUES ('http://test/5', 'five')" +
		"	INTO sample (id, name) VALUES ('http://test/6', 'six')" +
		"	INTO sample (id, name) VALUES ('http://test/7', 'seven')" +
		"	INTO sample (id, name) VALUES ('http://test/8', 'eight')" +
		"	INTO sample (id, name) VALUES ('http://test/9', 'nine')" +
		"	INTO sample (id, name) VALUES ('http://test/10', 'ten')" +
		"SELECT 1 FROM dual")
	if err != nil {
		t.Fatalf("Failed to insert sample data: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get affected rows: %v", err)
	}
	if affected != 10 {
		t.Fatalf("Expected 10 rows to be affected, got %d", affected)
	}

	// populate "sample2" table
	result, err = conn.Exec("INSERT ALL " +
		"   INTO sample2 (id, name, recorded, deleted) VALUES (1, 'one', 164565566, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (2, 'two', 164565567, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (3, 'three', 164565568, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (4, 'four', 164565569, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (5, 'five', 164565570, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (6, 'six', 164565571, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (7, 'seven', 164565572, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (8, 'eight', 164565573, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (9, 'nine', 164565574, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (10, 'ten', 164565574, false)" +
		"   INTO sample2 (id, name, recorded, deleted) VALUES (1, 'one-off', 164565666, false)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (7, 'seven', 164565772, true)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (9, 'nine', 164565874, true)" +
		"	INTO sample2 (id, name, recorded, deleted) VALUES (9, 'n9ne', 164565974, false)" +
		"SELECT 1 FROM dual")
	if err != nil {
		t.Fatalf("Failed to insert sample2 data: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get affected rows: %v", err)
	}
	if affected != 14 {
		t.Fatalf("Expected 14 rows to be affected, got %d", affected)
	}
}
