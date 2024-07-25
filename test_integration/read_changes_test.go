package test_integration

import (
	"net/http"
	"testing"

	egdm "github.com/mimiro-io/entity-graph-data-model"
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
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(resp.Body)
		if err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}
		if len(ec.GetEntities()) != 10 {
			t.Fatalf("Expected 10 entities, got %d", len(ec.GetEntities()))
		}
		// the next assertions are based on natural ordering in the database, so a bit fragile. but should be stable enough
		if ec.GetEntities()[0].ID != "http://test/1" {
			t.Fatalf("Expected first entity to have ID 'http://test/1', got %s", ec.GetEntities()[0].ID)
		}
		if len(ec.GetEntities()[0].Properties) != 1 {
			t.Fatalf("Expected first entity to have 1 property, got %d", len(ec.GetEntities()[0].Properties))
		}
		if ec.GetEntities()[0].Properties["http://test/prop1"] != "one" {
			t.Fatalf("Expected first entity to have property 'http://test/prop1' with value 'one', got %s", ec.GetEntities()[0].Properties["name"])
		}
		if ec.GetEntities()[9].ID != "http://test/10" {
			t.Fatalf("Expected last entity to have ID 'http://test/10', got %s", ec.GetEntities()[9].ID)
		}
	})
}

func TestReadChanges_simpleValues(t *testing.T) {
	defer testServer().Stop()
	t.Run("read all changes", func(t *testing.T) {
		primeTables(t)
		resp, err := http.Get(baseURL + "/datasets/sample2/changes")
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
		}
		entityParser := egdm.NewEntityParser(egdm.NewNamespaceContext()).WithExpandURIs()
		ec, err := entityParser.LoadEntityCollection(resp.Body)
		if err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}
		if len(ec.GetEntities()) != 14 {
			t.Fatalf("Expected 14 entities, got %d", len(ec.GetEntities()))
		}
		// the next assertions are based on natural ordering in the database, so a bit fragile. but should be stable enough
		if ec.GetEntities()[0].ID != "http://data.sample.org/things/1" {
			t.Fatalf("Expected first entity to have ID 'http://data.sample.org/things/1', got %s", ec.GetEntities()[0].ID)
		}
		if len(ec.GetEntities()[0].Properties) != 7 {
			t.Fatalf("Expected first entity to have 1 property, got %d", len(ec.GetEntities()[0].Properties))
		}
		if ec.GetEntities()[0].Properties["http://data.sample.org/name"] != "one" {
			t.Fatalf("Expected first entity to have property 'name' with value 'one', got %s", ec.GetEntities()[0].Properties["http://data.sample.org/name"])
		}
		if ec.GetEntities()[0].Properties["http://data.sample.org/AGE"] != 40.0 {
			t.Fatalf("Expected first entity to have property 'age' with value '40', got %s", ec.GetEntities()[0].Properties["http://data.sample.org/AGE"])
		}
		if ec.GetEntities()[0].Properties["http://data.sample.org/WEIGHT"] != 67.554 {
			t.Fatalf("Expected first entity to have property 'weight' with value '67.554', got %s", ec.GetEntities()[0].Properties["http://data.sample.org/WEIGHT"])
		}
		if ec.GetEntities()[0].Recorded != 164565566 {
			t.Fatalf("Expected first entity to have recorded timestamp 164565566, got %d", ec.GetEntities()[0].Recorded)
		}
		if ec.GetEntities()[9].ID != "http://data.sample.org/things/10" {
			t.Fatalf("Expected entity to have ID 'http://data.sample.org/things/10', got %s", ec.GetEntities()[9].ID)
		}
		if ec.GetEntities()[12].ID != "http://data.sample.org/things/9" {
			t.Fatalf("Expected entity to have ID 'http://data.sample.org/things/9', got %s", ec.GetEntities()[9].ID)
		}
		if ec.GetEntities()[12].IsDeleted != true {
			t.Fatalf("Expected entity to have be deleted, got %v", ec.GetEntities()[9].IsDeleted)
		}
		if ec.GetEntities()[13].Properties["http://data.sample.org/name"] != "n9ne" {
			t.Fatalf("Expected last entity to have property 'name' with value 'n9ne', got %s", ec.GetEntities()[9].Properties["name"])
		}
		if ec.GetEntities()[13].IsDeleted != false {
			t.Fatalf("Expected last entity to not be deleted, got %v", ec.GetEntities()[9].IsDeleted)
		}
		if ec.GetEntities()[13].Recorded != 164565974 {
			t.Fatalf("Expected last entity to have recorded timestamp 164565974, got %d", ec.GetEntities()[9].Recorded)
		}
	})
}

// TODOS: - Add test for "read all changes with limit"
//        - Add test for "read all changes with since"
//        - Add test for "read all changes with since and limit, paging"
//        - Add test for "read all changes with latest only"
//        - Add test for "read all changes with since but no since_column"

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
		"   INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (1, 'one', 164565566, false, 40, 67.554)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (2, 'two', 164565567, false, 37, 75.0)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (3, 'three', 164565568, false, 31, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (4, 'four', 164565569, false, null, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (5, 'five', 164565570, false, 100, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (6, 'six', 164565571, false, null, 89.5)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (7, 'seven', 164565572, false, 14, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (8, 'eight', 164565573, false, null, 107.601)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (9, 'nine', 164565574, false, null, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (10, 'ten', 164565574, false, null, null)" +
		"   INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (1, 'one-off', 164565666, false, null, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (7, 'seven', 164565772, true, 24, null)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (9, 'nine', 164565874, true, null, 80.2)" +
		"	INTO sample2 (id, name, recorded, deleted, age, weight) VALUES (9, 'n9ne', 164565974, false, 46, 57.65)" +
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
