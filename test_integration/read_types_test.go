package test_integration

import (
	"database/sql"
	"net/http"
	"os"
	"testing"
	"time"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	layer "github.com/mimiro-io/oracle-datalayer/internal"
	go_ora "github.com/sijms/go-ora/v2"
)

func TestReadAllTypes(t *testing.T) {
	createTypeTestTable(t)

	server := testServer()
	// set layer up with a table that contains a column for each supported oracle data type
	server.LayerService().UpdateConfiguration(&common.Config{
		DatasetDefinitions: []*common.DatasetDefinition{
			{
				SourceConfig: map[string]any{layer.TableName: "all_types"},
				OutgoingMappingConfig: &common.OutgoingMappingConfig{
					BaseURI: "http://any.type/",
					MapAll:  true,
				},
				DatasetName: "all_types_auto_mapped",
			},
			{
				SourceConfig: map[string]any{layer.TableName: "all_types"},
				OutgoingMappingConfig: &common.OutgoingMappingConfig{
					BaseURI: "http://any.type/",
					PropertyMappings: []*common.ItemToEntityPropertyMapping{{
						Property:        "id",
						IsIdentity:      true,
						URIValuePattern: "{value}",
					}, {
						EntityProperty: "ent_string",
						Property:       "col_varchar2",
					}, {
						EntityProperty: "rowid_string",
						Property:       "rowid",
					}, {
						EntityProperty: "col_number1",
						Property:       "col_number1",
						Datatype:       "bool",
					}, {
						EntityProperty: "col_bool",
						Property:       "col_bool",
						Datatype:       "bool",
					}},
				},
				DatasetName: "all_types_manual_mapped",
			},
		},
	})
	defer server.Stop()

	t.Run("read all oracle data types auto mapped", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/datasets/all_types_auto_mapped/changes")
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
		if len(ec.GetEntities()) != 2 {
			t.Fatalf("Expected 10 entities, got %d", len(ec.GetEntities()))
		}
		if len(ec.GetEntities()[0].Properties) != 20 {
			t.Fatalf("Expected 20 properties, got %d", len(ec.GetEntities()[0].Properties))
		}
		eq(t, ec.GetEntities()[0], "ID", "http://test/1")
		eq(t, ec.GetEntities()[0], "COL_VARCHAR2", "one")
		eq(t, ec.GetEntities()[0], "COL_NVARCHAR2", "two")
		eq(t, ec.GetEntities()[0], "COL_CHAR", "three")
		eq(t, ec.GetEntities()[0], "COL_NCHAR", "four ") // fixed length 5 char. TODO: who should trim?
		eq(t, ec.GetEntities()[0], "COL_NUMBER32", 32.0)
		eq(t, ec.GetEntities()[0], "COL_NUMBER5P2", 5.2)
		eq(t, ec.GetEntities()[0], "COL_NUMBER1", 1.0) // without data type hint in mapping, this would be a float
		eq(t, ec.GetEntities()[0], "COL_BOOL", 1.0)    // without data type hint in mapping, this would be a float
		eq(t, ec.GetEntities()[0], "COL_FLOAT64", 64.0)
		eq(t, ec.GetEntities()[0], "COL_FLOAT32", 32.0)
		eq(t, ec.GetEntities()[0], "COL_BINARY_FLOAT", 1.1)
		eq(t, ec.GetEntities()[0], "COL_BINARY_DOUBLE", 2.2)
		eq(t, ec.GetEntities()[0], "COL_DATE", "2021-01-01T00:00:00Z")
		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP", "2021-01-01T12:00:00Z")
		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP_TZ", "2021-01-01T12:00:00+01:00")

		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP_LTZ", time.Date(2021, 1, 1, 12, 0, 0, 0, time.FixedZone("CET", 3600)).In(time.UTC).Format(time.RFC3339))
		eq(t, ec.GetEntities()[0], "COL_INTERVAL_DS", "+01 12:00:00.000000")
		eq(t, ec.GetEntities()[0], "COL_INTERVAL_YM", "+01-02")
		eq(t, ec.GetEntities()[0], "COL_RAW", "AAABBBCCCDDD")

		if len(ec.GetEntities()[1].Properties) != 1 {
			t.Fatalf("Expected 1 property (id) only from row with all nulls, got %d", len(ec.GetEntities()[1].Properties))
		}
	})
	t.Run("read all oracle data types manual mapped", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/datasets/all_types_manual_mapped/changes")
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
		if len(ec.GetEntities()) != 2 {
			t.Fatalf("Expected 2 entities, got %d", len(ec.GetEntities()))
		}
		if ec.GetEntities()[0].ID != "http://test/1" {
			t.Fatalf("Expected first entity to have ID 'http://test/1', got %s", ec.GetEntities()[0].ID)
		}
		if ec.GetEntities()[1].ID != "http://test/2" {
			t.Fatalf("Expected 2nd entity to have ID 'http://test/2', got %s", ec.GetEntities()[1].ID)
		}
		if len(ec.GetEntities()[0].Properties) != 4 {
			t.Fatalf("Expected 4 properties, got %d", len(ec.GetEntities()[0].Properties))
		}
		eq(t, ec.GetEntities()[0], "ent_string", "one")
		eq(t, ec.GetEntities()[0], "col_number1", true)
		eq(t, ec.GetEntities()[0], "col_bool", true)
		rid := ec.GetEntities()[0].Properties["http://any.type/rowid_string"]
		if _, ok := rid.(string); !ok {
			t.Fatalf("Expected rowid to be a string, got %+v", rid)
		}
		if len(rid.(string)) < 6 {
			t.Fatalf("Expected rowid string to be at least 6 chars long, got %v", rid)
		}
	})
}

func eq(t *testing.T, e *egdm.Entity, key string, exp any) {
	k := "http://any.type/" + key
	val := e.Properties[k]
	if val != exp {
		t.Fatalf("Expected %s to be %s, got %s", k, exp, val)
	}
}

func createTypeTestTable(t *testing.T) {
	url := os.Getenv("ORACLE_URL")
	conn := sql.OpenDB(go_ora.NewConnector(url))
	defer conn.Close()

	conn.Exec("DROP TABLE all_types") // ignore errors, table may not exist
	_, err := conn.Exec("CREATE TABLE all_types (" +
		"id VARCHAR2(100), " +
		"col_varchar2 VARCHAR2(5), " +
		"col_nvarchar2 NVARCHAR2(5), " +
		"col_char CHAR(5), " +
		"col_nchar NCHAR(5), " +
		"col_number32 number(32), " +
		"col_number5p2 number(5,2), " +
		"col_number1 number(1), " +
		"col_bool boolean, " +
		"col_float64 FLOAT(64), " +
		"col_float32 FLOAT(32), " +
		"col_binary_float BINARY_FLOAT, " +
		"col_binary_double BINARY_DOUBLE, " +
		//"col_long LONG, " +
		"col_date DATE, " +
		"col_timestamp TIMESTAMP, " +
		"col_timestamp_tz TIMESTAMP WITH TIME ZONE, " +
		"col_timestamp_ltz TIMESTAMP WITH LOCAL TIME ZONE, " +
		"col_interval_ds INTERVAL DAY TO SECOND, " +
		"col_interval_ym INTERVAL YEAR TO MONTH, " +
		"col_raw RAW(128)" +
		")")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	stmt := "INSERT INTO all_types VALUES (" +
		"'http://test/1', " +
		"'one', " +
		"'two', " +
		"'three', " +
		"'four', " +
		"32, " +
		"5.2, " +
		"1, " +
		"true, " +
		"64, " +
		"32, " +
		"1.1, " +
		"2.2, " +
		"TO_DATE('2021-01-01', 'YYYY-MM-DD'), " +
		"TO_TIMESTAMP('2021-01-01 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), " +
		"TO_TIMESTAMP_TZ('2021-01-01 12:00:00 +01:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), " +
		"TO_TIMESTAMP_TZ('2021-01-01 12:00:00 +01:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), " +
		"INTERVAL '1 12:00:00' DAY TO SECOND, " +
		"INTERVAL '1-2' YEAR TO MONTH, " +
		"RAWTOHEX('AAABBBCCCDDD')" +
		")"

	_, err = conn.Exec(stmt)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// adding a 2nd row with all null values, to make sure the layer can handle that
	_, err = conn.Exec("INSERT INTO all_types VALUES (" +
		"'http://test/2', " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null, " +
		"null" +
		")")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
}
