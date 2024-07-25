package test_integration

import (
	"database/sql"
	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/oracle-datalayer/internal"
	go_ora "github.com/sijms/go-ora/v2"
	"net/http"
	"os"
	"strings"
	"testing"

	egdm "github.com/mimiro-io/entity-graph-data-model"
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
						EntityProperty: "ent_string",
						Property:       "col_string",
					}},
				},
				DatasetName: "all_types_manual_mapped",
			},
		},
	})
	defer server.Stop()

	t.Run("read all oracle data types auto mapped", func(t *testing.T) {
		primeTables(t)
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
		eq(t, ec.GetEntities()[0], "COL_NUMBER1", true)
		eq(t, ec.GetEntities()[0], "COL_FLOAT64", 64.0)
		eq(t, ec.GetEntities()[0], "COL_FLOAT32", 32.0)
		eq(t, ec.GetEntities()[0], "COL_BINARY_FLOAT", 1.1)
		eq(t, ec.GetEntities()[0], "COL_BINARY_DOUBLE", 2.2)
		eq(t, ec.GetEntities()[0], "COL_DATE", "2021-01-01T00:00:00Z")
		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP", "2021-01-01T12:00:00Z")
		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP_TZ", "2021-01-01T12:00:00+01:00")
		eq(t, ec.GetEntities()[0], "COL_TIMESTAMP_LTZ", "2021-01-01T10:00:00Z")
		eq(t, ec.GetEntities()[0], "COL_INTERVAL_DS", "+01 12:00:00.000000")
		eq(t, ec.GetEntities()[0], "COL_INTERVAL_YM", "+01-02")
		eq(t, ec.GetEntities()[0], "COL_RAW", "AAABBBCCCDDD")

		if len(ec.GetEntities()[1].Properties) != 3 {
			t.Fatalf("Expected 3 properties (2xbool+id), got %d", len(ec.GetEntities()[1].Properties))
		}
	})
}

func eq(t *testing.T, e *egdm.Entity, key string, exp any) {
	k := "http://any.type/" + strings.ToUpper(key)
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
		"TO_TIMESTAMP('2021-01-01 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), " +
		"INTERVAL '1 12:00:00' DAY TO SECOND, " +
		"INTERVAL '1-2' YEAR TO MONTH, " +
		"RAWTOHEX('AAABBBCCCDDD')" +
		")"

	_, err = conn.Exec(stmt)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
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
