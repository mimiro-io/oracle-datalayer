package test_integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	go_ora "github.com/sijms/go-ora/v2"
)

// sampleBody returns a JSON array with one entity per id for the sample dataset,
// each with name in prop1. The array is left open, so callers close it with "]"
// or append invalid JSON.
func sampleBody(name string, ids ...string) string {
	body := `[{"id":"@context","namespaces":{}}`
	for _, id := range ids {
		body += fmt.Sprintf(`,{"id":"http://test/%s","props":{"http://test/prop1":%q,"http://test/prop2":1}}`, id, name)
	}
	return body
}

// postSample posts body to the sample dataset and returns the status code.
func postSample(t *testing.T, client *http.Client, body string) int {
	t.Helper()
	resp, err := client.Post(baseURL+"/datasets/sample/entities", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// TestFailedWritesReleaseSessions checks that failed requests don't leave
// database sessions open.
func TestFailedWritesReleaseSessions(t *testing.T) {
	defer testServer().Stop()
	conn := freshTables(t)
	defer conn.Close()

	// testuser can't read v$session, so count sessions as system
	port, _ := strconv.Atoi(os.Getenv("ORACLE_PORT"))
	sys := sql.OpenDB(go_ora.NewConnector(go_ora.BuildUrl("localhost", port, "FREEPDB1", "system", "systempassword", nil)))
	defer sys.Close()
	sessions := func() int {
		var n int
		if err := sys.QueryRow("SELECT COUNT(*) FROM v$session WHERE username = 'TESTUSER'").Scan(&n); err != nil {
			t.Fatalf("Failed to count sessions: %v", err)
		}
		return n
	}

	if status := postSample(t, http.DefaultClient, sampleBody("ok", "1")+"]"); status != http.StatusOK {
		t.Fatalf("Expected status code 200, got %d", status)
	}
	before := sessions()
	tooLong := strings.Repeat("x", 101) // sample.name is VARCHAR2(100)
	for range 5 {
		if status := postSample(t, http.DefaultClient, sampleBody(tooLong, "1")+"]"); status != http.StatusInternalServerError {
			t.Fatalf("Expected status code 500, got %d", status)
		}
	}
	if after := sessions(); after > before+1 {
		t.Fatalf("Expected failed writes to reuse pooled sessions, but sessions went from %d to %d", before, after)
	}
}
