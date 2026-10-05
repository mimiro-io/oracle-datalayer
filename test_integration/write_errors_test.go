package test_integration

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

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

// TestFailedWritesReleaseLocks checks that a failed request rolls back its
// transaction, so its row locks don't block later writes to the same rows.
func TestFailedWritesReleaseLocks(t *testing.T) {
	defer testServer().Stop()
	conn := freshTables(t)
	defer conn.Close()
	client := &http.Client{Timeout: 10 * time.Second}

	if status := postSample(t, client, sampleBody("before", "1", "2", "3", "4", "5", "6", "7", "8", "9")+"]"); status != http.StatusOK {
		t.Fatalf("Expected status code 200, got %d", status)
	}

	t.Run("error in Write", func(t *testing.T) {
		// ten rows reach the dataset's flush_threshold of 10, so the batch is
		// flushed inside Write. The repeated row collapses in the MERGE's UNION,
		// so the rows-affected check fails after the other rows were updated.
		if status := postSample(t, client, sampleBody("failed", "1", "2", "3", "4", "5", "6", "7", "8", "9", "9")+"]"); status != http.StatusBadRequest {
			t.Fatalf("Expected status code 400, got %d", status)
		}
		if status := postSample(t, client, sampleBody("after", "1")+"]"); status != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", status)
		}
	})

	t.Run("error in Close", func(t *testing.T) {
		// two rows stay below flush_threshold, so the batch is flushed in Close
		if status := postSample(t, client, sampleBody("failed", "5", "5")+"]"); status != http.StatusInternalServerError {
			t.Fatalf("Expected status code 500, got %d", status)
		}
		if status := postSample(t, client, sampleBody("after", "5")+"]"); status != http.StatusOK {
			t.Fatalf("Expected status code 200, got %d", status)
		}
	})

	var failed int
	if err := conn.QueryRow("SELECT COUNT(*) FROM sample WHERE name = 'failed'").Scan(&failed); err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if failed != 0 {
		t.Fatalf("Expected failed requests to be rolled back, found %d rows named failed", failed)
	}
}
