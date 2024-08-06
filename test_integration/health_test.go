package test_integration

import (
	"net/http"
	"testing"
)

/**
 * @api {test} /health TestHealthEndpoint
 *   Test the health endpoint, make sure it answers with 200 OK after the server is started
 */
func TestHealthEndpoint(t *testing.T) {
	defer testServer().Stop()

	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
	}
}
