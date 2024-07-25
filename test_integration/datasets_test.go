package test_integration

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
)

/**
 * @api {test} /datasets
 *   Test the /datasets endpoint, make sure it lists all registered datasets
 */
func TestDatasetsEndpoint(t *testing.T) {
	defer testServer().Stop()

	resp, err := http.Get(baseURL + "/datasets")
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status code 200, got %d", resp.StatusCode)
	}
	// get body as string from resp
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Could not read body from response")
	}
	// bodyStr := string(bodyBytes)
	// fmt.Println(bodyStr)
	var received []map[string]any
	json.Unmarshal(bodyBytes, &received)
	expected := []map[string]any{
		{"name": "sample", "description": "", "metadata": nil},
		{"name": "sample2", "description": "", "metadata": nil},
	}
	if !reflect.DeepEqual(received, expected) {
		t.Fatalf("Expected response to contain \n\n%s\n\nbut observed\n\n%s\n\n", expected, received)
	}
}
