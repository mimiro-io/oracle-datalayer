package layer

import (
	"context"
	"testing"

	common "github.com/mimiro-io/common-datalayer"
)

func TestSqlVal(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"plain", "'plain'"},
		{"O'Brien", "'O''Brien'"},
		{"x' OR '1'='1", "'x'' OR ''1''=''1'"},
		{"''", "''''''"},
		{"\xC3'", "'\uFFFD'''"},
		{nil, "NULL"},
		{true, "'true'"},
		{42, "42"},
		{1.5, "1.5"},
	}
	for _, c := range cases {
		if got := sqlVal(c.in); got != c.want {
			t.Errorf("sqlVal(%#v) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestIncrementalWithoutTableName checks that a dataset without table_name
// fails cleanly instead of panicking.
func TestIncrementalWithoutTableName(t *testing.T) {
	_, _, logger := testDeps()
	ds := &Dataset{
		logger:            logger,
		datasetDefinition: &common.DatasetDefinition{DatasetName: "test", SourceConfig: map[string]any{}},
	}
	w, err := ds.Incremental(context.Background())
	if err == nil {
		t.Fatal("expected an error for a dataset without table_name")
	}
	if w != nil {
		t.Fatalf("expected no writer, got %v", w)
	}
}
