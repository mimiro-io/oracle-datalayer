package layer

import (
	"context"
	"strings"
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

func TestQuoteOracleTableRef(t *testing.T) {
	cases := []struct{ in, want string }{
		{"sample2", `"SAMPLE2"`},
		{"STORFE.PROGNOSIS14", `"STORFE"."PROGNOSIS14"`},
		{"testuser.sample2", `"TESTUSER"."SAMPLE2"`},
		{"  spaced.col  ", `"SPACED"."COL"`},
	}
	for _, tc := range cases {
		if got := quoteOracleTableRef(tc.in); got != tc.want {
			t.Errorf("quoteOracleTableRef(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestAppendUsesQuotedSchemaTable(t *testing.T) {
	// INSERT ALL must quote schema and table separately (#28).
	o := &OracleWriter{table: "STORFE.PROGNOSIS14"}
	item := &RowItem{Columns: []string{"id"}, Values: []any{1}}
	if err := o.append(item); err != nil {
		t.Fatal(err)
	}
	got := o.batch.String()
	want := `INTO "STORFE"."PROGNOSIS14" (`
	if !strings.Contains(got, want) {
		t.Fatalf("append SQL missing %q in:\n%s", want, got)
	}
	if strings.Contains(got, `"STORFE.PROGNOSIS14"`) {
		t.Fatalf("append still quotes the whole schema.table as one identifier:\n%s", got)
	}
}
