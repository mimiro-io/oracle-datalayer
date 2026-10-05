package layer

import (
	"encoding/base64"
	"testing"

	common "github.com/mimiro-io/common-datalayer"
)

func TestBuildQuerySince(t *testing.T) {
	def := &common.DatasetDefinition{
		SourceConfig:          map[string]any{TableName: "t", SinceColumn: "c"},
		OutgoingMappingConfig: &common.OutgoingMappingConfig{MapAll: true},
	}
	token := func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }

	cases := []struct {
		name, since, maxSince, want string
	}{
		{"numeric since", token("42"), "100",
			"SELECT * FROM t WHERE t.c > 42 AND t.c <= 100"},
		{"string since", token("abc"), "abd",
			"SELECT * FROM t WHERE t.c > 'abc' AND t.c <= 'abd'"},
		{"injection in since", token("x' OR '1'='1"), "100",
			"SELECT * FROM t WHERE t.c > 'x'' OR ''1''=''1' AND t.c <= 100"},
		{"quote in maxSince", "", "it's",
			"SELECT * FROM t WHERE t.c <= 'it''s'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildQuery(def, c.since, c.maxSince, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}
