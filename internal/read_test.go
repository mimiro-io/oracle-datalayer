package layer

import (
	"encoding/base64"
	"reflect"
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
		args                        []any
	}{
		{"numeric since", token("42"), "100",
			"SELECT * FROM t WHERE t.c > :1 AND t.c <= :2", []any{42, 100}},
		{"string since", token("abc"), "abd",
			"SELECT * FROM t WHERE t.c > :1 AND t.c <= :2", []any{"abc", "abd"}},
		{"injection in since", token("x' OR '1'='1"), "100",
			"SELECT * FROM t WHERE t.c > :1 AND t.c <= :2", []any{"x' OR '1'='1", 100}},
		{"invalid utf8 before quote", token("\xC3' OR 1=1 --"), "100",
			"SELECT * FROM t WHERE t.c > :1 AND t.c <= :2", []any{"\xC3' OR 1=1 --", 100}},
		{"quote in maxSince", "", "it's",
			"SELECT * FROM t WHERE t.c <= :1", []any{"it's"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, args, err := buildQuery(def, c.since, c.maxSince, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
			if !reflect.DeepEqual(args, c.args) {
				t.Errorf("got args  %#v\nwant args %#v", args, c.args)
			}
		})
	}

	t.Run("no since column", func(t *testing.T) {
		noSince := &common.DatasetDefinition{
			SourceConfig:          map[string]any{TableName: "t"},
			OutgoingMappingConfig: &common.OutgoingMappingConfig{MapAll: true},
		}
		got, args, err := buildQuery(noSince, token("42"), "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != "SELECT * FROM t" || args != nil {
			t.Errorf("got %s %#v, want SELECT * FROM t and no args", got, args)
		}
	})
}
