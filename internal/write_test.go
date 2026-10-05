package layer

import "testing"

func TestSqlVal(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"plain", "'plain'"},
		{"O'Brien", "'O''Brien'"},
		{"x' OR '1'='1", "'x'' OR ''1''=''1'"},
		{"''", "''''''"},
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
