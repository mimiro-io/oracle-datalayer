package layer

import (
	"database/sql"
	"database/sql/driver"

	common "github.com/mimiro-io/common-datalayer"
	go_ora "github.com/sijms/go-ora/v2"
)

type oracleDB struct {
	connector driver.Connector
}

func newOracleDB(conf *common.Config, logger common.Logger, metrics common.Metrics) (*oracleDB, error) {
	c := oracleConf(conf)
	connStr := go_ora.BuildUrl(c.str(OracleHostname),
		c.int(OraclePort),
		c.str(OracleDB),
		c.str(OracleUser),
		c.str(OraclePassword),
		nil)
	connector := go_ora.NewConnector(connStr)
	connPool := sql.OpenDB(connector)
	defer connPool.Close()
	perr := connPool.Ping()
	if perr != nil {
		return nil, ErrConnection(perr)
	}
	return &oracleDB{connector}, nil
}

type RowItem struct {
	Map     map[string]any
	Columns []string
	Values  []any
	deleted bool
}

func (r *RowItem) GetValue(name string) any {
	val := r.Map[name]
	switch v := val.(type) {
	case *sql.NullBool:
		return v.Valid && v.Bool
	case *sql.NullString:
		if v.Valid {
			return v.String
		} else {
			return nil
		}
	case *sql.NullFloat64:
		if v.Valid {
			return v.Float64
		} else {
			return nil
		}
	default:
		return "invalid type"
	}
}

func (r *RowItem) SetValue(name string, value any) {
	r.Columns = append(r.Columns, name)
	r.Values = append(r.Values, value)
	r.Map[name] = value
}

func (r *RowItem) NativeItem() any {
	return r.Map
}

func (r *RowItem) GetPropertyNames() []string {
	return r.Columns
}
