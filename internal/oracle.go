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
