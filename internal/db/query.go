package db

import (
	"fmt"
	"github.com/mimiro-io/oracle-datalayer/internal/conf"
)

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

type TableQuery interface {
	BuildQuery() string
}

type FullQuery struct {
	Datalayer *conf.Datalayer
	Request   DatasetRequest
	TableDef  *conf.TableMapping
}

func NewQuery(request DatasetRequest, tableDef *conf.TableMapping, datalayer *conf.Datalayer) TableQuery {

	return FullQuery{
		Datalayer: datalayer,
		Request:   request,
		TableDef:  tableDef,
	}

}

func (q FullQuery) BuildQuery() string {

	limit := ""
	query := ""
	if q.Request.Limit > 0 {
		limit = fmt.Sprintf(" FETCH NEXT %d ROWS ONLY", q.Request.Limit)
	}
	if q.Datalayer.Schema != "" {
		query = fmt.Sprintf("select * from %s.%s", q.Datalayer.Schema, q.TableDef.TableName)
	} else if q.Datalayer.Schema == "" && limit == "" {
		query = fmt.Sprintf("select * from %s", q.TableDef.TableName)
	} else {
		query = fmt.Sprintf("select * from %s%s", q.TableDef.TableName, limit)
	}

	if q.TableDef.CustomQuery != "" {
		query = fmt.Sprintf(q.TableDef.CustomQuery, limit)
	}

	return query
}
