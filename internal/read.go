package layer

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

	common "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

func (d *Dataset) Changes(since string, limit int, latestOnly bool) (common.EntityIterator, common.LayerError) {
	if latestOnly {
		// the layer does not know if the given table is a "change" table or not, so we cannot support this mode with confidence
		return nil, common.Err(fmt.Errorf("latest only operation not supported"), common.LayerNotSupported)
	}

	mapper := common.NewMapper(d.logger, d.datasetDefinition.IncomingMappingConfig, d.datasetDefinition.OutgoingMappingConfig)
	return d.newIterator(mapper, since, limit)
}

func (d *Dataset) Entities(from string, limit int) (common.EntityIterator, common.LayerError) {
	// the layer does not know if the given table is a "change" table or not, so implement /entities as /changes
	// TODO: consider adding source config options to allow for different behavior
	return d.Changes(from, limit, false)
}

func (d *Dataset) newIterator(mapper *common.Mapper, since string, limit int) (*dbIterator, common.LayerError) {
	db := sql.OpenDB(d.db.connector)
	query, err := buildQuery(d.datasetDefinition, since, limit)
	if err != nil {
		return nil, ErrQuery(err)
	}

	ctx := context.Background() // no timeout because we want to support long running stream operations
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, ErrQuery(err)
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, ErrQuery(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, ErrQuery(err)
	}
	// primimg the rowBuf array with correct types for the scan
	// since we are targeting json, we only need to support the types that can be represented in json
	// namely string, number (float64), boolean
	rowBuf := make([]any, 0, len(cts))
	for _, ct := range cts {
		ds, scale, ok := ct.DecimalSize()
		// oracle NUMBER(1,0) is a commonly used as boolean,
		// in newer oracle versions there is an actual boolean, which go_ora sees as NUMBER(38,255)
		if (ct.DatabaseTypeName() == "NUMBER" && ds == 1 && scale == 0 && ok) ||
			(ct.DatabaseTypeName() == "NUMBER" && ds == 38 && scale == 255 && ok) {
			rowBuf = append(rowBuf, &sql.NullBool{})
		} else {
			st := ct.ScanType()
			if st == nil {
				return nil, ErrQuery(fmt.Errorf("no scan type for column %s", ct.Name()))
			}
			ex := reflect.New(st).Interface()
			switch ex.(type) {
			case *bool:
				rowBuf = append(rowBuf, &sql.NullBool{})
			case *int64:
				rowBuf = append(rowBuf, &sql.NullFloat64{})
			case *float32:
				rowBuf = append(rowBuf, &sql.NullFloat64{})
			case *float64:
				rowBuf = append(rowBuf, &sql.NullFloat64{})
			default:
				rowBuf = append(rowBuf, &sql.NullString{})
			}
		}

	}

	return &dbIterator{
		logger:       d.logger,
		since:        since,
		limit:        limit,
		mapper:       mapper,
		currentToken: since,
		db:           db,
		rows:         rows,
		colTypes:     cts,
		columns:      columns,
		rowBuf:       rowBuf,
	}, nil
}

func buildQuery(definition *common.DatasetDefinition, since string, limit int) (string, error) {
	q := "SELECT * FROM " + definition.SourceConfig[TableName].(string)
	if since != "" {
		q += " WHERE " + definition.SourceConfig[SinceColumn].(string) + " > " + since
	}
	if limit != 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	return q, nil
}

type dbIterator struct {
	logger       common.Logger
	mapper       *common.Mapper
	db           *sql.DB
	rows         *sql.Rows
	since        string
	currentToken string
	colTypes     []*sql.ColumnType
	rowBuf       []any
	columns      []string
	limit        int
}

func (it *dbIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (it *dbIterator) Next() (*egdm.Entity, common.LayerError) {
	if it.rows.Next() {
		err := it.rows.Scan(it.rowBuf...)
		if err != nil {
			it.logger.Error("failed to scan row", "error", err)
			return nil, common.Err(err, common.LayerErrorInternal)
		}

		entity := egdm.NewEntity()
		ri := &RowItem{
			Columns: it.columns,
			// Values:  it.rowBuf,
			Map: make(map[string]any),
		}
		for i, col := range it.columns {
			ri.Map[col] = it.rowBuf[i]
		}

		err = it.mapper.MapItemToEntity(ri, entity)
		if err != nil {
			it.logger.Error("failed to map row", "error", err, "row", fmt.Sprintf("%+v", ri))
			return nil, common.Err(err, common.LayerErrorInternal)
		}
		return entity, nil

	} else {
		// exhausted or failed
		if it.rows.Err() != nil {
			it.logger.Error("failed to read rows", "error", it.rows.Err())
			return nil, common.Err(it.rows.Err(), common.LayerErrorInternal)
		}
		return nil, nil // end of result set
	}
}

func (it *dbIterator) Token() (*egdm.Continuation, common.LayerError) {
	cont := egdm.NewContinuation()
	cont.Token = it.currentToken
	return cont, nil
}

func (it *dbIterator) Close() common.LayerError {
	err := it.rows.Close()
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	err = it.db.Close()
	if err != nil {
		return ErrConnection(err)
	}
	return nil
}
