package layer

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"

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
	sinceCol, _ := d.datasetDefinition.SourceConfig[SinceColumn].(string)
	db := sql.OpenDB(d.db.connector)
	ctx := context.Background() // no timeout because we want to support long running stream operations

	var maxSince, nextToken string
	if sinceCol != "" {
		// build max since query
		maxSinceQuery := "SELECT MAX(" + sinceCol + ") AS \"_MAX_SINCE\" FROM " + d.datasetDefinition.SourceConfig[TableName].(string)
		maxRow := db.QueryRowContext(ctx, maxSinceQuery)
		if maxRow == nil || maxRow.Err() != nil {
			d.logger.Error("failed to get max since", "error", maxRow.Err())
			return nil, ErrQuery(maxRow.Err())
		}
		err := maxRow.Scan(&maxSince)
		if err != nil {
			d.logger.Error("failed to scan max since", "error", err)
			return nil, ErrQuery(err)
		}

		newSince := fmt.Sprintf("%v", maxSince)
		nextToken = base64.URLEncoding.EncodeToString([]byte(newSince))
	}

	// build the query
	query, err := buildQuery(d.datasetDefinition, since, maxSince, limit)
	d.logger.Debug(fmt.Sprintf("changes query for dataset %s: %s", d.Name(), query), "dataset", d.Name())
	if err != nil {
		d.logger.Error("failed to build query", "error", err)
		return nil, ErrQuery(err)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		d.logger.Error("failed to execute query", "error", err)
		return nil, ErrQuery(err)
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		d.logger.Error("failed to get column types", "error", err)
		return nil, ErrQuery(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		d.logger.Error("failed to get columns", "error", err)
		return nil, ErrQuery(err)
	}
	// primimg the rowBuf array with correct types for the scan
	// since we are targeting json, we only need to support the types that can be represented in json
	// namely string, number (float64), boolean
	rowBuf := make([]any, 0, len(cts))
	for _, ct := range cts {
		// oracle NUMBER(1,0) is a commonly used as boolean, but in newer versions there is a BOOLEAN
		// data type as well, which looks like NUMBER(38,255) to the driver.
		// we cant be sure that it is meant to be a boolean, so we need to check the mapping for a type hint
		var pm *common.ItemToEntityPropertyMapping
		for _, propMapping := range d.datasetDefinition.OutgoingMappingConfig.PropertyMappings {
			if strings.ToUpper(propMapping.Property) == ct.Name() {
				pm = propMapping
				break
			}
		}
		if ct.DatabaseTypeName() == "NUMBER" && pm != nil && (pm.IsDeleted || strings.HasPrefix(strings.ToUpper(pm.Datatype), "BOOL")) {
			rowBuf = append(rowBuf, &sql.NullBool{})
		} else {
			st := ct.ScanType()
			if st == nil {
				d.logger.Error("no scan type for column", "column", ct.Name())
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
		db:           db,
		rows:         rows,
		currentToken: nextToken,
		colTypes:     cts,
		columns:      columns,
		rowBuf:       rowBuf,
		sinceColumn:  sinceCol,
	}, nil
}

func buildQuery(definition *common.DatasetDefinition, since string, maxSince string, limit int) (string, error) {
	sinceCol, _ := definition.SourceConfig[SinceColumn].(string)
	cols := "*"
	if definition.OutgoingMappingConfig == nil {
		return "", fmt.Errorf("outgoing mapping config is missing")
	}
	if !definition.OutgoingMappingConfig.MapAll {
		cols = ""
		for _, pm := range definition.OutgoingMappingConfig.PropertyMappings {
			if len(cols) > 0 {
				cols = cols + ", "
			}
			cols = cols + pm.Property
		}
	}
	q := "SELECT " + cols + " FROM " + definition.SourceConfig[TableName].(string)

	_, err := strconv.Atoi(maxSince)
	if err != nil {
		maxSince = fmt.Sprintf("'%s'", maxSince)
	}

	if sinceCol != "" {
		if since != "" {
			sinceVal, err := base64.URLEncoding.DecodeString(since)
			if err != nil {
				return "", fmt.Errorf("failed to decode since token %s", since)
			}
			sinceValStr := string(sinceVal)
			_, err = strconv.Atoi(sinceValStr)
			if err != nil {
				sinceValStr = fmt.Sprintf("'%s'", sinceValStr)
			}

			q += fmt.Sprintf(" WHERE %s.%s > %s AND %s.%s <= %s",
				definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], sinceValStr,
				definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], maxSince)
		} else {
			q += fmt.Sprintf(" WHERE %s.%s <= %s",
				definition.SourceConfig[TableName], definition.SourceConfig[SinceColumn], maxSince)
		}
	}
	if limit != 0 {
		q += " FETCH FIRST " + strconv.Itoa(limit) + " ROWS ONLY"
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
	sinceColumn  string
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
	// cant currently skip token, egdm framework will produce invalid batch
	//if it.currentToken == "" {
	//	return nil, nil
	//}
	cont := egdm.NewContinuation()
	if it.currentToken != "" {
		cont.Token = it.currentToken
	}
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
