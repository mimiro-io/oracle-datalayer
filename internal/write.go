package layer

import (
	"context"
	"database/sql"
	"fmt"
	common "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/entity-graph-data-model"
	"strings"
)

func (d *Dataset) FullSync(ctx context.Context, batchInfo common.BatchInfo) (common.DatasetWriter, common.LayerError) {
	//TODO not supported (yet?)
	return nil, ErrNotSupported
}

func (d *Dataset) Incremental(ctx context.Context) (common.DatasetWriter, common.LayerError) {
	writer, err := d.newOracleWriter(ctx)
	writer.begin()
	return writer, err
}

func (d *Dataset) newOracleWriter(ctx context.Context) (*OracleWriter, common.LayerError) {
	mapper := common.NewMapper(d.logger, d.datasetDefinition.IncomingMappingConfig, d.datasetDefinition.OutgoingMappingConfig)
	db := sql.OpenDB(d.db.connector)
	tableName, ok := d.datasetDefinition.SourceConfig[TableName].(string)
	if !ok {
		return nil, ErrGeneric("table name not found in source config for dataset %s", d.datasetDefinition.DatasetName)
	}
	flushThreshold := 1000
	flushThresholdOverride, ok := d.datasetDefinition.SourceConfig[FlushThreshold]
	if ok {
		flushThresholdF, ok := flushThresholdOverride.(float64)
		if !ok {
			return nil, ErrGeneric("flush threshold must be an integer")
		}
		flushThreshold = int(flushThresholdF)
	}
	idColumn := "id"
	for _, m := range d.datasetDefinition.IncomingMappingConfig.PropertyMappings {
		if m.IsIdentity {
			idColumn = m.Property
			break
		}
	}
	return &OracleWriter{
		logger:         d.logger,
		mapper:         mapper,
		db:             db,
		ctx:            ctx,
		table:          tableName,
		flushThreshold: flushThreshold,
		appendMode:     d.datasetDefinition.SourceConfig[AppendMode] == true,
		idColumn:       idColumn,
	}, nil
}

type RowItem struct {
	Columns []string
	Values  []any
	Map     map[string]any
	deleted bool
}

func (r *RowItem) GetValue(name string) any {
	return r.Map[name]
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

type OracleWriter struct {
	logger         common.Logger
	mapper         *common.Mapper
	db             *sql.DB
	batch          strings.Builder
	batchSize      int
	ctx            context.Context
	table          string
	flushThreshold int
	appendMode     bool
	lastCols       []string
	idColumn       string
	tx             *sql.Tx
}

func (o *OracleWriter) Write(entity *egdm.Entity) common.LayerError {
	item := &RowItem{Map: map[string]any{}}
	err := o.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	// set the deleted flag, we always need this to do the right thing in upsert mode
	item.deleted = entity.IsDeleted

	if !o.appendMode {
		// if dataset is in latest only mode, we only keep one row per entity (unique by id).
		err = o.upsert(item)
	} else {
		// if dataset is not in latest only mode, we write all changes to the table. it is important to
		// have the deleted flag mapped to a column in the table, so that the data can reflect the state correctly
		err = o.append(item)
	}
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	if o.batchSize >= o.flushThreshold {
		err = o.flush()
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		o.batchSize = 0
		o.batch.Reset()
	}
	return nil
}

func (o *OracleWriter) Close() common.LayerError {
	err := o.flush()
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	if o.tx != nil {
		err = o.tx.Commit()
		if err != nil {
			return common.Err(err, common.LayerErrorInternal)
		}
		o.logger.Debug("Transaction committed")
	}
	err = o.db.Close()
	if err != nil {
		return common.Err(err, common.LayerErrorInternal)
	}
	return nil
}

// append builds an insert statement the complete batch.
// the insert statement is completed and executed when the batch is flushed
// en DEBUG mode, the complete statements are logged in flush
func (o *OracleWriter) append(item *RowItem) error {
	if o.batch.Len() == 0 {
		o.batch.WriteString("INSERT ALL\n")
	}
	o.batch.WriteString("\tINTO \"")
	o.batch.WriteString(strings.ToUpper(o.table))
	o.batch.WriteString("\" (")
	for i, k := range item.Columns {
		if i != 0 {
			o.batch.WriteString(", ")
		}
		o.batch.WriteRune('"')
		o.batch.WriteString(strings.ToUpper(k))
		o.batch.WriteRune('"')
	}
	o.batch.WriteString(") VALUES (")
	for i, v := range item.Values {
		if i != 0 {
			o.batch.WriteString(", ")
		}
		o.batch.WriteString(sqlVal(v))
	}
	o.batch.WriteString(")\n")
	o.batchSize++
	return nil
}

func sqlVal(v any) string {
	switch v.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (o *OracleWriter) flush() error {
	if o.batchSize == 0 {
		return nil
	}
	if o.appendMode {
		o.batch.WriteString("SELECT 1 FROM dual")
	} else {
		o.batch.WriteString(")) n ON (t.id = n.id)\nWHEN MATCHED THEN UPDATE SET ")
		needComma := false
		for _, col := range o.lastCols {
			if col == o.idColumn {
				continue
			}
			if needComma {
				o.batch.WriteString(", ")
			}
			o.batch.WriteString(fmt.Sprintf("t.%s = n.%s", col, col))
			needComma = true
		}
		o.batch.WriteString("\nDELETE WHERE n.\"_DELETED\"")
		o.batch.WriteString("\nWHEN NOT MATCHED THEN INSERT (")
		for i, col := range o.lastCols {
			if i != 0 {
				o.batch.WriteString(", ")
			}
			o.batch.WriteString(col)
		}
		o.batch.WriteString(") VALUES (")
		for i, col := range o.lastCols {
			if i != 0 {
				o.batch.WriteString(", ")
			}
			o.batch.WriteString(fmt.Sprintf("n.%s", col))
		}
		o.batch.WriteString(")")
	}
	stmt := o.batch.String()
	o.logger.Debug(stmt)
	res, err := o.tx.ExecContext(o.ctx, stmt)
	if err != nil {
		if o.tx != nil {
			err2 := o.tx.Rollback()
			if err2 != nil {
				o.logger.Error("Failed to rollback transaction")
				return fmt.Errorf("failed to rollback transaction: %w, underlying: %w", err2, err)
			}
			o.logger.Debug("Transaction rolled back")
		}
		return err
	}
	seen, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if int(seen) != o.batchSize {
		return ErrBatchSizeMismatch(int(seen), o.batchSize)
	}
	return nil
}

// Oracle does not have a proper upsert, but we can achieve the same (and even deletes) with a merge statement
// MERGE is supported in Oracle 9i and later
// the first call to upsert will start a new MERGE, and subsequent calls will more UNION SELECTs to the batch
// when the batch is flushed, the MERGE statement will be completed and  executed
// en DEBUG mode, the complete statements are logged in flush
func (o *OracleWriter) upsert(item *RowItem) error {
	if o.batch.Len() == 0 {
		o.batch.WriteString("MERGE INTO ")
		o.batch.WriteString(strings.ToUpper(o.table))
		o.batch.WriteString(" t USING ((\n\t")
	} else {
		o.batch.WriteString("\n\tUNION ")
	}
	o.batch.WriteString("SELECT ")
	for i, k := range item.Columns {
		o.batch.WriteString(sqlVal(item.Values[i]))
		o.batch.WriteString(" AS ")
		o.batch.WriteRune('"')
		o.batch.WriteString(strings.ToUpper(k))
		o.batch.WriteRune('"')
		//if i != len(item.Columns)-1 {
		o.batch.WriteString(", ")
		//}
	}
	// append synthetic column for deleted flag, so that the merge command can delete rows
	o.batch.WriteString(sqlVal(item.deleted))
	o.batch.WriteString(" AS \"_DELETED\"")
	o.batch.WriteString(" FROM dual")
	o.batchSize++
	o.lastCols = item.Columns
	return nil
}

func (o *OracleWriter) begin() error {
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	o.tx = tx
	o.logger.Debug("Transaction started")
	return nil
}
