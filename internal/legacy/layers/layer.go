package layers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mimiro-io/oracle-datalayer/internal/legacy/conf"
	"github.com/mimiro-io/oracle-datalayer/internal/legacy/db"
	_ "github.com/sijms/go-ora/v2"
	"go.uber.org/fx"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Layer struct {
	cmgr   *conf.ConfigurationManager
	logger *zap.SugaredLogger
	Repo   *Repository //exported because it needs to deferred from main
	statsd statsd.ClientInterface
	env    *conf.Env
}

type Repository struct {
	DB       *sql.DB
	ctx      context.Context
	tableDef *conf.TableMapping
	digest   [16]byte
}

type DatasetRequest struct {
	DatasetName string
	Since       string
	Limit       int64
}

const jsonNull = "null"

func NewLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, env *conf.Env) *Layer {
	layer := &Layer{}
	layer.cmgr = cmgr
	layer.logger = env.Logger.Named("layer")
	layer.Repo = &Repository{
		ctx: context.Background(),
	}
	dbErr := layer.ensureConnection(nil)
	if dbErr != nil {
		//layer.logger.Error("Error connecting to database: ", dbErr.Error())
		return nil
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if layer.Repo.DB != nil {
				layer.Repo.DB.Close()
			}
			return nil
		},
	})

	return layer
}

func (l *Layer) GetDatasetPostNames() []string {
	names := make([]string, 0)
	for _, table := range l.cmgr.Datalayer.PostMappings {
		names = append(names, table.DatasetName)
	}
	return names
}
func (l *Layer) GetDatasetNames() []string {
	names := make([]string, 0)
	for _, table := range l.cmgr.Datalayer.TableMappings {
		names = append(names, table.TableName)
	}
	return names
}

func (l *Layer) GetTableDefinition(datasetName string) *conf.TableMapping {
	for _, table := range l.cmgr.Datalayer.TableMappings {
		if table.TableName == datasetName {
			return table
		}
	}
	return nil
}

func (l *Layer) GetContext(datasetName string) map[string]interface{} {
	tableDef := l.GetTableDefinition(datasetName)
	ctx := make(map[string]interface{})
	namespaces := make(map[string]string)

	namespace := tableDef.TableName
	if tableDef.NameSpace != "" {
		namespace = tableDef.NameSpace
	}

	namespaces["ns0"] = l.cmgr.Datalayer.BaseNameSpace + namespace + "/"
	namespaces["rdf"] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	ctx["namespaces"] = namespaces
	ctx["id"] = "@context"
	return ctx
}

func (l *Layer) DoesDatasetExist(datasetName string) bool {
	names := l.GetDatasetNames()
	for _, name := range names {
		if name == datasetName {
			return true
		}
	}
	return false
}

func (l *Layer) ChangeSet(request db.DatasetRequest, callBack func(*Entity)) error {

	tableDef := l.GetTableDefinition(request.DatasetName)
	if tableDef == nil {
		l.er(fmt.Errorf("could not find defined dataset: %s", request.DatasetName))
		return nil
	}

	err := l.ensureConnection(tableDef)
	if err != nil {
		return err
	}

	//create query, also checks if there is a custom query and uses that if set.
	query := db.NewQuery(request, tableDef, l.cmgr.Datalayer)
	var rows *sql.Rows

	since, err := l.getSince(l.Repo.DB, tableDef)
	l.logger.Debug(since)
	if err != nil {
		l.er(err)
		return err
	}

	rows, err = l.Repo.DB.QueryContext(l.Repo.ctx, query.BuildQuery())
	if err != nil {
		l.er(err)
		return err
	}

	// set up the row interface from the returned types

	cols, err := rows.Columns()
	colTypes, _ := rows.ColumnTypes()

	// set up the row interface from the returned types
	nullableRowData := buildRowType(cols, colTypes)

	for rows.Next() {
		err = rows.Scan(nullableRowData...)

		if err != nil {
			l.er(err)
		} else {
			// map it
			entity := l.toEntity(nullableRowData, cols, colTypes, tableDef)

			if entity != nil {
				// add types to entity
				if len(tableDef.Types) == 1 {
					entity.References["rdf:type"] = tableDef.Types[0]
				} else if len(tableDef.Types) > 1 {
					// multiple types...
					// fix me
				}

				// call back function
				callBack(entity)
			}
		}

	}
	if err := rows.Err(); err != nil {
		l.er(err)
		return nil // this is already at the end, we don't care about this error now
	}

	// clean it up
	return nil
}

func buildRowType(cols []string, colTypes []*sql.ColumnType) []interface{} {
	nullableRowData := make([]interface{}, len(cols))
	for i := range cols {
		colDef := colTypes[i]
		ctType := colDef.DatabaseTypeName()

		switch ctType {
		case "INTEGER", "INT":
			nullableRowData[i] = new(sql.NullInt64)
		case "VARCHAR", "NVARCHAR", "NCHAR", "NTEXT", "CHAR", "VARCHAR2", "NVARCHAR2":
			nullableRowData[i] = new(sql.NullString)
		case "DATETIME", "DATE", "DATETIME2", "TIMESTAMP", "TimeStampDTY", "TIMESTAMP WITH LOCAL TIME ZONE":
			nullableRowData[i] = new(sql.NullTime)
		case "MONEY", "DECIMAL":
			nullableRowData[i] = new(sql.NullFloat64)
		case "BIT":
			nullableRowData[i] = new(sql.NullBool)
		default:
			nullableRowData[i] = new(sql.RawBytes)
		}
	}
	return nullableRowData
}

func (l *Layer) er(err error) {
	l.logger.Warnf("Got error %s", err)
}

func (l *Layer) ensureConnection(table *conf.TableMapping) error {
	l.logger.Debug("Ensuring connection")
	var err error
	if l.Repo.DB == nil {
		err = errors.New("unintitialized")
	} else {
		err = l.Repo.DB.Ping()
	}
	if err != nil || l.cmgr.State.Digest != l.Repo.digest {
		if err != nil {
			l.logger.Info("Error pinging connection: ", err.Error(), ". Resetting connection")
		} else {
			l.logger.Debug("Configuration has changed need to reset connection")
		}
		if l.Repo.DB != nil {
			err2 := l.Repo.DB.Close()
			if err2 != nil {
				l.logger.Warn("Error closing old connection: ", err2.Error(), ". Ignoring and continuing")
			}
		}
		newDb, err2 := l.connect(table)
		if err2 != nil {
			l.Repo.DB = nil
			return err2
		}
		l.Repo.DB = newDb
		l.Repo.digest = l.cmgr.State.Digest
	}
	return nil
}

func (l *Layer) connect(table *conf.TableMapping) (*sql.DB, error) {

	u := l.cmgr.Datalayer.GetUrl(table)

	db, err := sql.Open("oracle", u)
	if err != nil {
		l.logger.Warn("Error creating connection: ", err.Error())
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		l.logger.Errorf("Could not ping db. DBURL: %s, err: %v", u, err)
		return nil, err
	}
	return db, nil
}

// mapColumns remaps the ColumnMapping into Column
func mapColumns(columns []*conf.ColumnMapping) map[string]*conf.ColumnMapping {
	cms := make(map[string]*conf.ColumnMapping)

	for _, cm := range columns {
		cms[cm.FieldName] = cm
	}
	return cms

}
func (l *Layer) toEntity(rowType []interface{}, cols []string, colTypes []*sql.ColumnType, tableDef *conf.TableMapping) *Entity {
	entity := NewEntity()

	for i, raw := range rowType {
		if raw != nil {
			ct := colTypes[i]
			ctName := ct.DatabaseTypeName()
			colName := cols[i]
			colMapping := tableDef.Columns[colName]
			colName = "ns0:" + colName

			var val interface{} = nil
			var strVal = ""

			if colName == "ns0:__$operation" {
				ptrToNullInt := raw.(*sql.NullInt64)
				if (*ptrToNullInt).Valid {
					operation := (*ptrToNullInt).Int64
					if operation == 1 {
						entity.IsDeleted = true
					}
				}
			}

			if colMapping != nil {
				if colMapping.IgnoreColumn {
					continue
				}

				if colMapping.PropertyName != "" {
					colName = colMapping.PropertyName
				}
			}

			entity.Properties[colName] = nil

			switch ctName {
			case "VARCHAR", "NVARCHAR", "TEXT", "NTEXT", "NCHAR", "VARCHAR2", "NVARCHAR2", "CHAR":
				ptrToNullString := raw.(*sql.NullString)
				if (*ptrToNullString).Valid {
					val = (*ptrToNullString).String
					strVal = val.(string)
					entity.Properties[colName] = val
				}
			case "UNIQUEIDENTIFIER":
				ptrToString := raw.(*sql.RawBytes)
				if (*ptrToString) != nil {
					uid, _ := uuid.FromBytes(*ptrToString)
					val = uid.String()
					entity.Properties[colName] = val
					strVal = val.(string)
				}
			case "DATETIME", "DATE", "DATETIME2", "TIMESTAMP", "TimeStampDTY", "TIMESTAMP WITH LOCAL TIME ZONE":
				ptrToNullDatetime := raw.(*sql.NullTime)
				if (*ptrToNullDatetime).Valid {
					val = (*ptrToNullDatetime).Time
					entity.Properties[colName] = val
				}
			case "INT", "SMALLINT", "TINYINT":
				ptrToNullInt := raw.(*sql.NullInt64)
				if (*ptrToNullInt).Valid {
					val = (*ptrToNullInt).Int64
					strVal = strconv.FormatInt((*ptrToNullInt).Int64, 10)
					entity.Properties[colName] = val
				}
			case "BIGINT":
				ptrToSomething := raw.(*sql.RawBytes)
				if *ptrToSomething != nil {
					val, err := toInt64(*ptrToSomething)
					if err != nil {
						l.logger.Warnf("Error converting to int64: %v", err)
					} else {
						strVal = strconv.FormatInt(val, 10)
						entity.Properties[colName] = val
					}
				}
			case "FLOAT", "FLOAT64", "NUMBER":
				ptrToSomething := raw.(*sql.RawBytes)
				if *ptrToSomething != nil {
					val, err := toFloat64(*ptrToSomething)
					if err != nil {
						l.logger.Warnf("Error converting ot float64: %v", err)
					} else {
						strVal = strconv.FormatFloat(val, 'f', -1, 64)
						entity.Properties[colName] = val
					}
				}
			case "MONEY", "DECIMAL":
				ptrToNullFloat := raw.(*sql.NullFloat64)
				if (*ptrToNullFloat).Valid {
					val = (*ptrToNullFloat).Float64
				}
			case "BIT":
				ptrToNullBool := raw.(*sql.NullBool)
				if (*ptrToNullBool).Valid {
					entity.Properties[colName] = (*ptrToNullBool).Bool
				} else {
					entity.Properties[colName] = false // default to false
				}
			default:
				l.logger.Errorf("Got: %s for %s", ctName, colName)
			}

			if colMapping != nil {
				// is this the id column
				if colMapping.IsIdColumn && strVal != "" {
					entity.ID = l.cmgr.Datalayer.BaseUri + fmt.Sprintf(tableDef.EntityIdConstructor, strVal)
				}

				if colMapping.IsReference && strVal != "" {
					entity.References[colName] = fmt.Sprintf(colMapping.ReferenceTemplate, strVal)
				}
			}
		}
	}

	if entity.ID == "" { // this is invalid
		l.logger.Warnf("Oooh, I got an empty id value from the database, this is probably pretty wrong.")
		return nil
	}

	return entity
}

// serverSince queries the server for its time, this can be used as the source of the since to return
// when using cdc (Oracle have put this in their Goldengate, TODO: look into if that can be implemented. The return value is Base64 encoded
//
/*func serverSince(db *pgxpool.Pool) (string, error) {
	var dt sql.NullTime
	err := db.QueryRow(context.Background(), "select current_timestamp;").Scan(&dt)
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("%s", dt.Time.Format(time.RFC3339))
	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}*/
func (l *Layer) getSince(db *sql.DB, tableDef *conf.TableMapping) (string, error) {

	s := ""
	if tableDef.SinceColumn != "" {
		var dt time.Time
		var query = fmt.Sprintf("SELECT MAX(%s) from %s", tableDef.SinceColumn, tableDef.TableName)
		row := db.QueryRow(query)
		err := row.Scan(&dt)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("%s", dt.Format("2006-01-02T15:04:05.000Z"))
	} else {
		var dt time.Time
		row := db.QueryRow("SELECT TRUNC(CURRENT_DATE) AS current_date FROM dual")
		err := row.Scan(&dt)
		if err != nil {
			return "", err
		}
		s = fmt.Sprintf("%s", dt.Format(time.RFC3339))
	}
	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}
func toInt64(payload sql.RawBytes) (int64, error) {
	content := reflect.ValueOf(payload).Interface().(sql.RawBytes)
	data := string(content)                  //convert to string
	i, err := strconv.ParseInt(data, 10, 64) // convert to int or your preferred data type
	if err != nil {
		return 0, err
	}
	return i, nil
}
func toFloat64(payload sql.RawBytes) (float64, error) {
	content := reflect.ValueOf(payload).Interface().(sql.RawBytes)
	data := string(content)
	i, err := strconv.ParseFloat(data, 64)
	if err != nil {
		return 0, err
	}
	return i, nil
}
