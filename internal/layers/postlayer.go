package layers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/mimiro-io/oracle-datalayer/internal/conf"
	go_ora "github.com/sijms/go-ora/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"sort"
	"strings"
)

type PostLayer struct {
	cmgr     *conf.ConfigurationManager //
	logger   *zap.SugaredLogger
	PostRepo *PostRepository //exported because it needs to deferred from main??
}
type PostRepository struct {
	DB           *sql.DB
	ctx          context.Context
	postTableDef *conf.PostMapping
	digest       [16]byte
}

func NewPostLayer(lc fx.Lifecycle, cmgr *conf.ConfigurationManager, logger *zap.SugaredLogger) *PostLayer {
	postLayer := &PostLayer{logger: logger.Named("layer")}
	postLayer.cmgr = cmgr
	postLayer.PostRepo = &PostRepository{
		ctx: context.Background(),
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if postLayer.PostRepo.DB != nil {
				err := postLayer.PostRepo.DB.Close()
				if err != nil {
					return err
				}
			}
			return nil
		},
	})
	return postLayer
}

func (postLayer *PostLayer) connect(table *conf.PostMapping) (*sql.DB, error) {
	//test with pure sql
	u := postLayer.cmgr.Datalayer.GetPostUrl(table)

	db, err := sql.Open("oracle", u)

	if err != nil {
		postLayer.logger.Warn("Error creating connection: ", err.Error())
		return nil, err
	}

	err = db.PingContext(postLayer.PostRepo.ctx)
	if err != nil {
		postLayer.logger.Warn(err.Error())
		return nil, err
	}

	return db, nil
}

func (postLayer *PostLayer) PostEntities(datasetName string, entities []*Entity) error {

	postLayer.PostRepo.postTableDef = postLayer.GetTableDefinition(datasetName)

	if postLayer.PostRepo.postTableDef == nil {
		return errors.New(fmt.Sprintf("No configuration found for dataset: %s", datasetName))
	}

	connString := postLayer.getConnString(postLayer.PostRepo.postTableDef)
	conn, err := go_ora.NewConnection(connString)
	if err != nil {
		return err
	}
	err = conn.Open()
	if err != nil {
		return err
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			postLayer.logger.Warnf("Unable to close connection: %s", err)
		}
	}()
	query := postLayer.PostRepo.postTableDef.Query
	if query == "" {
		postLayer.logger.Errorf("Please add query in config for %s in ", datasetName)
		return errors.New(fmt.Sprintf("no query found in config for dataset: %s", datasetName))
	}
	//postLayer.logger.Debug(query)
	// requires idColumn to be the first in sortOrder
	queryDel := fmt.Sprintf(`DELETE FROM %s WHERE %s = :1`, strings.ToLower(postLayer.PostRepo.postTableDef.TableName), strings.ToLower(postLayer.PostRepo.postTableDef.IdColumn))

	//postLayer.logger.Debug(queryDel)

	fields := postLayer.PostRepo.postTableDef.FieldMappings

	if len(fields) == 0 {
		postLayer.logger.Errorf("Please define all fields in config that is involved in dataset %s and query: %s", datasetName, query)
		return errors.New("fields needs to be defined in the configuration")
	}

	//Only Sort Fields if SortOrder is set
	count := 0
	for _, field := range fields {
		if field.SortOrder == 0 {
			count++
		}
	}
	if count >= 2 {
		postLayer.logger.Warn("No sort order is defined for fields in config, this might corrupt the query")
	} else {
		sort.SliceStable(fields, func(i, j int) bool {
			return fields[i].SortOrder < fields[j].SortOrder
		})
	}
	//BULKINSERT
	//TODO: Get proper batching in here bulkInsert seems to have som bugs as of 18/08/2022
	/*	rowNum := batchSize
		payload := make(map[string][]driver.Value)
		for _, field := range fields {
			payload[field.FieldName] = make([]driver.Value, 0)
		}*/
	args := make([]driver.Value, len(fields))
	for _, entity := range entities {
		s := entity.StripProps()
		//insert the values of each Field in an array in payload with the correct FieldName BULKINSERT
		//TODO: Get proper batching in here bulkInsert seems to have som bugs as of 18/08/2022
		/*		for _, field := range fields {
				// fieldName := make([]driver.Value, rowNum)
				payload[field.FieldName] = append(payload[field.FieldName], s[field.FieldName])
			}*/

		for i, field := range fields {
			args[i] = s[field.FieldName]
		}
		if !entity.IsDeleted { //If is deleted True --> Do not store
			_, err := conn.Exec(query, args...)
			if err != nil {
				postLayer.logger.Error(err)
				return err
			}
		} else { //Should be deleted if it exists
			postLayer.logger.Info(queryDel)
			_, err := conn.Exec(queryDel, args[0])
			if err != nil {
				postLayer.logger.Error(err)
				return err
			}
		}
	}
	return nil
}

//BULKINSERT
//TODO: Get proper batching in here bulkInsert seems to have som bugs as of 18/08/2022
/*var columns [][]driver.Value
	for _, field := range fields {
		columns = append(columns, payload[field.FieldName])
	}
	result, err := conn.BulkInsert(query, 5, columns...)
	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("%v rows inserted\n", rowsAffected)
	return nil
}*/

func (postLayer *PostLayer) getConnString(table *conf.PostMapping) string {
	connString := postLayer.cmgr.Datalayer.GetPostUrl(table)
	return connString
}

func (postLayer *PostLayer) GetTableDefinition(datasetName string) *conf.PostMapping {
	for _, table := range postLayer.cmgr.Datalayer.PostMappings {
		if table.DatasetName == datasetName {
			return table
		} else if table.TableName == datasetName { // fallback
			return table
		}
	}
	return nil
}
