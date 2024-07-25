package layer

import (
	"context"

	common "github.com/mimiro-io/common-datalayer"
)

type OracleDatalayer struct {
	db       *oracleDB
	datasets map[string]*Dataset
	config   *common.Config
	logger   common.Logger
	metrics  common.Metrics
}

type Dataset struct {
	logger            common.Logger
	db                *oracleDB
	datasetDefinition *common.DatasetDefinition
}

func (d *Dataset) MetaData() map[string]any {
	return d.datasetDefinition.SourceConfig
}

func (d *Dataset) Name() string {
	return d.datasetDefinition.DatasetName
}

func (dl *OracleDatalayer) Stop(ctx context.Context) error {
	// nothing to do for now
	return nil
}

func (dl *OracleDatalayer) Dataset(dataset string) (common.Dataset, common.LayerError) {
	ds, found := dl.datasets[dataset]
	if found {
		return ds, nil
	}
	return nil, ErrDatasetNotFound(dataset)
}

func (dl *OracleDatalayer) DatasetDescriptions() []*common.DatasetDescription {
	var datasetDescriptions []*common.DatasetDescription
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &common.DatasetDescription{Name: key})
	}
	return datasetDescriptions
}

func NewOracleDataLayer(conf *common.Config, logger common.Logger, metrics common.Metrics) (common.DataLayerService, error) {
	oracledb, err := newOracleDB(conf, logger, metrics)
	if err != nil {
		return nil, err
	}
	l := &OracleDatalayer{
		datasets: map[string]*Dataset{},
		logger:   logger,
		metrics:  metrics,
		config:   conf,
		db:       oracledb,
	}
	err = l.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}
	return l, nil
}
