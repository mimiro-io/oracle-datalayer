package layer

import (
	"fmt"
	common "github.com/mimiro-io/common-datalayer"
	"strconv"
	"strings"
)

const (
	// dataset mapping config
	TableName      = "table_name"
	FlushThreshold = "flush_threshold"
	AppendMode     = "append_mode"
	SinceColumn    = "since_column"

	// native system config
	OracleHostname = "oracle_hostname"
	OraclePort     = "oracle_port"
	OracleDB       = "oracle_db"
	OracleUser     = "oracle_user"
	OraclePassword = "oracle_password"
)

func EnvOverrides(config *common.Config) error {
	return common.BuildNativeSystemEnvOverrides(
		common.Env("oracle_hostname", true),
		common.Env("oracle_port", true),
		common.Env("oracle_db", true),
		common.Env("oracle_user", true),
		common.Env("oracle_password", true),
	)(config)
}

type oraConf struct {
	common.NativeSystemConfig
}

func (c oraConf) str(key string) string {
	return c.NativeSystemConfig[key].(string)
}

func (c oraConf) int(key string) int {
	intVal, err := strconv.Atoi(c.NativeSystemConfig[key].(string))
	if err != nil {
		panic(fmt.Errorf("NativeSystemConfig key %s is not an int", key))
	}
	return intVal
}

func oracleConf(config *common.Config) oraConf {
	return oraConf{config.NativeSystemConfig}
}

func (dl *OracleDatalayer) UpdateConfiguration(config *common.Config) common.LayerError {
	existingDatasets := map[string]bool{}
	// update existing datasets
	for k, v := range dl.datasets {
		for _, dsd := range config.DatasetDefinitions {
			if k == dsd.DatasetName {
				existingDatasets[k] = true
				v.datasetDefinition = dsd
			}
		}
	}
	// remove deleted datasets
	for k := range dl.datasets {
		if _, found := existingDatasets[k]; !found {
			delete(dl.datasets, k)
		}
	}

	// add new datasets
	for _, dsd := range config.DatasetDefinitions {
		if _, found := existingDatasets[dsd.DatasetName]; !found {
			dl.datasets[dsd.DatasetName] = &Dataset{
				logger:            dl.logger,
				db:                dl.db,
				datasetDefinition: dsd,
			}
		}
	}

	// convert all column names to uppercase
	for _, ds := range dl.datasets {
		if ds.datasetDefinition.OutgoingMappingConfig != nil {
			for _, pm := range ds.datasetDefinition.OutgoingMappingConfig.PropertyMappings {
				pm.Property = strings.ToUpper(pm.Property)
			}
		}
	}

	return nil
}
