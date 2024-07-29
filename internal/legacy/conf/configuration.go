package conf

import (
	go_ora "github.com/sijms/go-ora/v2"
	"os"
)

type Datalayer struct {
	Id             string          `json:"id"`
	DatabaseServer string          `json:"databaseServer"`
	BaseUri        string          `json:"baseUri"`
	Database       string          `json:"database"`
	Port           int             `json:"port"`
	Schema         string          `json:"schema"`
	BaseNameSpace  string          `json:"baseNameSpace"`
	User           string          `json:"user"`
	Password       string          `json:"password"`
	Service        string          `json:"serviceName"`
	TableMappings  []*TableMapping `json:"tableMappings"`
	PostMappings   []*PostMapping  `json:"postMappings"`
}

type TableMapping struct {
	TableName           string           `json:"tableName"`
	NameSpace           string           `json:"nameSpace"`
	CustomQuery         string           `json:"query"`
	EntityIdConstructor string           `json:"entityIdConstructor"`
	SinceColumn         string           `json:"sinceColumn"`
	Types               []string         `json:"types"`
	ColumnMappings      []*ColumnMapping `json:"columnMappings"`
	Config              *TableConfig     `json:"config"`
	Columns             map[string]*ColumnMapping
}

type ColumnMapping struct {
	FieldName         string           `json:"fieldName"`
	PropertyName      string           `json:"propertyName"`
	IsIdColumn        bool             `json:"isIdColumn"`
	IsReference       bool             `json:"isReference"`
	IsEntity          bool             `json:"isEntity"`
	ReferenceTemplate string           `json:"referenceTemplate"`
	IgnoreColumn      bool             `json:"ignoreColumn"`
	IdTemplate        string           `json:"idTemplate"`
	ColumnMappings    []*ColumnMapping `json:"columnMappings"`
}

type PostMapping struct {
	DatasetName   string          `json:"datasetName"`
	TableName     string          `json:"tableName"`
	Query         string          `json:"query"`
	Config        *TableConfig    `json:"config"`
	IdColumn      string          `json:"idColumn"`
	FieldMappings []*FieldMapping `json:"fieldMappings"`
}

type TableConfig struct {
	DatabaseServer *string         `json:"databaseServer"`
	Database       *string         `json:"database"`
	Port           *int            `json:"port"`
	Schema         *string         `json:"schema"`
	User           *VariableGetter `json:"user"`
	Password       *VariableGetter `json:"password"`
	Service        *string         `json:"service"`
}

type FieldMapping struct {
	FieldName string `json:"fieldName"`
	SortOrder int    `json:"order"`
	Type      string `json:"type"`
}

type VariableGetter struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (v *VariableGetter) GetValue() string {
	switch v.Type {
	case "direct":
		return v.Key
	default:
		return os.Getenv(v.Key)
	}
}

func (layer *Datalayer) GetUrl(table *TableMapping) string {
	port := layer.Port
	server := layer.DatabaseServer
	service := layer.Service
	user := layer.User
	password := layer.Password
	if table != nil && table.Config != nil {
		if table.Config.Port != nil {
			port = *table.Config.Port
		}
		if table.Config.DatabaseServer != nil {
			server = *table.Config.DatabaseServer
		}
		if table.Config.Service != nil {
			service = *table.Config.Service
		}
		if table.Config.User != nil {
			user = table.Config.User.GetValue()
		}
		if table.Config.Password != nil {
			password = table.Config.Password.GetValue()
		}
	}
	oracleUrl := go_ora.BuildUrl(server, port, service, user, password, map[string]string{"SERVER": server + ":"})

	//connectionString := "oracle://" + user + ":" + password + "@" + server + ":" + port + "/XE"
	return oracleUrl
}
func (layer *Datalayer) GetPostUrl(mapping *PostMapping) string {
	port := layer.Port
	server := layer.DatabaseServer
	service := layer.Service
	user := layer.User
	password := layer.Password
	scheme := layer.Schema
	if scheme == "" {
		scheme = "oracle"
	}

	if mapping.Config != nil {
		if mapping.Config.Schema != nil {
			scheme = *mapping.Config.Schema
		}
		if mapping.Config.Port != nil {
			port = *mapping.Config.Port
		}
		if mapping.Config.DatabaseServer != nil {
			server = *mapping.Config.DatabaseServer
		}
		if mapping.Config.User != nil {
			user = mapping.Config.User.GetValue()
		}
		if mapping.Config.Password != nil {
			password = mapping.Config.Password.GetValue()
		}
	}

	oracleUrl := go_ora.BuildUrl(server, port, service, user, password, map[string]string{"SERVER": server + ":"})

	//connectionString := "oracle://" + user + ":" + password + "@" + server + ":" + port + "/XE"
	return oracleUrl

}
