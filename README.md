# UDA Datalayer for Oracle database

A Data Layer for [Oracle DB](https://www.oracle.com/) that conforms to the
[Universal Data API specification](https://open.mimiro.io/specifications/uda/latest.html).
This data layer can be used in conjunction with the [MIMIRO data hub](https://github.com/mimiro-io/datahub)
to create a modern data fabric.
The Oracle data layer can be configured to expose tables and views from an
Oracle SQL database as a stream of changes or a current snapshot. Rows in
a table are represented as JSON entities according to the
Entity Graph Data model that is described in the UDA specification.

This data layer can be run as a standalone binary or as a docker container.

Releases of this data layer are published to docker hub in the repository: mimiro/oracle-datalayer

## Configuration

The layer can be configured with a [common-datalayer configuration](https://github.com/mimiro-io/common-datalayer?tab=readme-ov-file#data-layer-configuration)
file.

Example for the `layer_config` section, which configures the API.:

```json
{
  "layer_config": {
    "service_name": "my-oracle-datalayer",
    "port": "8080",
    "config_refresh_interval": "600s",
    "log_level": "warn",
    "log_format": "json"
  }
}
```

In addition, the Oracle data layer requires a `system_config` section to configure the Oracle connection:

```json
{
  "system_config": {
    "oracle_hostname": "localhost",
    "oracle_port": "1521",
    "oracle_db": "FREEPDB1",
    "oracle_user": "testuser",
    "oracle_password": "testpassword"
  }
}
```

To add datasets (tables) to the configuration, refer to the [common-datalayer configuration](https://github.com/mimiro-io/common-datalayer?tab=readme-ov-file#data-layer-configuration).
The oracle specific options in a dataset configuration are these `source` options:

```json
{
  "source": {
    "table_name": "name of the mapped table", // required
    "flush_threshold": 1000, // max number of rows to buffer before writing to db. optional
    "append_mode": false, // default is false, if true, the layer will append all rows instead of updating rows with the same ID
    "since_column": "MY_COLUMN" // optional, column to use as a watermark for incremental reads
  }
}
```

### flush threshold

The layer will combine many DML operations into one big statement to improve performance. Depending
on the size of the rows, the maximum number of rows to buffer before writing to the database can be
adjusted. The default is 1000 rows.write

### append mode

The layer will update rows with the same ID by default. If `append_mode` is set to `true`, the layer
will instead append all rows. Make sure there are no duplicate IDs in the dataset if you enable this.
It is also advisable to map `recorded` and `deleted` columns in the dataset configuration to ensure
multiple versions of the same entity can be distinguished.

### since column

If the dataset is configured with a `since_column`, the layer will use this
column as a watermark in incremental reads.
The max value in the column will be encoded as continuation token in read responses.
In oracle, the synthetic `ROWID` column can be used as a `since_column` to
achieve incremental reads, even when the data does not have a suitable attribute.

See [here](./test_integration/integration-test-config.json) for a full example configuration.

## Running

### run the binary

From source:

```bash
DATALAYER_CONFIG_PATH=/path/to/config.json go run ./cmd/oracle-datalayer/main.go
```

### run the docker container

```bash
docker run \
  -p 8080:8080 \
  -v /path/to/config.json:./config/config.json \
  mimiro/oracle-datalayer oracle-datalayer
```

Note that most top level configuration parameters can be provided by environment
variables, overriding corresponding values in json configuration.
The accepted environment variables are:

```bash
DATALAYER_CONFIG_PATH
SERVICE_NAME
PORT
CONFIG_REFRESH_INTERVAL
LOG_LEVEL
LOG_FORMAT
STATSD_ENABLED
STATSD_AGENT_ADDRESS
ORACLE_HOSTNAME
ORACLE_PORT
ORACLE_DB
ORACLE_USER
ORACLE_PASSWORD
```

So a typical docker run command could look like this:

```bash
docker run \
  -p 8080:8080 \
  -e PORT=8080 \
  -e LOG_LEVEL=info \
  -e LOG_FORMAT=json \
  -e config_refresh_interval=1h \
  -e ORACLE_HOSTNAME=localhost \
  -e ORACLE_PORT=1521 \
  -e ORACLE_DB=FREEPDB1 \
  -e ORACLE_USER=testuser \
  -e ORACLE_PASSWORD=testpassword \
  -e DATALAYER_CONFIG_PATH=/etc/config.json \
  -v /path/to/config.json:/etc/config.json \
  mimiro/oracle-datalayer oracle-datalayer
```

## Legacy Datalayer

The repository contains an old version in `cmd/oracle` (and `internal/legacy`).
The old version uses a different configuration format and is for backwards
compatibility still the default version in the `mimiro/oracle-datalayer` docker image.

The new version will be the default version in the docker image in future releases.
