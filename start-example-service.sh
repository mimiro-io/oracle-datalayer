# Usage: ./start-example-service.sh
#
#
# This script starts an Oracle database in a Docker container and
# primes it with some test data. It then runs the Oracle datalayer service
# with a configuration that maps the test data to a dataset.
#
# This assumes that you have image gvenzl/oracle-free pulled locally and named oracle-free. If not, run:
# docker pull gvenzl/oracle-free:slim-faststart
# docker image tag gvenzl/oracle-free:slim-faststart oracle-free
# The startup of the Oracle database can take a minute or two, be patient.
#
# When ready, to test the service, run the following commands:
#
# 1. list the available datasets:
# curl http://localhost:8080/datasets
#
# 2. list the changes in the dataset:
# curl http://localhost:8080/datasets/testdata/changes
#
# 3. add an entity to the dataset:
# curl -d '[{"id": "@context", "namespaces": {"a":"http://test/"}},{"id":"a:3","refs":{},"props":{"a:id":3,"a:name":"test3"}}]' http://localhost:8080/datasets/testdata/entities
#
#!/bin/sh bash

mytmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')
trap "rm -rf $mytmpdir; docker kill oracle-free" EXIT

cat >${mytmpdir}/init.sql <<EOL
connect testuser/testpassword@FREEPDB1;
create table testtable (id number(8), name varchar2(50), "0" number(3), "14" number(4));
insert into testtable values (1, 'test', 100, 200);
insert into testtable values (2, 'test2', 300, 400);
insert into testtable values (3, 'test2', 500, 600);
insert into testtable values (4, 'test2', 700, 800);
insert into testtable values (5, 'test2', 900, 1000);
commit;
EOL
cat >${mytmpdir}/conf.json <<EOL
{
  "layer_config": {
    "port": "8080",
    "service_name": "test_service",
    "log_level": "warn",
    "log_format": "text",
    "config_refresh_interval": "24h"
  },
  "system_config": {
    "oracle_hostname": "localhost",
    "oracle_port": "11521",
    "oracle_db": "FREEPDB1",
    "oracle_user": "testuser",
    "oracle_password": "testpassword"
  },
  "dataset_definitions": [
    {
      "name": "testdata",
      "source_config": {
        "table_name": "testtable"
      },
      "incoming_mapping_config": {
        "base_uri": "http://data.sample.org/",
        "property_mappings": [
          {
            "property": "id",
            "is_identity": true,
            "strip_ref_prefix": true
          },
          {
            "entity_property": "http://test/name",
            "property": "name"
          },
          {
            "entity_property": "http://test/0",
            "property": "0"
          },
          {
            "entity_property": "http://test/14",
            "property": "14"
          }
        ]
      },
      "outgoing_mapping_config": {
        "base_uri": "http://data.sample.org/",
        "constructions":  [ {
          "property": "ID",
          "operation": "replace",
          "args": ["ID", ".000000", ""]
        } ],
        "property_mappings": [
          {
            "property": "ID",
            "is_identity": true,
            "uri_value_pattern": "http://test/id/{value}"
          },
          {
            "entity_property": "http://test/name",
            "property": "NAME"
          },
          {
            "entity_property": "http://test/0",
            "property": "0"
          },
          {
            "entity_property": "http://test/14",
            "property": "14"
          }
]
      }
    }
  ]
}
EOL

docker run -d --name oracle-free \
	--rm \
	-p 11521:1521 \
	-e ORACLE_PASSWORD=pwd \
	-e APP_USER=testuser \
	-e APP_USER_PASSWORD=testpassword \
	-v $mytmpdir/init.sql:/container-entrypoint-initdb.d/init.sql \
	gvenzl/oracle-free:slim-faststart

echo "Waiting for oracle-free to be ready"
until [[ $(docker logs oracle-free | grep "DATABASE IS READY TO USE!") ]]; do sleep 0.1; done
echo "oracle-free is running"

DATALAYER_CONFIG_PATH=$mytmpdir go run ./cmd/oracle-datalayer/main.go
