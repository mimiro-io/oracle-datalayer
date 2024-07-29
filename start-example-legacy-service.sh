# Usage: ./start-example-legacy-service.sh
#
#
# This script starts an Oracle database in a Docker container and
# primes it with some test data. It then runs the Oracle datalayer service
# with a configuration that maps the test data to a dataset.
#
# The startup of the Oracle database can take a minute or two, be patient.
#
# When ready, to test the service, run the following commands:
#
# 1. list the available datasets:
# curl http://localhost:8080/datasets
#
# 2. list the changes in the dataset:
# curl http://localhost:8080/datasets/TESTTABLE/changes
#
# 3. add an entity to the dataset:
# curl -d '[{"id": "@context", "namespaces": {"a":"http://test/"}},{"id":"a:3","refs":{},"props":{"a:id":3,"a:name":"test3"}}]' http://localhost:8080/datasets/testtable/entities
#
#!/bin/sh bash

mytmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')
trap "rm -rf $mytmpdir; docker kill oracle-free" EXIT

cat >${mytmpdir}/init.sql <<EOL
connect testuser/testpassword@FREEPDB1;
create table testtable (id number, name varchar2(50));
insert into testtable values (1, 'test');
insert into testtable values (2, 'test2');
commit;
EOL
cat >${mytmpdir}/conf.json <<EOL
{
  "id": "test-import",
  "databaseServer": "localhost",
  "baseUri": "http://data.test.io/testnamespace/",
  "database": "FREEPDB1",
  "port": 11521,
  "schema": "TESTUSER",
  "baseNameSpace": "http://data.test.io/newtestnamespace/",
  "user": "testuser",
  "password": "testpassword",
  "serviceName":"FREEPDB1",
  "postMappings": [
    {
      "datasetName": "testdata",
      "tableName": "testtable",
      "idColumn": "ID",
      "query": "BEGIN INSERT INTO TESTTABLE (ID, NAME) VALUES (:1, :2); EXCEPTION WHEN DUP_VAL_ON_INDEX THEN UPDATE TESTTABLE SET NAME=:2 WHERE ID = :1; END;",
      "fieldMappings": [
        {
          "fieldName": "id",
          "order": 1,
          "type": "varchar2(255)"
        },
        {
          "fieldName": "name",
          "order": 2,
          "type": "varchar2(255)"
        }
      ]
    }
  ],
  "tableMappings": [
    {
      "tableName": "TESTTABLE",
      "nameSpace": "tesuser",
      "entityIdConstructor": "testid/%s",
      "types": [
        "http://data.test.io/newtestnamespace/Test"
      ],
      "columnMappings": [
        {
          "fieldName": "ID",
          "isIdColumn": true
        }
      ]
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

CONFIG_LOCATION=file://$mytmpdir/conf.json go run ./cmd/oracle/main.go

# curl http://localhost:8080/datasets/TESTTABLE/changes
# curl -d '[{"id": "@context", "namespaces": {"a":"http://test/"}},{"id":"a:3","refs":{},"props":{"a:id":3,"a:name":"test3"}}]' http://localhost:8080/datasets/testtable/entities
