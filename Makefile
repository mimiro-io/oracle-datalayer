build:
	go build -o bin/server cmd/oracle-datalayer/main.go

build-legacy:
	go build -o bin/server cmd/oracle/main.go

run:
	go run cmd/oracle-datalayer/main.go

run-legacy:
	go run cmd/oracle/main.go

docker:
	docker build . -t oracle-datalayer

test:
	go test -v ./... -vet "all"