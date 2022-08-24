build:
	go build -o bin/server cmd/oracle/main.go

run:
	go run cmd/oracle/main.go

docker:
	docker build . -t oracle-datalayer

test:
	go vet ./...
	go test ./... -v

testlocal:
	go vet ./...
	go test ./... -v

integration:
	go test ./... -v -tags=integration
