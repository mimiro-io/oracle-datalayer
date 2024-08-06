FROM golang:1.22.5 as build_base

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

FROM  build_base as builder
# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the legacy Go app
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o legacy-server cmd/oracle/main.go && \
  CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o oracle-datalayer cmd/oracle-datalayer/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/oracle-datalayer .
COPY --from=builder /app/legacy-server .

# Expose port 8080 to the outside world
EXPOSE 8080

CMD ["./legacy-server"]
