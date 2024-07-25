package test_integration

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"

	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/oracle-datalayer/internal"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	go_ora "github.com/sijms/go-ora/v2"
)

func TestMain(m *testing.M) {
	// Start a new docker pool
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	// Uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	oracleContainer, found := pool.ContainerByName("oracle-datalayer-testdb")
	if !found {
		//// default db: FREEPDB1
		oracleContainer, err = pool.RunWithOptions(&dockertest.RunOptions{
			Repository:   "gvenzl/oracle-free",
			Tag:          "slim-faststart",
			ExposedPorts: []string{"1521"},
			Name:         "oracle-datalayer-testdb",
			Env: []string{
				"ORACLE_PASSWORD=systempassword",
				"APP_USER=testuser",
				"APP_USER_PASSWORD=testpassword",
			},
		}, func(config *docker.HostConfig) {
			// set AutoRemove to true so that stopped container goes away by itself
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{
				Name: "no",
			}
		})
		//oracleContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		//	Repository: "oscarfonts/h2",
		//	Tag:        "latest",
		//}, func(config *docker.HostConfig) {
		//	config.AutoRemove = true
		//	config.RestartPolicy = docker.RestartPolicy{
		//		Name: "no",
		//	}
		//})
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}

		// if we start the container ourselves, we need to close it ourselves
		defer func() {
			if recover() != nil {
				log.Println("Recovered from panic")
			}
			// To keep the container running, comment out oracleContainer.Close().
			// This can speed up development. But greater care must be taken to clean up properly between runs.
			// `docker stop oracle-datalayer-testdb` can be used to stop the container manually.
			oracleContainer.Close()
		}()
	}

	// Set this so our app can use it
	oraclePort := oracleContainer.GetPort("1521/tcp")
	os.Setenv("ORACLE_PORT", oraclePort)
	port, err := strconv.Atoi(oraclePort)
	if err != nil {
		log.Fatalf("Could not convert port to int: %s", err)
	}
	connStr := go_ora.BuildUrl("localhost",
		port,
		//"XEPDB1",
		"FREEPDB1",
		"testuser",
		"testpassword",
		nil)
	os.Setenv("ORACLE_URL", connStr)

	// Wait for the db to be ready
	if err := pool.Retry(func() error {
		c, connErr := go_ora.NewConnection(connStr, nil)
		if connErr != nil {
			return connErr
		}
		oerr := c.Open()
		if oerr != nil {
			return oerr
		}
		perr := c.Ping(context.Background())
		if perr != nil {
			return perr
		}
		return nil
	}); err != nil {
		panic("Could not connect to test oracle: " + err.Error())
	}

	code := m.Run()
	if code != 0 {
		log.Fatalf("Test failed with code: %d", code)
	}
}

func testServer() *common.ServiceRunner {
	sr := common.NewServiceRunner(layer.NewOracleDataLayer).
		WithConfigLocation(".").
		WithEnrichConfig(layer.EnvOverrides)
	sr.Start()
	return sr
}

const baseURL = "http://localhost:11694"
