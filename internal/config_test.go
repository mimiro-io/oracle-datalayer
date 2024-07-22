package layer

import (
	common "github.com/mimiro-io/common-datalayer"
	"testing"
)

func testDeps() (*common.Config, common.Metrics, common.Logger) {
	return &common.Config{
			NativeSystemConfig: map[string]any{
				"oracle_db":       "test",
				"oracle_user":     "test",
				"oracle_password": "test",
				"oracle_hostname": "test",
				"oracle_port":     "1234",
			},
			DatasetDefinitions: []*common.DatasetDefinition{
				{DatasetName: "test"},
			},
		},
		nil, nil
}

func TestConfig(t *testing.T) {
	var subject common.DataLayerService
	setup := func() {
		conf, metrics, logger := testDeps()
		var err error
		subject = &OracleDatalayer{
			datasets: map[string]*Dataset{},
			logger:   logger,
			metrics:  metrics,
			config:   conf,
			db:       nil,
		}
		err = subject.UpdateConfiguration(conf)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("should add dataset definitions", func(t *testing.T) {
		setup()
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}
	})
	t.Run("should update dataset definitions", func(t *testing.T) {
		setup()
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}

		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test", SourceConfig: map[string]any{"test": "test"}}},
		}) != nil {
			t.Fatal("failed to update dataset definition")
		}
		ds, err = subject.Dataset("test")
		if err != nil {
			t.Fatal(err)

		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if ds.MetaData()["test"] != "test" {
			t.Fatal("source config not updated")
		}
	})
	t.Run("should remove dataset definitions", func(t *testing.T) {
		setup()
		if subject.UpdateConfiguration(&common.Config{
			DatasetDefinitions: []*common.DatasetDefinition{{DatasetName: "test"}},
		}) != nil {
			t.Fatal("failed to add dataset definition")
		}
		ds, err := subject.Dataset("test")
		if err != nil {
			t.Fatal(err)
		}
		if ds == nil {
			t.Fatal("dataset is nil")
		}
		if len(ds.MetaData()) != 0 {
			t.Fatal("empty here means non implicit")
		}

		if subject.UpdateConfiguration(&common.Config{}) != nil {
			t.Fatal("failed to remove dataset definition")
		}
		ds, err = subject.Dataset("test")
		if err == nil {
			t.Fatal("expected dataset not found error, all datasets are deleted herer")
		}
	})
	//t.Run("should fail on missing layer_config", func(t *testing.T) {
	//	t.Skip("check not doable without db, currently")
	//	conf, _, _ := testDeps()
	//	conf.LayerServiceConfig = nil
	//	err := subject.UpdateConfiguration(conf)
	//	if err == nil {
	//		t.Fatal("expected error")
	//	}
	//	if err.Error() != "missing required layer_config block" {
	//		t.Fatalf("unexpected error: %v", err)
	//	}
	//})
	//t.Run("should fail on missing system_config", func(t *testing.T) {
	//	t.Skip("check not doable without db, currently")
	//	conf, metrics, logger := testDeps()
	//	conf.NativeSystemConfig = nil
	//	_, err := NewOracleDataLayer(conf, logger, metrics)
	//	if err == nil {
	//		t.Fatal("expected error")
	//	}
	//	if err.Error() != "missing required system_config block" {
	//		t.Fatalf("unexpected error: %v", err)
	//	}
	//})
	//t.Run("should fail on missing required config", func(t *testing.T) {
	//	t.Skip("check not doable without db, currently")
	//	conf, metrics, logger := testDeps()
	//	// remove required config param
	//	_, err := NewOracleDataLayer(conf, logger, metrics)
	//	if err == nil {
	//		t.Fatal("expected error")
	//	}
	//	if err.Error() != "missing required config value oracle" {
	//		t.Fatalf("unexpected error: %v", err)
	//	}
	//})
	t.Run("with EnvOverrides", func(t *testing.T) {
		t.Setenv("ORACLE_DB", "overridden_test")
		t.Run("should override config with env vars", func(t *testing.T) {
			conf, _, _ := testDeps()
			EnvOverrides(conf)
			if conf.NativeSystemConfig["oracle_db"] != "overridden_test" {
				t.Fatalf("expected overridden_test, got %v", conf.NativeSystemConfig["oracle_db"])
			}
		})
	})
}
