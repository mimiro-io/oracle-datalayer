package layer

import (
	"fmt"
	common "github.com/mimiro-io/common-datalayer"
)

var (
	ErrQuery           = common.Errorf(common.LayerErrorInternal, "failed to query database")
	ErrNotSupported    = common.Errorf(common.LayerErrorInternal, "operation not supported in this layer")
	ErrDatasetNotFound = func(datasetName string) common.LayerError {
		return common.Errorf(common.LayerErrorBadParameter, "dataset %s not found", datasetName)
	}
	ErrConnection = func(e error) common.LayerError {
		return common.Errorf(common.LayerErrorInternal, "failed to connect to database. %w", e)
	}
	ErrBatchSizeMismatch = func(observed, expected int) common.LayerError {
		return common.Errorf(common.LayerErrorInternal, "batch size mismatch. rows affected: %d, expected: %d", observed, expected)
	}
	ErrGeneric = func(msg string, extra ...any) common.LayerError {
		return common.Errorf(common.LayerErrorInternal, fmt.Sprintf(msg, extra...))
	}
)
