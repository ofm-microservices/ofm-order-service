package yugabyte

import "errors"

var (
	ErrNilYugaByteDB = errors.New("yugabyte db is nil")
	ErrNilLogger     = errors.New("logger is nil")
)
