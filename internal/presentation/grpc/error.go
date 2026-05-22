package grpc

import "errors"

var (
	ErrNilService = errors.New("service is nil")
	ErrNilLogger  = errors.New("logger is nil")
)
