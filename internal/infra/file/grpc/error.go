package grpc

import "errors"

var (
	ErrEmptyFileServiceAddress = errors.New("file service address is empty")
	ErrNilLogger               = errors.New("logger is nil")
)
