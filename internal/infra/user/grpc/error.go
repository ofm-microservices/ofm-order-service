package grpc

import "errors"

var (
	ErrEmptyUserServiceAddress = errors.New("user service address is empty")
	ErrNilLogger               = errors.New("logger is nil")
)
