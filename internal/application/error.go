package application

import "errors"

var (
	ErrNilWriteRepository = errors.New("order write repository is nil")
	ErrNilReadRepository  = errors.New("order read repository is nil")
	ErrNilEventBroker     = errors.New("event broker is nil")
	ErrNilLogger          = errors.New("logger is nil")
	ErrPublishResult      = errors.New("publish order result failed")
)
