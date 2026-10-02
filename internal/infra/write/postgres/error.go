package postgres

import "errors"

var (
	ErrNilPostgresDB = errors.New("postgres db is nil")
	ErrNilLogger     = errors.New("logger is nil")
)
