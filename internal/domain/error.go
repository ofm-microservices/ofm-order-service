package domain

import "errors"

var (
	ErrOrderNotFound = errors.New("order not found")
	ErrOrderNotOwned = errors.New("order not owned")
	ErrInvalidOrder  = errors.New("invalid order")
)
