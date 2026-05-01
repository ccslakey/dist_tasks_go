package queue

import "errors"

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrUnknownTask    = errors.New("unknown task")
	ErrInvalidJob     = errors.New("invalid job")
)
