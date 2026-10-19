package queue

import "errors"

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrUnknownTask    = errors.New("unknown task")
	ErrInvalidJob     = errors.New("invalid job")
	// ErrLeaseLost means the caller no longer owns the job's delivery: it was
	// recovered by another worker (or redelivered) after the lease expired.
	ErrLeaseLost = errors.New("lease lost")
)
