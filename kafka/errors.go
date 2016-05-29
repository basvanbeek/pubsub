package kafka

import "errors"

// Errors
var (
	ErrNoBrokers             = errors.New("at least one broker address is required")
	ErrNoTopic               = errors.New("topic name is required")
	ErrNoLogger              = errors.New("you need to provide a valid logger")
	ErrInvalidPartitionValue = errors.New("invalid partition value")
	ErrInvalidOffset         = errors.New("invalid offset")
	ErrNoSuccessHandler      = errors.New("you need to provide a valid success handler")
)
