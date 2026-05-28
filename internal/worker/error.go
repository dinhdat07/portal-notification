package worker

import "errors"

var ErrNonRetryable = errors.New("non-retryable error")
