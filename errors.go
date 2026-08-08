package pubsubqueue

import "errors"

var (
	ErrQueueClosed     = errors.New("queue is closed")
	ErrQueueFull       = errors.New("queue buffer is full")
	ErrMessageNotFound = errors.New("message not found")
)
