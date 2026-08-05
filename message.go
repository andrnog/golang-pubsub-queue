package pubsubqueue

import "sync/atomic"

var globalID atomic.Uint64

// Message — единица данных в очереди.
// ID - уникален в рамках брокера, а не в рамках очереди. Таким образом можно однозначно идентифицировать сообщения
// не имея инфы о том из какой оно очереди
type Message struct {
	ID    uint64
	Value int
}

func newMessage(value int) Message {
	return Message{ID: globalID.Add(1), Value: value}
}
