package pubsubqueue

import (
	"sync"
)

// Broker управляет именованными очередями и их producer'ами.
type Broker interface {
	Queue(name string) Queue
	NewProducer(queueName string) Producer
	Close()
}

type broker struct {
	mu       sync.Mutex
	queues   map[string]*queue
	isClosed bool // флаг закрытия брокера. Если закрыт - кидаем панику. По аналогии с записью в закрытый канал
}

func NewBroker() Broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) Queue(name string) Queue {
	return b.getOrCreate(name)
}

func (b *broker) NewProducer(queueName string) Producer {
	return newProducer(
		b.getOrCreate(queueName),
	)
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed {
		return
	}

	b.isClosed = true
	for _, q := range b.queues {
		q.closeQueue()
	}
	clear(b.queues)
}

func (b *broker) getOrCreate(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isClosed {
		panic("broker is closed")
	}
	if q, ok := b.queues[name]; ok {
		return q
	}
	q := newQueue(name)
	b.queues[name] = q
	return q
}
