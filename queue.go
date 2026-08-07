package pubsubqueue

import "sync"

const defaultBufSize = 64

// Queue — именованный pub-sub канал между producer'ами и consumer'ами.
type Queue interface {
	Name() string
	Subscribe() Consumer
	ConsumerCount() int
}

type queue struct {
	name string
	buf  chan Message

	mu     sync.RWMutex // защищает closed; RLock на горячих путях publish/subscribe
	closed bool

	cmu       sync.Mutex // защищает slice consumer'ов
	consumers []*consumer
}

func newQueue(name string) *queue {
	q := &queue{
		name: name,
		buf:  make(chan Message, defaultBufSize),
	}
	go q.deliver()
	return q
}

func (q *queue) Name() string { return q.name }

func (q *queue) Subscribe() Consumer {
	q.mu.RLock()
	defer q.mu.RUnlock()

	c := newConsumer(q)
	if !q.closed {
		q.cmu.Lock()
		q.consumers = append(q.consumers, c)
		q.cmu.Unlock()
	}
	return c
}

func (q *queue) ConsumerCount() int {
	q.cmu.Lock()
	defer q.cmu.Unlock()
	return len(q.consumers)
}

func (q *queue) unsubscribe(c *consumer) {
	q.cmu.Lock()
	for i, cons := range q.consumers {
		if cons == c {
			q.consumers[i] = q.consumers[len(q.consumers)-1]
			q.consumers = q.consumers[:len(q.consumers)-1]
			break
		}
	}
	q.cmu.Unlock()
}

func (q *queue) closeQueue() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.buf)
	}
}

// deliver читает сообщения из буфера очереди и рассылает их всем подписанным consumer'ам.
// Работает в отдельной горутине на протяжении всего жизненного цикла очереди.
func (q *queue) deliver() {
	for m := range q.buf {
		q.cmu.Lock()
		targets := make([]*consumer, len(q.consumers))
		for i, c := range q.consumers {
			targets[i] = c
		}
		q.cmu.Unlock()

		for _, c := range targets {
			c.push(m)
		}
	}

	// Буферный канал закрыт — уведомляем всех оставшихся consumer'ов.
	q.cmu.Lock()
	for _, c := range q.consumers {
		c.shutdown()
	}
	q.consumers = nil
	q.cmu.Unlock()
}
