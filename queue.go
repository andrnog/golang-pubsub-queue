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

	cmu       sync.Mutex // защищает карту consumer'ов
	consumers map[*consumer]struct{}
}

func newQueue(name string) *queue {
	q := &queue{
		name:      name,
		buf:       make(chan Message, defaultBufSize),
		consumers: make(map[*consumer]struct{}),
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
		q.consumers[c] = struct{}{}
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
	delete(q.consumers, c)
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
		for c := range q.consumers {
			c.push(m)
		}
		q.cmu.Unlock()
	}

	// Буферный канал закрыт — уведомляем всех оставшихся consumer'ов.
	q.cmu.Lock()
	for c := range q.consumers {
		c.shutdown()
	}
	clear(q.consumers)
	q.cmu.Unlock()
}
