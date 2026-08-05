package pubsubqueue

import (
	"fmt"
	"sync"
)

// Consumer читает сообщения из очереди.
type Consumer interface {
	// Messages возвращает канал для чтения. Закрывается при закрытии consumer'а или очереди.
	Messages() <-chan Message

	// Ack подтверждает обработку сообщения, удаляя его из Pending.
	Ack(id uint64) error

	// Pending возвращает все неподтверждённые сообщения.
	Pending() []Message

	// Close отписывается от очереди и останавливает доставку.
	Close()
}

type consumer struct {
	queue *queue
	out   chan Message // пользовательский канал для чтения

	mu     sync.Mutex // защищает buf и closed
	cond   *sync.Cond
	buf    []Message
	closed bool // true когда новых сообщений не будет

	pmu     sync.Mutex // защищает pending
	pending map[uint64]Message

	closeOnce sync.Once
	doneOnce  sync.Once
	done      chan struct{} // закрывается при явном вызове Close() пользователем
}

const consumerBufCap = 16

func newConsumer(q *queue) *consumer {
	c := &consumer{
		queue:   q,
		out:     make(chan Message),
		buf:     make([]Message, 0, consumerBufCap),
		pending: make(map[uint64]Message),
		done:    make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)

	// Если очередь уже закрыта — возвращаем «мёртвый» consumer сразу.
	if q.closed {
		c.closed = true
		c.closeDone()
		close(c.out)
		return c
	}

	go c.run()
	return c
}

func (c *consumer) closeDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *consumer) Messages() <-chan Message { return c.out }

func (c *consumer) Ack(id uint64) error {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	if _, ok := c.pending[id]; !ok {
		return fmt.Errorf("%w: id=%d", ErrMessageNotFound, id)
	}
	delete(c.pending, id)
	return nil
}

func (c *consumer) Pending() []Message {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	result := make([]Message, 0, len(c.pending))
	for _, m := range c.pending {
		result = append(result, m)
	}
	return result
}

func (c *consumer) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		alreadyClosed := c.closed
		c.closed = true
		c.mu.Unlock()

		// Закрываем done чтобы run() вышел, даже если заблокирован на отправке в c.out.
		c.closeDone()
		c.cond.Signal()

		if !alreadyClosed {
			c.queue.unsubscribe(c)
		}
	})
}

// push вызывается горутиной deliver очереди для добавления сообщения во внутренний буфер.
func (c *consumer) push(m Message) {
	c.mu.Lock()
	c.buf = append(c.buf, m)
	c.mu.Unlock()
	c.cond.Signal()
}

// shutdown сигнализирует consumer'у, что очередь закрыта и новых сообщений не будет.
func (c *consumer) shutdown() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cond.Signal()
}

func (c *consumer) run() {
	defer close(c.out)

	for {
		c.mu.Lock()
		for len(c.buf) == 0 && !c.closed {
			c.cond.Wait()
		}
		if len(c.buf) == 0 { // закрыт с пустым буфером — завершаем горутину
			c.mu.Unlock()
			return
		}
		// Меняем буфер местами, чтобы не держать мьютекс во время отправки.
		batch := c.buf
		c.buf = make([]Message, 0, consumerBufCap)
		c.mu.Unlock()

		for _, m := range batch {
			c.pmu.Lock()
			c.pending[m.ID] = m
			c.pmu.Unlock()

			select {
			case c.out <- m:
			case <-c.done: // пользователь вызвал Close() — выходим немедленно
				return
			}
		}
	}
}
