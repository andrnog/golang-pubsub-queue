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

	// Close отписывается от очереди и закрывает канал.
	Close()
}

// consumerBufCap — ёмкость буфера канала consumer'а. Если буфер полон, новые
// сообщения дропаются (at-most-once) — это и есть естественный backpressure.
const consumerBufCap = 64

type consumer struct {
	queue *queue
	ch    chan Message // буферизованный канал; его же отдаёт Messages()

	mu     sync.Mutex // сериализует отправку в ch с его закрытием
	closed bool

	pmu     sync.Mutex // защищает pending
	pending map[uint64]Message
}

// newConsumer создаёт consumer'а с буферизованным каналом. Если очередь уже
// закрыта — сразу возвращает «мёртвого» consumer'а с закрытым каналом.
func newConsumer(q *queue) *consumer {
	c := &consumer{
		queue:   q,
		ch:      make(chan Message, consumerBufCap),
		pending: make(map[uint64]Message),
	}
	if q.closed {
		c.closed = true
		close(c.ch)
	}
	return c
}

// Messages отдаёт наружу канал для чтения сообщений.
func (c *consumer) Messages() <-chan Message { return c.ch }

// Ack подтверждает обработку сообщения: удаляет его из pending.
// Если id там нет — возвращает ErrMessageNotFound.
func (c *consumer) Ack(id uint64) error {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	if _, ok := c.pending[id]; !ok {
		return fmt.Errorf("%w: id=%d", ErrMessageNotFound, id)
	}
	delete(c.pending, id)
	return nil
}

// Pending возвращает снимок всех неподтверждённых (не Ack'нутых) сообщений.
func (c *consumer) Pending() []Message {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	result := make([]Message, 0, len(c.pending))
	for _, m := range c.pending {
		result = append(result, m)
	}
	return result
}

// push вызывается горутиной deliver очереди. Неблокирующая доставка: если буфер
// полон — сообщение дропается. Отправка идёт под c.mu, поэтому никогда не
// пересекается с закрытием канала (иначе был бы panic: send on closed channel).
func (c *consumer) push(m Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	// Кладём в pending ДО отправки: как только сообщение попадёт в канал, читатель
	// может тут же его получить и вызвать Ack — к этому моменту pending уже должен
	// знать о сообщении, иначе Ack промахнётся и запись повиснет навсегда.
	c.pmu.Lock()
	c.pending[m.ID] = m
	c.pmu.Unlock()

	select {
	case c.ch <- m:
		// доставлено
	default:
		// буфер полон — сообщение не доставлено, откатываем pending (at-most-once)
		c.pmu.Lock()
		delete(c.pending, m.ID)
		c.pmu.Unlock()
	}
}

// closeChannel помечает consumer'а закрытым и закрывает ch. Идемпотентно; держит
// c.mu, поэтому безопасно даже при конкурентной попытке отправки в push.
func (c *consumer) closeChannel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.ch)
}

// Close — пользовательское закрытие: отписывается от очереди и закрывает канал.
func (c *consumer) Close() {
	c.queue.unsubscribe(c)
	c.closeChannel()
}

// shutdown вызывается очередью при её закрытии — закрывает канал consumer'а.
func (c *consumer) shutdown() {
	c.closeChannel()
}
