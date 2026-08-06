package pubsubqueue

import "fmt"

// Producer публикует сообщения в очередь.
type Producer interface {
	// Publish отправляет значение в очередь. Неблокирующий: возвращает ErrQueueFull если буфер заполнен.
	Publish(value int) error

	// WithFilter добавляет пользовательский предикат-фильтр (builder-паттерн).
	WithFilter(f Filter) Producer

	WithZeroFilter() Producer
	WithEvenFilter() Producer
	WithOddFilter() Producer
	WithMinMaxFilter(min, max int) Producer

	// Close закрывает связанную очередь.
	Close()
}

type producer struct {
	q       *queue
	filters []Filter
}

func newProducer(q *queue) *producer {
	return &producer{q: q}
}

// Publish потокобезопасен: несколько горутин могут вызывать его одновременно.
// Фильтры применяются к черновику с ID=0 до присвоения реального ID,
// поэтому предикаты не должны опираться на поле ID.
func (p *producer) Publish(value int) error {
	p.q.mu.RLock()
	defer p.q.mu.RUnlock()

	if p.q.closed {
		return fmt.Errorf("%w: %s", ErrQueueClosed, p.q.name)
	}

	draft := Message{Value: value}
	for _, f := range p.filters {
		if !f(draft) {
			return nil // сообщение отфильтровано
		}
	}

	m := newMessage(value)
	select {
	case p.q.buf <- m:
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrQueueFull, p.q.name)
	}
}

func (p *producer) WithFilter(f Filter) Producer {
	p.filters = append(p.filters, f)
	return p
}

func (p *producer) WithZeroFilter() Producer { return p.WithFilter(zeroFilter) }
func (p *producer) WithEvenFilter() Producer { return p.WithFilter(evenFilter) }
func (p *producer) WithOddFilter() Producer  { return p.WithFilter(oddFilter) }
func (p *producer) WithMinMaxFilter(min, max int) Producer {
	return p.WithFilter(func(m Message) bool { return m.Value >= min && m.Value <= max })
}

func (p *producer) Close() { p.q.closeQueue() }
