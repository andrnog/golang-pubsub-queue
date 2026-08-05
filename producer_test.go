package pubsubqueue

import (
	"errors"
	"sync"
	"testing"
)

func TestProducerPublish(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	c := b.Queue("q").Subscribe()
	p := b.NewProducer("q")

	if err := p.Publish(42); err != nil {
		t.Fatal(err)
	}
	if m := <-c.Messages(); m.Value != 42 {
		t.Fatalf("want 42, got %d", m.Value)
	}
}

func TestProducerFiltersEven(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("q").WithEvenFilter()
	c := b.Queue("q").Subscribe()

	p.Publish(1) // filtered
	p.Publish(2) // passes
	p.Publish(3) // filtered
	p.Publish(4) // passes

	if m := <-c.Messages(); m.Value != 2 {
		t.Fatalf("want 2, got %d", m.Value)
	}
	if m := <-c.Messages(); m.Value != 4 {
		t.Fatalf("want 4, got %d", m.Value)
	}
}

func TestProducerFiltersCombined(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// even AND in [1,10]
	p := b.NewProducer("q").WithEvenFilter().WithMinMaxFilter(1, 10)
	c := b.Queue("q").Subscribe()

	p.Publish(3)  // odd → out
	p.Publish(12) // even but > 10 → out
	p.Publish(4)  // passes
	p.Publish(6)  // passes

	if m := <-c.Messages(); m.Value != 4 {
		t.Fatalf("want 4, got %d", m.Value)
	}
	if m := <-c.Messages(); m.Value != 6 {
		t.Fatalf("want 6, got %d", m.Value)
	}
}

func TestProducerCustomFilter(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("q").WithFilter(func(m Message) bool { return m.Value > 100 })
	c := b.Queue("q").Subscribe()

	p.Publish(50)  // filtered
	p.Publish(150) // passes

	if m := <-c.Messages(); m.Value != 150 {
		t.Fatalf("want 150, got %d", m.Value)
	}
}

func TestProducerClosedQueue(t *testing.T) {
	b := NewBroker()
	p := b.NewProducer("q")
	b.Close()

	if err := p.Publish(1); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
}

func TestProducerFullQueue(t *testing.T) {
	// Bypass broker/deliver so the channel buffer actually fills up.
	// deliver() drains q.buf continuously, so we skip it intentionally here.
	q := &queue{
		name:      "tiny",
		buf:       make(chan Message, 1),
		consumers: make(map[*consumer]struct{}),
	}
	p := newProducer(q)

	if err := p.Publish(1); err != nil {
		t.Fatalf("first publish should succeed: %v", err)
	}
	if err := p.Publish(2); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull on full buffer, got %v", err)
	}
}

func TestProducerConcurrentPublish(t *testing.T) {
	b := NewBroker()

	p := b.NewProducer("q")
	c := b.Queue("q").Subscribe()

	const goroutines = 10
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Publish(1)
		}()
	}
	wg.Wait()
	p.Close() // close queue → deliver drains → consumer channel closes

	received := 0
	for range c.Messages() {
		received++
	}
	// All 10 fit in the buffer (size 64), so all must be delivered.
	if received != goroutines {
		t.Fatalf("want %d messages, got %d", goroutines, received)
	}
}
