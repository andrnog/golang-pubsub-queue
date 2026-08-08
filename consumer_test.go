package pubsubqueue

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsumerFanOut(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("q")
	c1 := q.Subscribe()
	c2 := q.Subscribe()
	p := b.NewProducer("q")

	// n is well within the default buffer size so synchronous publish works.
	const n = 50
	for i := 1; i <= n; i++ {
		if err := p.Publish(i); err != nil {
			t.Fatalf("Publish(%d): %v", i, err)
		}
	}

	readN := func(c Consumer) []int {
		out := make([]int, 0, n)
		for m := range c.Messages() {
			out = append(out, m.Value)
			if len(out) == n {
				c.Close()
			}
		}
		return out
	}

	var got1, got2 []int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); got1 = readN(c1) }()
	go func() { defer wg.Done(); got2 = readN(c2) }()
	wg.Wait()

	if len(got1) != n || len(got2) != n {
		t.Fatalf("want %d messages each, got %d and %d", n, len(got1), len(got2))
	}
	for i := range got1 {
		if got1[i] != got2[i] {
			t.Fatalf("consumers diverged at index %d: c1=%d c2=%d", i, got1[i], got2[i])
		}
	}
}

func TestConsumerAck(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	c := b.Queue("q").Subscribe()
	b.NewProducer("q").Publish(7)

	m := <-c.Messages()
	if len(c.Pending()) != 1 {
		t.Fatal("want 1 pending before Ack")
	}
	if err := c.Ack(m.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(c.Pending()) != 0 {
		t.Fatal("want 0 pending after Ack")
	}
}

func TestConsumerAckIsolation(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("q")
	c1 := q.Subscribe()
	c2 := q.Subscribe()
	b.NewProducer("q").Publish(42)

	m1 := <-c1.Messages()
	<-c2.Messages()

	if err := c1.Ack(m1.ID); err != nil {
		t.Fatal(err)
	}

	if len(c1.Pending()) != 0 {
		t.Fatal("c1: want 0 pending after Ack")
	}
	if len(c2.Pending()) != 1 {
		t.Fatal("c2: Ack on c1 must not affect c2")
	}
}

func TestConsumerAckNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	c := b.Queue("q").Subscribe()
	if err := c.Ack(999); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("want ErrMessageNotFound, got %v", err)
	}
}

func TestConsumerCloseUnsubscribes(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("q")
	c := q.Subscribe()
	c.Close()

	if n := q.ConsumerCount(); n != 0 {
		t.Fatalf("want 0 consumers after Close, got %d", n)
	}
}

func TestConsumerQueueClose(t *testing.T) {
	b := NewBroker()

	c := b.Queue("q").Subscribe()
	b.NewProducer("q").Publish(1)
	<-c.Messages() // drain the one message
	b.Close()      // triggers shutdown()

	// After queue closes, Messages() channel must close eventually.
	select {
	case _, ok := <-c.Messages():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout: Messages() channel never closed after broker Close()")
	}
}

// TestStressManyProducers — стресс: много конкурентных producer'ов долбят одну
// очередь. Проверяем отсутствие дедлока и чистое завершение (канал consumer'а
// закрывается после Close — иначе таймаут). Утечку горутин проверяет отдельно
// TestNoGoroutineLeak. Точное число полученных сообщений не детерминировано:
// часть теряется на backpressure (ErrQueueFull на q.buf и дроп при полном буфере
// consumer'а), поэтому сверяем не равенство, а сам факт корректного завершения.
func TestStressManyProducers(t *testing.T) {
	b := NewBroker()

	const (
		numProducers = 10
		msgsPerP     = 100_000
	)

	c := b.Queue("q").Subscribe()

	var accepted int64
	var wg sync.WaitGroup
	for range numProducers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := b.NewProducer("q")
			for j := range msgsPerP {
				if err := p.Publish(j); err == nil {
					atomic.AddInt64(&accepted, 1)
				}
			}
		}()
	}
	wg.Wait()
	b.Close()

	received := 0
	timeout := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-c.Messages():
			if !ok {
				t.Logf("accepted=%d received=%d (дропы на backpressure ожидаемы)",
					atomic.LoadInt64(&accepted), received)
				return
			}
			received++
		case <-timeout:
			t.Fatalf("timeout after receiving %d messages", received)
		}
	}
}
