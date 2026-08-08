package pubsubqueue

import (
	"errors"
	"testing"
)

func TestBrokerQueueDedup(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q1 := b.Queue("events")
	q2 := b.Queue("events")
	if q1 != q2 {
		t.Fatal("same name must return the same queue instance")
	}
}

func TestBrokerDistinctQueues(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	if b.Queue("a") == b.Queue("b") {
		t.Fatal("different names must return different queues")
	}
}

func TestBrokerCloseStopsPublish(t *testing.T) {
	b := NewBroker()
	p := b.NewProducer("q")
	b.Close()

	if err := p.Publish(1); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
}
