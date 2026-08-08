package pubsubqueue

import "testing"

func TestQueueName(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("my-queue")
	if q.Name() != "my-queue" {
		t.Fatalf("want %q, got %q", "my-queue", q.Name())
	}
}

func TestQueueConsumerCount(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("q")
	if n := q.ConsumerCount(); n != 0 {
		t.Fatalf("want 0, got %d", n)
	}

	c1 := q.Subscribe()
	c2 := q.Subscribe()
	if n := q.ConsumerCount(); n != 2 {
		t.Fatalf("want 2, got %d", n)
	}

	c1.Close()
	if n := q.ConsumerCount(); n != 1 {
		t.Fatalf("want 1 after first close, got %d", n)
	}

	c2.Close()
	if n := q.ConsumerCount(); n != 0 {
		t.Fatalf("want 0 after second close, got %d", n)
	}
}
