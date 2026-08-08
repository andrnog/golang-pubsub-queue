package pubsubqueue

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// waitGoroutines ждёт, пока число горутин не опустится до target (или ниже),
// но не дольше timeout. Используется как дешёвая (без внешних зависимостей)
// проверка на утечку горутин вместо goleak.
func waitGoroutines(t *testing.T, target int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n := runtime.NumGoroutine()
		if n <= target {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("горутины не завершились: got %d, want <= %d", n, target)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNoGoroutineLeak проверяет главный сценарий БЛОКЕРА 1: после закрытия
// брокера и вычитывания каналов не должно остаться фоновых горутин (deliver
// обязан выйти при закрытии q.buf).
func TestNoGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()

	b := NewBroker()
	q := b.Queue("q")
	c1 := q.Subscribe()
	c2 := q.Subscribe()
	p := b.NewProducer("q")

	for i := 0; i < 100; i++ {
		p.Publish(i)
	}
	<-c1.Messages()
	<-c2.Messages()

	b.Close()

	// Дренируем до конца, чтобы range завершился по закрытию каналов.
	for range c1.Messages() {
	}
	for range c2.Messages() {
	}

	waitGoroutines(t, base, 2*time.Second)
}

// TestPublishNeverBlocksWithDeadConsumer — критерий 1: Publish не блокирует
// вызывающую горутину, даже если подписчик вообще ничего не читает.
func TestPublishNeverBlocksWithDeadConsumer(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	q := b.Queue("q")
	_ = q.Subscribe() // подписчик, который никогда не читает
	p := b.NewProducer("q")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100_000; i++ {
			p.Publish(i) // не должно блокировать
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish заблокировался при неработающем consumer'е")
	}
}

// TestSubscribeAfterQueueClosed — ветка «мёртвого» consumer'а: подписка на уже
// закрытую очередь должна вернуть consumer'а с сразу закрытым каналом.
func TestSubscribeAfterQueueClosed(t *testing.T) {
	b := NewBroker()
	p := b.NewProducer("q") // создаёт очередь
	q := b.Queue("q")
	p.Close() // закрывает очередь

	c := q.Subscribe()
	select {
	case _, ok := <-c.Messages():
		if ok {
			t.Fatal("канал «мёртвого» consumer'а должен быть закрыт")
		}
	case <-time.After(time.Second):
		t.Fatal("Messages() consumer'а на закрытой очереди должен быть уже закрыт")
	}
}

// TestRepeatedClose — повторный Close() consumer'а не должен паниковать.
func TestRepeatedClose(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	c := b.Queue("q").Subscribe()
	c.Close()
	c.Close() // идемпотентно, без паники
}

// TestConsumerCloseAfterQueueClose — сначала очередь закрывает канал consumer'а
// (shutdown), затем пользователь зовёт Close(): не должно быть паники/двойного close.
func TestConsumerCloseAfterQueueClose(t *testing.T) {
	b := NewBroker()

	c := b.Queue("q").Subscribe()
	b.Close() // shutdown закрывает канал consumer'а
	c.Close() // не должно паниковать
}

// TestConcurrentAckPendingClose — конкурентные Ack/Pending/Close с одного
// consumer'а: под -race не должно быть гонок (pmu/mu).
func TestConcurrentAckPendingClose(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	c := b.Queue("q").Subscribe()
	p := b.NewProducer("q")
	for i := 0; i < 200; i++ {
		p.Publish(i)
	}

	var wg sync.WaitGroup

	// Читатели, подтверждающие сообщения.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range c.Messages() {
				_ = c.Ack(m.ID)
			}
		}()
	}

	// Параллельные читатели Pending().
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = c.Pending()
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	c.Close() // закрывает канал → range'ы читателей завершаются
	wg.Wait()
}

// TestProducerFilterRace — регресс на БЛОКЕР 2: конкурентные Publish и WithFilter
// на одном продюсере. До фикса падает под -race, после — чисто.
func TestProducerFilterRace(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	p := b.NewProducer("q")
	_ = b.Queue("q").Subscribe()

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				p.Publish(j)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				p.WithFilter(func(m Message) bool { return true })
			}
		}()
	}

	wg.Wait()
}

// BenchmarkPublish — пропускная способность и аллокации на сообщение
// при активном подписчике.
func BenchmarkPublish(b *testing.B) {
	br := NewBroker()
	defer br.Close()

	c := br.Queue("q").Subscribe()
	go func() {
		for range c.Messages() {
		}
	}()
	p := br.NewProducer("q")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish(i)
	}
}
