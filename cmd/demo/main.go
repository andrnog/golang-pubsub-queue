package main

import (
	"fmt"
	"sync"

	pubsubqueue "github.com/andrnog/golang-pubsub-queue"
)

func main() {
	broker := pubsubqueue.NewBroker()
	defer broker.Close()

	fmt.Println("1. Базовая публикация и чтение")

	q := broker.Queue("события")
	consumer1 := q.Subscribe()
	consumer2 := q.Subscribe()

	fmt.Printf("   Подписчиков: %d\n", q.ConsumerCount())

	producer := broker.NewProducer("события")
	producer.Publish(10)
	producer.Publish(20)
	producer.Publish(30)

	var wg sync.WaitGroup
	readN := func(name string, c pubsubqueue.Consumer, n int) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m := <-c.Messages()
			fmt.Printf("   [%s] получил: id=%d value=%d\n", name, m.ID, m.Value)
		}
	}

	wg.Add(2)
	go readN("consumer-1", consumer1, 3)
	go readN("consumer-2", consumer2, 3)
	wg.Wait()
	fmt.Println()

	fmt.Println("2. Фильтры: чётные числа в диапазоне [10, 50]")

	filteredProducer := broker.NewProducer("фильтры").
		WithEvenFilter().
		WithMinMaxFilter(10, 50)

	fc := broker.Queue("фильтры").Subscribe()

	values := []int{3, 10, 15, 22, 50, 55, 100}
	for _, v := range values {
		err := filteredProducer.Publish(v)
		if err != nil {
			fmt.Printf("   Publish(%d) отклонён: %v\n", v, err)
		}
	}

	for i := 0; i < 3; i++ {
		m := <-fc.Messages()
		fmt.Printf("   Прошло фильтр: value=%d\n", m.Value)
	}
	fc.Close()
	fmt.Println()

	fmt.Println("3. Подтверждение сообщений (Ack) и список необработанных (Pending)")

	ackQ := broker.Queue("ack-demo")
	ackConsumer := ackQ.Subscribe()
	ackProducer := broker.NewProducer("ack-demo")

	ackProducer.Publish(100)
	ackProducer.Publish(200)
	ackProducer.Publish(300)

	m1 := <-ackConsumer.Messages()
	m2 := <-ackConsumer.Messages()
	_ = <-ackConsumer.Messages()

	fmt.Printf("   Pending до Ack: %d сообщений\n", len(ackConsumer.Pending()))

	ackConsumer.Ack(m1.ID)
	ackConsumer.Ack(m2.ID)

	pending := ackConsumer.Pending()
	fmt.Printf("   Pending после 2x Ack: %d сообщение(й)\n", len(pending))
	for _, p := range pending {
		fmt.Printf("   → необработанное: id=%d value=%d\n", p.ID, p.Value)
	}

	err := ackConsumer.Ack(999)
	fmt.Printf("   Ack(999): %v\n", err)
	ackConsumer.Close()
	fmt.Println()

	fmt.Println("4. Изоляция Ack: подтверждение у одного не влияет на другого")

	isoQ := broker.Queue("изоляция")
	iso1 := isoQ.Subscribe()
	iso2 := isoQ.Subscribe()
	broker.NewProducer("изоляция").Publish(42)

	msg1 := <-iso1.Messages()
	<-iso2.Messages()

	iso1.Ack(msg1.ID)
	fmt.Printf("   consumer-1 Pending: %d (после Ack)\n", len(iso1.Pending()))
	fmt.Printf("   consumer-2 Pending: %d (не тронут)\n", len(iso2.Pending()))

	iso1.Close()
	iso2.Close()
	fmt.Println()

	fmt.Println("5. Неблокирующая публикация: переполнение буфера")

	overP := broker.NewProducer("переполнение")
	dropped := 0

	for i := 0; i < 200; i++ {
		if err := overP.Publish(i); err != nil {
			dropped++
		}
	}
	fmt.Printf("   Отправлено: 200, дропнуто: %d (Publish никогда не блокирует)\n", dropped)
	fmt.Println()

	fmt.Println("=== End ===")
}
