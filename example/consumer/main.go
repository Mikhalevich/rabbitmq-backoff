package main

import (
	"context"
	"log"
	"time"

	rabbitmqbackoff "github.com/Mikhalevich/rabbitmq-backoff"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	workersCount = 3
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	panicOnError(err, "failed to dial rabbitmq")

	defer conn.Close()

	channel, err := conn.Channel()
	panicOnError(err, "failed to open channel")

	defer channel.Close()

	err = channel.ExchangeDeclare(
		"test_exchange",
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	panicOnError(err, "failed to declare exchange")

	consumer, err := rabbitmqbackoff.NewConsumer(
		channel,
		rabbitmqbackoff.Queue{
			Name: "test_queue",
		},
		[]int64{1000, 2000, 4000, 8000},
	)

	panicOnError(err, "failed to init consumer")

	err = channel.QueueBind(
		"test_queue",
		"test_key",
		"test_exchange",
		false,
		nil,
	)

	panicOnError(err, "failed to bind main queue")

	err = consumer.Consume(
		context.Background(), workersCount,
		func(ctx context.Context, msg *rabbitmqbackoff.Message) {
			log.Printf("received: %s time: %s", string(msg.Payload()), time.Now().Format(time.TimeOnly))

			if err := msg.Requeue(); err != nil {
				log.Printf("failed to requeue message: %v", err)
			}
		},
	)
	panicOnError(err, "failed to consume messages")
}

func panicOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %v", msg, err)
	}
}
