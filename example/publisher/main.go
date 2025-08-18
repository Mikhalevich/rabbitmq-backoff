package main

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
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

	err = channel.PublishWithContext(
		context.Background(),
		"test_exchange",
		"test_key",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("test message"),
		},
	)

	panicOnError(err, "failed to publish message")

	log.Println("published...")
}

func panicOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %v", msg, err)
	}
}
