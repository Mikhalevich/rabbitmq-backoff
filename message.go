package rabbitmqbackoff

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents single rabbitmq delivery info.
type Message struct {
	delivery    amqp.Delivery
	isProcessed bool
}

// Payload returns message payload.
func (m *Message) Payload() []byte {
	return m.delivery.Body
}

// Ack send an acknowledgement for that message for finishing work on a delivery.
func (m *Message) Ack() error {
	if err := m.delivery.Ack(false); err != nil {
		return fmt.Errorf("delivery ack: %w", err)
	}

	m.isProcessed = true

	return nil
}

// Requeue send message to next backoff queue with some waiting interval.
func (m *Message) Requeue() error {
	if err := m.delivery.Nack(false, false); err != nil {
		return fmt.Errorf("delivery nack: %w", err)
	}

	m.isProcessed = true

	return nil
}
