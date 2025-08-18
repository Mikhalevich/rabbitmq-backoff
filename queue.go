package rabbitmqbackoff

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type queueOptions struct {
	DLX                 string
	TTL                 int64
	ExchangeToSubscribe string
}

type Option func(opts *queueOptions)

func WithDLX(dlx string) Option {
	return func(opts *queueOptions) {
		opts.DLX = dlx
	}
}

func WithTTL(ttl int64) Option {
	return func(opts *queueOptions) {
		opts.TTL = ttl
	}
}

func WithBind(exchange string) Option {
	return func(opts *queueOptions) {
		opts.ExchangeToSubscribe = exchange
	}
}

func (q *Queue) DeclareQueue(
	channel *amqp.Channel,
	opts ...Option,
) error {
	var options queueOptions

	for _, opt := range opts {
		opt(&options)
	}

	if options.DLX != "" {
		if err := channel.ExchangeDeclare(
			options.DLX,
			"fanout",
			q.Durable,
			q.AutoDelete,
			false,
			q.NoWait,
			nil,
		); err != nil {
			return fmt.Errorf("exchange declare %s: %w", options.DLX, err)
		}

		q.addArg("x-dead-letter-exchange", options.DLX)
	}

	if options.TTL > 0 {
		q.addArg("x-message-ttl", options.TTL)
	}

	if _, err := channel.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		q.NoWait,
		q.Args,
	); err != nil {
		return fmt.Errorf("queue declare %s: %w", q.Name, err)
	}

	if options.ExchangeToSubscribe != "" {
		if err := channel.QueueBind(q.Name, "", options.ExchangeToSubscribe, false, nil); err != nil {
			return fmt.Errorf("bind queue %s to exchange %s: %w", q.Name, options.ExchangeToSubscribe, err)
		}
	}

	return nil
}

func (q *Queue) addArg(key string, value any) {
	if q.Args == nil {
		q.Args = make(amqp.Table)
	}

	q.Args[key] = value
}
