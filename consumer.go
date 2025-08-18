package rabbitmqbackoff

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type UpdatesFn func(ctx context.Context, d amqp.Delivery) error

type Consumer struct {
	channel *amqp.Channel
	queues  []string
}

func NewConsumer(
	channel *amqp.Channel,
	queue Queue,
	backoffIntervals []int64,
) (*Consumer, error) {
	queues, err := declareQueues(channel, queue, backoffIntervals)
	if err != nil {
		return nil, fmt.Errorf("declare queues: %w", err)
	}

	return &Consumer{
		channel: channel,
		queues:  queues,
	}, nil
}

func (c *Consumer) Consume(
	ctx context.Context,
	workersCount int,
	updatesFn UpdatesFn,
) error {
	msgChan := make(chan amqp.Delivery)

	wgConsume, ctx := c.runConsumers(ctx, msgChan)

	wgWorkers := runWorkers(ctx, workersCount, msgChan, updatesFn)

	consumeErr := wgConsume.Wait()

	close(msgChan)

	wgWorkers.Wait()

	if consumeErr != nil {
		return fmt.Errorf("run consumers: %w", consumeErr)
	}

	return nil
}

func runWorkers(
	ctx context.Context,
	count int,
	msgChan chan amqp.Delivery,
	updatesFn UpdatesFn,
) *sync.WaitGroup {
	var wgWorker sync.WaitGroup

	for range count {
		wgWorker.Add(1)

		go func() {
			defer wgWorker.Done()

			for msg := range msgChan {
				//nolint:errcheck
				updatesFn(ctx, msg)
			}
		}()
	}

	return &wgWorker
}

func (c *Consumer) runConsumers(
	ctx context.Context,
	msgChan chan amqp.Delivery,
) (*errgroup.Group, context.Context) {
	wgConsume, ctx := errgroup.WithContext(ctx)

	for _, queue := range c.queues {
		wgConsume.Go(func() error {
			return c.consumeMessages(ctx, queue, msgChan)
		})
	}

	return wgConsume, ctx
}

func (c *Consumer) consumeMessages(
	ctx context.Context,
	queue string,
	msgChan chan amqp.Delivery,
) error {
	msgs, err := c.channel.ConsumeWithContext(ctx, queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume messages: %w", err)
	}

	for msg := range msgs {
		msgChan <- msg
	}

	return nil
}

// declareQueues declare main and backoff queues
// resturns slace of declared queues.
func declareQueues(channel *amqp.Channel, queue Queue, backoffIntervals []int64) ([]string, error) {
	var (
		queueNames = make([]string, 0, len(backoffIntervals)+1)
		currentDLX = makeDLXName(queue.Name)
	)

	if err := declareQueueWithTTLAndDLX(channel, queue, currentDLX, 0); err != nil {
		return nil, fmt.Errorf("declare main queue: %w", err)
	}

	queueNames = append(queueNames, queue.Name)

	for _, interval := range backoffIntervals {
		var (
			backoffQueue = makeBackoffQueueName(queue.Name, interval)
			backoffDLX   = makeDLXName(backoffQueue)
		)

		if err := declareAndBindQueue(
			channel,
			copyQueueParams(backoffQueue, queue),
			backoffDLX,
			interval,
			currentDLX,
		); err != nil {
			return nil, fmt.Errorf("declare and bind queue: %w", err)
		}

		var (
			consumableBackoffQueue = makeConsumableBackoffQueueName(queue.Name, interval)
			consumableBackoffDLX   = makeDLXName(consumableBackoffQueue)
		)

		if err := declareAndBindQueue(
			channel,
			copyQueueParams(consumableBackoffQueue, queue),
			consumableBackoffDLX,
			0,
			backoffDLX,
		); err != nil {
			return nil, fmt.Errorf("declare and bind queue: %w", err)
		}

		currentDLX = consumableBackoffDLX

		queueNames = append(queueNames, consumableBackoffQueue)
	}

	return queueNames, nil
}

func copyQueueParams(name string, queue Queue) Queue {
	return Queue{
		Name:       name,
		Durable:    queue.Durable,
		AutoDelete: queue.AutoDelete,
		Exclusive:  queue.Exclusive,
		NoWait:     queue.NoWait,
	}
}

func declareAndBindQueue(
	channel *amqp.Channel,
	queue Queue,
	backoffDLX string,
	backoffInterval int64,
	exchangeToSubscribe string,
) error {
	if err := declareQueueWithTTLAndDLX(
		channel,
		queue,
		backoffDLX,
		backoffInterval,
	); err != nil {
		return fmt.Errorf("create backoff queue: %w", err)
	}

	if err := channel.QueueBind(queue.Name, "", exchangeToSubscribe, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", queue.Name, err)
	}

	return nil
}

func makeDLXName(queueName string) string {
	return fmt.Sprintf("%s_dlx", queueName)
}

func makeBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d", queueName, backoffInterval)
}

func makeConsumableBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d_consume", queueName, backoffInterval)
}

func declareQueueWithTTLAndDLX(channel *amqp.Channel, queue Queue, dlx string, ttl int64) error {
	if err := channel.ExchangeDeclare(
		dlx,
		"fanout",
		queue.Durable,
		queue.AutoDelete,
		false,
		queue.NoWait,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare %s: %w", dlx, err)
	}

	if queue.Args == nil {
		queue.Args = make(amqp.Table)
	}

	if dlx != "" {
		queue.Args["x-dead-letter-exchange"] = dlx
	}

	if ttl > 0 {
		queue.Args["x-message-ttl"] = ttl
	}

	if _, err := channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		queue.NoWait,
		queue.Args,
	); err != nil {
		return fmt.Errorf("queue declare %s: %w", queue.Name, err)
	}

	return nil
}
