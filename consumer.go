package rabbitmqbackoff

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

// UpdatesFn message processing func. This function invokes message processing worker.
type UpdatesFn func(ctx context.Context, d amqp.Delivery) error

// Consumer responsible for declaring queues and exchanges for backoff logic.
type Consumer struct {
	channel *amqp.Channel
	queues  []string
}

// NewConsumer constructor for consumer, also declares main and backoff queues.
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

// Consume start consuming messages from main and backoff queues.
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

// runWorkers run workers for processing messages, each worker execute updatesFn func.
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

// runConsumers run goroutines for dispatch messages for each consumable queues.
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

// consumeMessages dispatch messages from channel and forward to msgChan for further worker processing.
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

	if err := queue.DeclareQueue(channel, WithDLX(currentDLX), WithTTL(0)); err != nil {
		return nil, fmt.Errorf("declare main queue: %w", err)
	}

	queueNames = append(queueNames, queue.Name)

	for _, interval := range backoffIntervals {
		var (
			backoffQueueName = makeBackoffQueueName(queue.Name, interval)
			backoffDLX       = makeDLXName(backoffQueueName)
			backoffQueue     = copyQueueParams(backoffQueueName, queue)
		)

		if err := backoffQueue.DeclareQueue(
			channel,
			WithDLX(backoffDLX),
			WithTTL(interval),
			WithBind(currentDLX),
		); err != nil {
			return nil, fmt.Errorf("declare and bind queue: %w", err)
		}

		var (
			consumableBackoffQueue = makeConsumableBackoffQueueName(queue.Name, interval)
			consumableBackoffDLX   = makeDLXName(consumableBackoffQueue)
			consumableQueue        = copyQueueParams(consumableBackoffQueue, queue)
		)

		if err := consumableQueue.DeclareQueue(
			channel,
			WithDLX(consumableBackoffDLX),
			WithBind(backoffDLX),
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

// makeDLXName creates deadletter queue name based on original queue name.
func makeDLXName(queueName string) string {
	return fmt.Sprintf("%s_dlx", queueName)
}

// makeBackoffQueueName creates backoff queue name based on original queue name and backoff interval.
func makeBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d", queueName, backoffInterval)
}

// makeConsumableBackoffQueueName creates consumable queue name based on origin queue and backoff interval.
func makeConsumableBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d_consume", queueName, backoffInterval)
}
