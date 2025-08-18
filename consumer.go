package rabbitmqbackoff

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

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

func makeDLXName(queueName string) string {
	return fmt.Sprintf("%s_dlx", queueName)
}

func makeBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d", queueName, backoffInterval)
}

func makeConsumableBackoffQueueName(queueName string, backoffInterval int64) string {
	return fmt.Sprintf("%s_%d_consume", queueName, backoffInterval)
}
