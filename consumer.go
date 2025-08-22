package rabbitmqbackoff

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

// UpdatesFn message processing func. This function invokes message processing worker.
type UpdatesFn func(ctx context.Context, msg *Message)

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
	queues, err := makeQueues(channel, queue, backoffIntervals)
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

			for delivery := range msgChan {
				msg := Message{
					delivery: delivery,
				}

				updatesFn(ctx, &msg)

				if !msg.isProcessed {
					//nolint:errcheck
					msg.Ack()
				}
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

type declareQueueInfo struct {
	Queue               Queue
	DLX                 string
	TTL                 int64
	ExchangeToSubscribe string
}

// declareQueuesByInfo declare queues by declareQueueInfo.
func declareQueuesByInfo(channel *amqp.Channel, queues []declareQueueInfo) error {
	for _, queue := range queues {
		if err := queue.Queue.DeclareQueue(
			channel,
			WithDLX(queue.DLX),
			WithTTL(queue.TTL),
			WithBind(queue.ExchangeToSubscribe),
		); err != nil {
			return fmt.Errorf("declare queue %s: %w", queue.Queue.Name, err)
		}
	}

	return nil
}

// consumableQueueNames returns queue names for consuming messages from it.
func consumableQueueNames(queues []declareQueueInfo) []string {
	consumableCount := 0

	for _, queue := range queues {
		if queue.TTL == 0 {
			consumableCount++
		}
	}

	if consumableCount == 0 {
		return nil
	}

	names := make([]string, 0, consumableCount)

	for _, queue := range queues {
		if queue.TTL == 0 {
			names = append(names, queue.Queue.Name)
		}
	}

	return names
}

// makeQueues declare main and backoff queues
// generates names for backoff queues based on backoffIntervals
// for each interval declaring two queues (backoff and for consuption)
// resturns slace of consumable declared queues.
func makeQueues(channel *amqp.Channel, queue Queue, backoffIntervals []int64) ([]string, error) {
	var (
		queueInfos = make([]declareQueueInfo, 0, len(backoffIntervals)*2+1)
		currentDLX = makeDLXName(queue.Name)
	)

	queueInfos = append(queueInfos, declareQueueInfo{
		Queue: queue,
		DLX:   currentDLX,
	})

	for _, interval := range backoffIntervals {
		var (
			backoffQueueName = makeBackoffQueueName(queue.Name, interval)
			backoffDLX       = makeDLXName(backoffQueueName)
			backoffQueue     = copyQueueParams(backoffQueueName, queue)
		)

		queueInfos = append(queueInfos, declareQueueInfo{
			Queue:               backoffQueue,
			DLX:                 backoffDLX,
			TTL:                 interval,
			ExchangeToSubscribe: currentDLX,
		})

		var (
			consumableBackoffQueue = makeConsumableBackoffQueueName(queue.Name, interval)
			consumableBackoffDLX   = makeDLXName(consumableBackoffQueue)
			consumableQueue        = copyQueueParams(consumableBackoffQueue, queue)
		)

		queueInfos = append(queueInfos, declareQueueInfo{
			Queue:               consumableQueue,
			DLX:                 consumableBackoffDLX,
			ExchangeToSubscribe: backoffDLX,
		})

		currentDLX = consumableBackoffDLX
	}

	if err := declareQueuesByInfo(channel, queueInfos); err != nil {
		return nil, fmt.Errorf("declare queues: %w", err)
	}

	return consumableQueueNames(queueInfos), nil
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
