package event_consumer

import (
	"context"
	"log"
	// "sync"
	"time"

	"bot_india/events"
)

type Consumer struct {
	fetcher   events.Fetcher
	processor events.Processor
	batchSize int
}

const delay_seconds = 1

func New(fetcher events.Fetcher, processor events.Processor, batchSize int) Consumer {
	return Consumer{
		fetcher:   fetcher,
		processor: processor,
		batchSize: batchSize,
	}
}

func (c Consumer) Start() error {
	for {
		gotEvents, err := c.fetcher.Fetch(context.Background(), c.batchSize)
		if err != nil {
			log.Printf("[ERR] consumer: %s", err.Error())
			// добавить доп. попытки (while) с растущим интервалом времени между ними
			continue
		}

		if len(gotEvents) == 0 {
			time.Sleep(delay_seconds * time.Second)

			continue
		}

		if err := c.handleEvents(context.Background(), gotEvents); err != nil {
			log.Print(err)

			continue
		}
	}
}

func (c *Consumer) handleEvents(ctx context.Context, events []events.Event) error {
	// нужна параллельная обработка, см горутины и структуру
	// sync.WaitGroup{}

	for _, event := range events {
		log.Printf("got new event: %s", event.Text)

		if err := c.processor.Process(ctx, event); err != nil {
			log.Printf("can't handle event: %s", err.Error())

			continue
		}
	}

	return nil
}
