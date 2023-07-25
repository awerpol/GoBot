package main

import (
	"log"
	// "time"

	tgClient "bot_india/clients/telegram"
	"bot_india/config"
	"bot_india/consumer/event-consumer"
	"bot_india/events/telegram"
	"bot_india/storage/files"
	// "bot_india/storage/mongo"
)

const (
	tgBotHost   = "api.telegram.org"
	storagePath = "files_storage"
	batchSize   = 100
)

func main() {
	cfg := config.MustLoad()

	storage := files.New(storagePath)
	// storage := mongo.New(cfg.MongoConnectionString, 10*time.Second)

	eventsProcessor := telegram.New(
		tgClient.New(tgBotHost, cfg.TgBotToken),
		storage,
	)

	log.Print("service started")

	consumer := event_consumer.New(eventsProcessor, eventsProcessor, batchSize)

	if err := consumer.Start(); err != nil {
		log.Fatal("service is stopped", err)
	}
}
