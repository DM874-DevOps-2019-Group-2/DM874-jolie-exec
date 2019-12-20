package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	//"./control"
	"DM874-jolie-exec/control"
	//"./messaging"
	"DM874-jolie-exec/messaging"
	// "./database"
	"DM874-jolie-exec/database"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

const (
	gceGet = "http://gce.link/recv/userID"
)


func main() {
	configTopic := os.Getenv("JOLIE_EXEC_CONFIG_TOPIC")
	inTopic := os.Getenv("JOLIE_EXEC_CONSUMER_TOPIC")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	listedBrokers := strings.Split(kafkaBrokers, ",")

	dbHost := os.Getenv("DATABASE_HOST")
	dbPort := os.Getenv("DATABASE_PORT")
	dbUser := os.Getenv("DATABASE_USER")
	dbPassword := os.Getenv("DATABASE_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")

	db, err := database.DBConnect(dbHost, dbPort, dbUser, dbPassword, dbName)
	if err != nil {
		fmt.Printf("[ERROR] %v", err)
		return
	}
	defer db.Close()

	messageReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     listedBrokers,
		Topic:       inTopic,
		MinBytes:    10 << 10, // 10KiB
		MaxBytes:    10 << 20, // 10 MiB
		MaxWait:     time.Millisecond * 100,
		GroupID:     "jolie_exec_consumer_group",
		StartOffset: kafka.LastOffset,
	})
	defer messageReader.Close()

	controlReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     listedBrokers,
		Topic:       configTopic,
		MinBytes:    10 << 10, // 10KiB
		MaxBytes:    10 << 20, // 10 MiB
		MaxWait:     time.Millisecond * 100,
		GroupID:     "jolie_exec_config_consumer_group", // Assumes all instances share same DB to be updated.
		StartOffset: kafka.LastOffset,
	})
	defer controlReader.Close()

	go control.ConfigManager(controlReader, db)

	messaging.MessageService(messageReader, db)

}