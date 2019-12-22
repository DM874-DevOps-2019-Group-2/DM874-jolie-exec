package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	// "./control"
	"DM874-jolie-exec/control"
	// "./messaging"
	"DM874-jolie-exec/messaging"
	// "./database"
	"DM874-jolie-exec/database"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func warnEnv(env string) string {
	val := os.Getenv(env)
	if val == "" {
		fmt.Printf("[ warn ] empty environment variable: %v\n", env)
	}
	return val
}

func main() {
	configTopic := warnEnv("JOLIE_EXEC_CONFIG_TOPIC")
	inTopic := warnEnv("JOLIE_EXEC_CONSUMER_TOPIC")
	newMessageOutTopic := warnEnv("ROUTE_MESSAGE_TOPIC")
	kafkaBrokers := warnEnv("KAFKA_BROKERS")
	listedBrokers := strings.Split(kafkaBrokers, ",")


	gcsBucketName := warnEnv("JOLIE_EXEC_GCS_BUCKET_NAME")

	dbHost := warnEnv("DATABASE_HOST")
	dbPort := warnEnv("DATABASE_PORT")
	dbUser := warnEnv("POSTGRES_USER")
	dbPassword := warnEnv("POSTGRES_PASSWORD")
	dbName := warnEnv("JOLIE_EXEC_DB_NAME")


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
		MaxBytes:    10 << 20, // 10MiB
		MaxWait:     time.Millisecond * 100,
		GroupID:     "jolie_exec_consumer_group",
		StartOffset: kafka.LastOffset,
	})
	defer messageReader.Close()

	controlReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     listedBrokers,
		Topic:       configTopic,
		MinBytes:    10 << 10, // 10KiB
		MaxBytes:    10 << 20, // 10MiB
		MaxWait:     time.Millisecond * 100,
		GroupID:     "jolie_exec_config_consumer_group", // Assumes all instances share same DB to be updated.
		StartOffset: kafka.LastOffset,
	})
	defer controlReader.Close()

	go control.ConfigManager(controlReader, db)

	messaging.MessageService(messageReader, db, gcsBucketName, listedBrokers, newMessageOutTopic)

}
