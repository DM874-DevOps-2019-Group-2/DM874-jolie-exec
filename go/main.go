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

func envWithPrint(env string) string {
	val := os.Getenv(env)
	fmt.Printf("%v: %v\n", env, val)
	return val
}

func main() {
	configTopic := envWithPrint("JOLIE_EXEC_CONFIG_TOPIC")
	inTopic := envWithPrint("JOLIE_EXEC_CONSUMER_TOPIC")
	newMessageOutTopic := envWithPrint("DEFAULT_PRODUCER_TOPIC")
	kafkaBrokers := envWithPrint("KAFKA_BROKERS")
	listedBrokers := strings.Split(kafkaBrokers, ",")


	gcsBucketName := envWithPrint("JOLIE_EXEC_GCS_BUCKET_NAME")

	dbHost := envWithPrint("DATABASE_HOST")
	dbPort := envWithPrint("DATABASE_PORT")
	dbUser := envWithPrint("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbName := envWithPrint("JOLIE_EXEC_DB_NAME")


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
