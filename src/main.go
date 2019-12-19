package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

/*Message structure*/
type Message struct {
	DestinationID int
	MessageID     string
	MessageText   string
}

/*The EventSourcingStruct describes the format of input and output*/
type EventSourcingStruct struct {
	MessageID           string
	SessionID           string
	SenderID            int
	FromAutoReply       bool
	MessageDestinations []*Message
	EventDestinations   map[string]string
}

func parseEventSourcingStructure(jsonBytes []byte) (*EventSourcingStruct, error) {

	type InputMessage struct {
		DestiantionID *int    `json:"destinationId"`
		MessageID     *string `json:"messageId"`
		MessageText   *string `json:"message"`
	}

	type InputEventSourcingStruct struct {
		MessageID           *string            `json:"messageId"`
		SessionID           *string            `json:"sessionId"`
		SenderID            *int               `json:"senderId"`
		FromAutoReply       *bool              `json:"fromAutoReply"`
		MessageDestinations *[]InputMessage    `json:"messageDestinations"`
		EventDestinations   *map[string]string `json:"eventDestinations"`
	}

	jsonDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	jsonDecoder.DisallowUnknownFields()

	var decoded InputEventSourcingStruct
	var messages []InputMessage
	var err error
	err = jsonDecoder.Decode(&decoded)
	if err != nil {
		return nil, err
	}

	if (decoded.MessageID == nil) ||
		(decoded.SessionID == nil) ||
		(decoded.SenderID == nil) ||
		(decoded.FromAutoReply == nil) ||
		(decoded.MessageDestinations == nil) ||
		(decoded.EventDestinations == nil) {
		err = errors.New("a required key was not found")
		return nil, err
	}

	messages = *decoded.MessageDestinations

	for _, inputMessage := range messages {
		if (inputMessage.DestiantionID == nil) ||
			(inputMessage.MessageID == nil) ||
			(inputMessage.MessageText == nil) {
			err = errors.New("a required key was not found")
			return nil, err
		}
	}

	result := new(EventSourcingStruct)
	result.MessageID = *decoded.MessageID
	result.SessionID = *decoded.SessionID
	result.SenderID = *decoded.SenderID
	result.FromAutoReply = *decoded.FromAutoReply
	result.EventDestinations = *decoded.EventDestinations

	for _, inputMessage := range messages {
		var msg = new(Message)
		msg.DestinationID = *inputMessage.DestiantionID
		msg.MessageID = *inputMessage.MessageID
		msg.MessageText = *inputMessage.MessageText
		result.MessageDestinations = append(result.MessageDestinations, msg)
	}

	return result, nil
}

func composeEventSourcingStruct() {

}

func runJolie(c chan string) {

}

func toJolie(program string, ess *EventSourcingStruct) {

}

func dbConnect(host string, port string, user string, pwd string, dbname string) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%s user=%s "+
			"password=%s dbname=%s sslmode=disable",
		host, port, user, pwd, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, err
}

func configManager(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	for {
		reader.ReadMessage(ctx)

		time.Sleep(time.Millisecond * 50)
	}
}

func messageService(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	for {
		// Get a message
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
		}

		// Parse the json
		eventSourcingStructure, err := parseEventSourcingStructure(msg.Value)
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
		}
		fmt.Printf("Parsed event sourcing struct:\n%v\n", eventSourcingStructure)

		// Check if reciever set up jolie script
		var receivers []int = make([]int, len(eventSourcingStructure.MessageDestinations))
		for _, destination := range eventSourcingStructure.MessageDestinations {
			receivers = append(receivers, destination.DestinationID)
		}

	}

}

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

	db, err := dbConnect(dbHost, dbPort, dbUser, dbPassword, dbName)
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

	go configManager(controlReader, db)

	messageService(messageReader, db)

}
