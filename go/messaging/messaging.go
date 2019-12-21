package messaging // import "DM874-jolie-exec/messaging"

import (
	"bytes" /*Message structure*/
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

var listedBrokers []string
var gcsBucketName string
var execSemaphore *semaphore.Weighted
var newMsgTopic string

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

/*Message TODO use new EventSourcingStruct*/
type Message struct {
	DestinationID int
	MessageID     string
	MessageText   string
}

/*The EventSourcingStruct describes the format of input and output*/
type EventSourcingStruct struct {
	MessageUID        string   `json:"messageUid"`
	SessionUID        string   `json:"sessionUid"`
	FromAutoReply     bool     `json:"fromAutoReply"`
	MessageBody       string   `json:"messageBody"`
	SenderID          int      `json:"senderId"`
	RecipientIDs      []int    `json:"recipientIds"`
	EventDestinations []string `json:"eventDestinations"`
}

func parseEventSourcingStructure(jsonBytes []byte) (*EventSourcingStruct, error) {
	type InputEventSourcingStruct struct {
		MessageUID        *string   `json:"messageUid"`
		SessionUID        *string   `json:"sessionUid"`
		FromAutoReply     *bool     `json:"fromAutoReply"`
		MessageBody       *string   `json:"messageBody"`
		SenderID          *int      `json:"senderId"`
		RecipientIDs      *[]int    `json:"recipientIds"`
		EventDestinations *[]string `json:"eventDestinations"`
	}

	jsonDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	jsonDecoder.DisallowUnknownFields()

	var decoded InputEventSourcingStruct
	var err error
	err = jsonDecoder.Decode(&decoded)
	if err != nil {
		return nil, err
	}

	if (decoded.MessageUID == nil) ||
		(decoded.SessionUID == nil) ||
		(decoded.FromAutoReply == nil) ||
		(decoded.MessageBody == nil) ||
		(decoded.SenderID == nil) ||
		(decoded.RecipientIDs == nil) ||
		(decoded.EventDestinations == nil) {
		err = errors.New("a required key was not found")
		return nil, err
	}

	result := new(EventSourcingStruct)
	result.MessageUID = *decoded.MessageUID
	result.SessionUID = *decoded.SessionUID
	result.FromAutoReply = *decoded.FromAutoReply
	result.MessageBody = *decoded.MessageBody
	result.SenderID = *decoded.SenderID
	result.RecipientIDs = *decoded.RecipientIDs
	result.EventDestinations = *decoded.EventDestinations

	return result, nil
}

func essToJSONString(ess *EventSourcingStruct) ([]byte, error) {
	jsonBytes, err := json.Marshal(*ess)
	if err != nil {
		fmt.Println("error encoding EventSourcingStruct as JSON")
	}
	return jsonBytes, err
}

/*
* Runs user program on message.
* If an error occurs, original message must be forwarded as is
 */
func getAndRunUserProgram(userID int, msgDirection string, ess *EventSourcingStruct) {
	// Get user program
	// GCS code
	client, err := storage.NewClient(context.Background())
	if err != nil {
		if userID == ess.SenderID {
			forwardMessageToMultiple(ess, ess.RecipientIDs)
		} else {
			forwardMessageToSingleUser(ess, userID)
		}
		return
	}

	bucket := client.Bucket(gcsBucketName)
	fmt.Println(bucket) // TODO
}

func forwardMessageToMultiple(ess *EventSourcingStruct, recipientIDs []int) {
	newESS := new(EventSourcingStruct)
	newESS.MessageUID = ess.MessageUID
	newESS.SessionUID = ess.SessionUID
	newESS.MessageBody = ess.MessageBody
	newESS.SenderID = ess.SenderID
	newESS.FromAutoReply = ess.FromAutoReply
	newESS.EventDestinations = ess.EventDestinations

	dispatchMessage(newESS)
}

func forwardMessageToSingleUser(originalESS *EventSourcingStruct, userID int) {
	newESS := new(EventSourcingStruct)
	newESS.RecipientIDs = []int{userID}
}

func userHasProgram(userID int, msgDirection string, db *sql.DB) (bool, error) {
	var validTargets = []string{"send", "recv"}
	if !stringInSlice(msgDirection, validTargets) {
		return false, errors.New("invalid message direction")
	}

	queryString := "SELECT * FROM jolie_exec WHERE user_id=$1 AND direction=$2;"
	row := db.QueryRow(queryString, userID, msgDirection)
	if row == nil {
		return false, nil
	}

	var user int
	var direction string

	err := row.Scan(&user, &direction)
	if err != sql.ErrNoRows {

	}

	if !stringInSlice(direction, validTargets) {
		return false, errors.New("read invalid direction from database")
	}

	return true, nil
}

func newMessage() {

}

func dispatchMessage(ess *EventSourcingStruct) {
	var topic string
	topic, ess.EventDestinations = ess.EventDestinations[1], ess.EventDestinations[2:]

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  listedBrokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	fmt.Println(writer)

}

/*MessageService continuoisly reads and handles configuration messages from kafka*/
func MessageService(reader *kafka.Reader, db *sql.DB, bucketName string, brokers []string) {
	ctx := context.Background()
	execSemaphore = semaphore.NewWeighted(8)
	gcsBucketName = bucketName
	listedBrokers = brokers

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

		// Check if sender set up jolie script for outgoing messages

		// Replace message with script output if script was present

		// Check if reciever set up jolie script for incoming messages
		var forwardToNoChange []int = make([]int, len(eventSourcingStructure.RecipientIDs))
		for _, recipient := range eventSourcingStructure.RecipientIDs {
			hasProgram, err := userHasProgram(recipient, "recv", db)
			if err != nil || !hasProgram {
				forwardToNoChange = append(forwardToNoChange)
				continue
			} else {
				// User has program, run in new goroutine
				go getAndRunUserProgram(recipient, "recv", eventSourcingStructure)
			}

		}
	}
}
