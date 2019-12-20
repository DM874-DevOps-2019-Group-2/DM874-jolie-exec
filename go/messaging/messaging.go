package messaging // import "DM874-jolie-exec/messaging"

import (
	"bytes" /*Message structure*/
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

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

func essToJSONString(*EventSourcingStruct) []byte {
	return make([]byte, 0)
}

func runJolie(sem *semaphore.Weighted, msg *EventSourcingStruct, program string) {
	sem.Acquire(context.TODO(), 1)

	msgJSON := essToJSONString(msg)
	fmt.Println(msgJSON)

	jolieString := fmt.Sprintf("jolie %s %s", program, msgJSON)
	fmt.Println(jolieString)

	runnnable := exec.Command("pwd")
	err := runnnable.Start()
	if err != nil {
		panic(errors.New("failed to start command"))
	}

	err = runnnable.Wait()
	if err != nil {
		panic(errors.New("error occured while waiting for command to finish"))
	}
}

func toJolie(program string, ess *EventSourcingStruct) {

}

func composeEventSourcingStruct() {

}

/*MessageService continuoisly reads and handles configuration messages from kafka*/
func MessageService(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	semaphore := semaphore.NewWeighted(8)

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

		// TODO: execute only if db tells us to.
		runJolie(semaphore, eventSourcingStructure, "http://dummy.url.com/")

	}

}
