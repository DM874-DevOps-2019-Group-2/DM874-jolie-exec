package messaging // import "DM874-jolie-exec/messaging"

import (
	"bytes" /*Message structure*/
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"

	"cloud.google.com/go/storage"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/iterator"
)

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

func gcsInit(projectID string, bucketName string) (*storage.Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	var buckets []string

	it := client.Buckets(ctx, projectID)
	for {
		battrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, battrs.Name)
	}

	if !stringInSlice(bucketName, buckets) {
		// TODO Create bucket
	}

	return nil, nil
}

/*The EventSourcingStruct describes the format of input and output*/
type EventSourcingStruct struct {
	MessageUID        string
	SessionUID        string
	MessageBody       string
	SenderID          int
	FromAutoReply     bool
	RecipientIDs      []int
	EventDestinations []string
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
	result.SenderID = *decoded.SenderID
	result.FromAutoReply = *decoded.FromAutoReply
	result.EventDestinations = *decoded.EventDestinations

	return result, nil
}

func essToJSONString(*EventSourcingStruct) []byte {
	return make([]byte, 0)
}

func getProgram(userID int, direction string) {
	// GCS stuff
}

func runJolie(sem *semaphore.Weighted, msg *EventSourcingStruct) {
	sem.Acquire(context.TODO(), 1)

	msgJSON := essToJSONString(msg)
	fmt.Println(msgJSON)

	// jolieString := fmt.Sprintf("jolie %s %s", program, msgJSON)
	// fmt.Println(jolieString)

	// program := getProgram()

	// Run the code
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
func MessageService(reader *kafka.Reader, db *sql.DB, gcsProjectID string, gcsBucketName string) {
	ctx := context.Background()
	// semaphore := semaphore.NewWeighted(8)

	client, err := storage.NewClient(ctx)
	if err != nil {
		// TODO: Handle error
	}
	fmt.Println(client)

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
		for _, recipient := range eventSourcingStructure.RecipientIDs {
			fmt.Println(recipient)
		}

	}

}
