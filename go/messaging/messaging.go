package messaging // import "DM874-jolie-exec/messaging"

import (
	"bytes" /*Message structure*/
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

var gcsContext context.Context
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

type outboundMsgJolieIn struct {
	MessageBody  string `json:"messageBody"`
	OwnID        int    `json:"ownID"`
	RecipientIDs []int  `json:"recipientIDs"`
}

type outboundMsgJolieOut struct {
	MessageBody string `json:"messageBody"`
}

type inboundMsgJolieIn struct {
	MessageBody  string `json:"messageBody"`
	OwnID        int    `json:"ownID"`
	SenderID     int    `json:"senderID"`
	RecipientIDs []int  `json:"recipientIDs"`
}

type simpleMessage struct {
	To      int    `json:"to"`
	Message string `json:"message"`
}

type inboundMsgJolieOut struct {
	Action string          `json:"action"`
	Reply  []simpleMessage `json:"reply"`
}

// TODO?
func parseJolieOutputIncoming(output []byte) {

}

func parseJolieOutputOutgoing() {

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
	// If we cannot access GCS, we simply forward the messages as if no program was set.
	client, err := storage.NewClient(context.Background())
	if err != nil {
		forwardHelper(ess, userID, msgDirection)
		return
	}

	validDirections := []string{"send", "recv"}
	if !stringInSlice(msgDirection, validDirections) { // Fail gracefully
		forwardHelper(ess, userID, msgDirection)
		return
	}

	// Save the file
	bucket := client.Bucket(gcsBucketName)
	filename := fmt.Sprintf("%d-%s.ol", userID, msgDirection)
	rc, err := bucket.Object(filename).NewReader(context.Background())
	if err != nil {
		forwardHelper(ess, userID, msgDirection)
		return
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		forwardHelper(ess, userID, msgDirection)
		return
	}
	absolutePath := fmt.Sprintf("/tmp/%s", filename)
	err = ioutil.WriteFile(absolutePath, data, 644)
	if err != nil {
		forwardHelper(ess, userID, msgDirection)
		return
	}

	// The file is now downloaded, time to prepare the eventSourceStruct & actually run the code
	runJolie(absolutePath, msgDirection, ess)
}

func runJolie(pathToFile string, msgDirection string, eventSourceStruct *EventSourcingStruct) {
	// Prepare structs
	switch msgDirection {
	case "send":
		break
	case "recv":
		break
	}
	execSemaphore.Acquire(context.Background(), 1)
	// Do the actual running of user code here, to avoid too much memory usage (JVM LUL)

	execSemaphore.Release(1)

	// Parse output

}

func forwardHelper(ess *EventSourcingStruct, userID int, msgDirection string) {
	if userID == ess.SenderID {
		forwardMessageToMultiple(ess, ess.RecipientIDs)
	} else {
		forwardMessageToSingleUser(ess, userID)
	}
}

func forwardMessageToMultiple(ess *EventSourcingStruct, recipientIDs []int) {
	newESS := new(EventSourcingStruct)
	newESS.MessageUID = ess.MessageUID
	newESS.SessionUID = ess.SessionUID
	newESS.MessageBody = ess.MessageBody
	newESS.SenderID = ess.SenderID
	newESS.FromAutoReply = ess.FromAutoReply
	newESS.EventDestinations = ess.EventDestinations
	newESS.RecipientIDs = recipientIDs

	dispatchMessage(newESS)
}

func forwardMessageToSingleUser(ess *EventSourcingStruct, userID int) {
	newESS := new(EventSourcingStruct)
	newESS.MessageUID = ess.MessageUID
	newESS.SessionUID = ess.SessionUID
	newESS.MessageBody = ess.MessageBody
	newESS.SenderID = ess.SenderID
	newESS.FromAutoReply = ess.FromAutoReply
	newESS.EventDestinations = ess.EventDestinations
	newESS.RecipientIDs = []int{userID}
}

func userHasProgram(userID int, msgDirection string, db *sql.DB) (bool, error) {
	var validTargets = []string{"send", "recv"}
	if !stringInSlice(msgDirection, validTargets) {
		return false, errors.New("invalid message direction")
	}

	// Check if entry is in database
	queryString := "SELECT * FROM jolie_exec WHERE user_id=$1 AND direction=$2;"
	row := db.QueryRow(queryString, userID, msgDirection)
	if row == nil {
		// Shouldn't happen, but if it does, we continue running according to degradation of service
		return false, nil
	}

	var user int
	var direction string

	// Scan the query result to check if user registered a program
	err := row.Scan(&user, &direction)
	if err != sql.ErrNoRows {
		// Some error occured different from the expected
		return false, err
	} else if err == sql.ErrNoRows {
		// If no row was returned, the user has no script set for given message direction
		return false, nil
	}

	// Check that what we read is expected
	if !stringInSlice(direction, validTargets) {
		return false, errors.New("read invalid direction from database")
	}

	// If nothing failed yet, we are know that the user has a script registered for gived msg direction
	return true, nil
}

/*
* TODO: below is not quite right, instead, write functions for newReply, and modifyBody.
*       Those functions should closer represent what is actually allowed through user programs.
*       `newReply` should parse the reply list and send as few different kafka messages as viable.
*       `modifyBody` should send a single kafka message, with the modified message body of a sender's msg
*
 */
func forkMessage(ess *EventSourcingStruct, direction string) (*EventSourcingStruct, error) {
	switch direction {
	case "send":
		return newMessage(ess.SessionUID, false, ess.MessageBody, ess.SenderID, ess.RecipientIDs)
	case "recv":
		return newMessage(ess.SessionUID, true, ess.MessageBody, ess.SenderID, ess.RecipientIDs)
	default:
		// best-effort
		fmt.Println("[ warn ] could not determine message goal, marking as auto-reply message")
		return newMessage(ess.SessionUID, true, ess.MessageBody, ess.SenderID, ess.RecipientIDs)
	}
}

func newReply() {

}

func modifyBody() {

}

func newMessage(sessionID string,
	shouldSetAutoReply bool,
	text string,
	senderID int,
	recipientList []int) (*EventSourcingStruct, error) {

	var msg *EventSourcingStruct = new(EventSourcingStruct)
	newUUID, err := uuid.NewRandom()
	if err != nil {
		fmt.Println(" [ fatal ] failed to generate UUID for new message, discarding")
		return nil, errors.New("failed to generate UUID for new message, discarding")
	}

	msg.MessageUID = newUUID.String()
	msg.SessionUID = sessionID
	msg.FromAutoReply = shouldSetAutoReply
	msg.MessageBody = text
	msg.SenderID = senderID
	msg.RecipientIDs = recipientList
	msg.EventDestinations = []string{"", newMsgTopic}

	return msg, nil
}

/* Write the Event sourcing struct to the appropiate kafka topic (second element in EventDestinations) */
func dispatchMessage(ess *EventSourcingStruct) {
	// Get the target topic, and remove self from EventDestinations, without removing the new target
	// This is consistent with the discussed semantic where each consumer is responsible for removing itself
	// from the EventDestinations routing list/chain.
	var topic string
	topic, ess.EventDestinations = ess.EventDestinations[1], ess.EventDestinations[1:]

	// Create a new kafka writer for the topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  listedBrokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	fmt.Println(writer)

}

/*MessageService continuoisly reads and handles configuration messages from kafka*/
func MessageService(reader *kafka.Reader, db *sql.DB, bucketName string, brokers []string, newMessageOutTopic string) {
	gcsContext = context.Background()
	execSemaphore = semaphore.NewWeighted(8)
	gcsBucketName = bucketName
	listedBrokers = brokers
	newMsgTopic = newMessageOutTopic

	for {
		// Get a message
		msg, err := reader.ReadMessage(gcsContext)
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
