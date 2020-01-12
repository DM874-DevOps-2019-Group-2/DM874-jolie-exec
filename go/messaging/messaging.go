package messaging // import "DM874-jolie-exec/messaging"

import (
	"bytes" /*Message structure*/
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os/exec"

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
	MessageUID        string   `json:"messageId"`
	SessionUID        string   `json:"sessionId"`
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
func parseJolieOutputIncoming(output []byte) *inboundMsgJolieOut {
	var result = new(inboundMsgJolieOut)
	err := json.Unmarshal(output, result)
	if err != nil {
		return nil
	}
	return result
}

func parseJolieOutputOutgoing(output []byte) *outboundMsgJolieOut {
	var result = new(outboundMsgJolieOut)
	err := json.Unmarshal(output, result)
	if err != nil {
		return nil
	}
	return result
}

func parseJolieInputIncoming(data *inboundMsgJolieIn) []byte {
	result, err := json.Marshal(*data)
	if err != nil {
		fmt.Println("[ error ] unable to encode message for jolie argument")
		return nil
	}

	return result
}

func parseJolieInputOutgoing(data *outboundMsgJolieIn) []byte {
	result, err := json.Marshal(*data)
	if err != nil {
		fmt.Println("[ error ] unable to encode message for jolie argument")
		return nil
	}

	return result
}

func parseEventSourcingStructure(jsonBytes []byte) (*EventSourcingStruct, error) {
	type InputEventSourcingStruct struct {
		MessageUID        *string   `json:"messageId"`
		SessionUID        *string   `json:"sessionId"`
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
func handleReciever(userID int, ess *EventSourcingStruct) {
	path, err := downloadUserProgram(userID, false)
	if err != nil {
		// Failed to download the program.
		// Act as if no program was expected.
	}

	// The file is now downloaded, time to prepare the eventSourceStruct & actually run the code
	runJolieReceiver(path, userID, ess)
}

/*
 * downloadUserProgram downloads and saves a users program to disk, then returns the path to the file.
 * Does not do any form of error recovery.
 */
func downloadUserProgram(userID int, sender bool) (string, error) {
	var msgDirection string
	if sender {
		msgDirection = "send"
	} else {
		msgDirection = "recv"
	}
	// Get user program
	// If we cannot access GCS, we simply forward the messages as if no program was set.
	client, err := storage.NewClient(gcsContext)
	if err != nil {
		return "", errors.New("unable to start google cloud storage client")
	}

	// Save the file
	bucket := client.Bucket(gcsBucketName)
	filename := fmt.Sprintf("%d-%s.ol", userID, msgDirection)
	rc, err := bucket.Object(filename).NewReader(gcsContext)
	if err != nil {
		return "", errors.New("error downloading user program")
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return "", errors.New("error reading user program")
	}
	absolutePath := fmt.Sprintf("/tmp/%s", filename)
	err = ioutil.WriteFile(absolutePath, data, 644)
	if err != nil {
		return "", errors.New("error writing user program to disk")
	}
	return absolutePath, nil
}

func execJolie(pathToFile string, argument []byte) ([]byte, error) {
	var subProcess *exec.Cmd
	subProcess = exec.Command("timeout", "--kill-after=15s", "10s", "ni", "jolie", pathToFile, string(argument))
	out, err := subProcess.Output()
	if err != nil {
		fmt.Printf("[ warn ] error running user script for user: %v\n", pathToFile)
		return nil, err
	}

	return out, nil
}

func runJolieSender(pathToFile string, ess *EventSourcingStruct) error {
	var jsonBytes []byte
	var arg = outboundMsgJolieIn{
		MessageBody:  ess.MessageBody,
		OwnID:        ess.SenderID,
		RecipientIDs: ess.RecipientIDs,
	}
	jsonBytes = parseJolieInputOutgoing(&arg)
	if jsonBytes == nil {
		return errors.New("unable to generate input for user program")
	}

	execSemaphore.Acquire(context.Background(), 1)
	// Do the actual running of user code here, to avoid too much memory usage (JVM LUL)
	output, err := execJolie(pathToFile, jsonBytes)
	execSemaphore.Release(1)
	if err != nil {
		// Do nothing
		return err
	}

	//Parse output
	parsedMessage := parseJolieOutputOutgoing(output)
	if parsedMessage == nil {
		// Do nothing
		return err
	}

	// Replace message body as we are sender
	ess.MessageBody = parsedMessage.MessageBody

	return nil
}

/*
 * In contrast to runJolieSender, runJolieReceiver must create new messages, but never modify the original.
 */
func runJolieReceiver(pathToFile string, userID int, ess *EventSourcingStruct) {
	// Prepare structs
	var jsonBytes []byte
	var arg = inboundMsgJolieIn{
		MessageBody:  ess.MessageBody,
		SenderID:     ess.SenderID,
		OwnID:        userID,
		RecipientIDs: ess.RecipientIDs,
	}
	jsonBytes = parseJolieInputIncoming(&arg)
	if jsonBytes == nil {
		localESS := copyMessageForSingleUser(ess, userID)
		dispatchMessage(localESS)
		return
		// TODO: forward message
	}

	execSemaphore.Acquire(context.Background(), 1)
	// Do the actual running of user code here, to avoid too much memory usage (JVM LUL)
	output, err := execJolie(pathToFile, jsonBytes)
	execSemaphore.Release(1)
	if err != nil {
		localESS := copyMessageForSingleUser(ess, userID)
		dispatchMessage(localESS)
		// TODO: forward as if no program existed
	}

	// Parse output
	parsedOutput := parseJolieOutputIncoming(output)
	if parsedOutput.Action == "drop" {
		// Drop message
	} else {
		// Forward message
		localESS := copyMessageForSingleUser(ess, userID)
		dispatchMessage(localESS)
	}
	replyList := parsedOutput.Reply
	handleReplyList(userID, replyList, ess)
}

func handleReplyList(userID int, messageList []simpleMessage, originalESS *EventSourcingStruct) {
	groups := make(map[string][]int)
	for _, msg := range messageList {
		groups[msg.Message] = append(groups[msg.Message], msg.To)
	}

	eventSourceStructures := make([]*EventSourcingStruct, len(groups))
	for body, recipientList := range groups {
		newESS, err := newMessage(originalESS.SessionUID, true, body, userID, recipientList)
		if err == nil {
			eventSourceStructures = append(eventSourceStructures, newESS)
		}
	}

	for _, ess := range eventSourceStructures {
		dispatchMessage(ess)
	}
}

func copyMessageForMultipleUsers(ess *EventSourcingStruct, recipientIDs []int) *EventSourcingStruct {
	newESS := new(EventSourcingStruct)
	newESS.MessageUID = ess.MessageUID
	newESS.SessionUID = ess.SessionUID
	newESS.MessageBody = ess.MessageBody
	newESS.SenderID = ess.SenderID
	newESS.FromAutoReply = ess.FromAutoReply
	newESS.EventDestinations = ess.EventDestinations
	newESS.RecipientIDs = recipientIDs
	return newESS
}

func copyMessageForSingleUser(ess *EventSourcingStruct, userID int) *EventSourcingStruct {
	newESS := new(EventSourcingStruct)
	newESS.MessageUID = ess.MessageUID
	newESS.SessionUID = ess.SessionUID
	newESS.MessageBody = ess.MessageBody
	newESS.SenderID = ess.SenderID
	newESS.FromAutoReply = ess.FromAutoReply
	newESS.EventDestinations = ess.EventDestinations
	newESS.RecipientIDs = []int{userID}
	return newESS
}

func userHasProgram(userID int, msgDirection string, db *sql.DB) (bool, error) {
	var validTargets = []string{"send", "recv"}
	if !stringInSlice(msgDirection, validTargets) {
		return false, errors.New("invalid message direction")
	}

	// Check if entry is in database
	queryString := "SELECT * FROM user_scripts_enabled WHERE user_id=$1 AND direction=$2;"
	row := db.QueryRow(queryString, userID, msgDirection)
	if row == nil {
		// Shouldn't happen, but if it does, we continue running according to degradation of service
		return false, errors.New("database query returned nil")
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
		return false, errors.New("database query returned no rows")
	}

	// Check that what we read is expected
	if !stringInSlice(direction, validTargets) {
		return false, errors.New("read invalid direction from database")
	}

	// If nothing failed yet, we are know that the user has a script registered for given msg direction
	return true, nil
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

	fmt.Printf("[ info ] Attempting to dispatch message: \n%v\n", *ess)

	// Create a new kafka writer for the topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  listedBrokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	jsonBytes, err := json.Marshal(*ess)
	if err != nil {
		fmt.Println("[ error ] could not encode eventSourcingStruct as json for dispatch, skipping")
		return
	}

	writer.WriteMessages(context.Background(), kafka.Message{Value: jsonBytes})

}

func handleSender(ess *EventSourcingStruct) { // WARNING: MODIFIES INPUT STRUCT
	path, err := downloadUserProgram(ess.SenderID, true)
	if err != nil {
		// Log error
		fmt.Printf("[ error ] Failed to download user program for user %v: \n%v\n", ess.SenderID, err)
		return
	}

	err = runJolieSender(path, ess)
	if err != nil {
		// Do nothing
	}
	// Thats all
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
		fmt.Println("[ info ] Waiting for regular message")
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("[ error ] %v\n", err)
			continue
		}
		fmt.Printf("Got message:\n%v\n", msg.Value)

		// Parse the json
		eventSourcingStructure, err := parseEventSourcingStructure(msg.Value)
		if err != nil {
			fmt.Printf("[ error ] %v\n", err)
			continue
		}
		fmt.Printf("Parsed event sourcing struct:\n%v\n", eventSourcingStructure)

		// Check if sender set up jolie script for outgoing messages
		// Must be done in same thread & write to eventSourcingStructure directly
		hasProgram, err := userHasProgram(eventSourcingStructure.SenderID, "send", db)
		if err != nil {
			// Log error message
			fmt.Printf("[ warn ] userHasProgram(sender) err output: %v\n", err)
		}
		if hasProgram {
			handleSender(eventSourcingStructure)
		}

		// Check if reciever set up jolie script for incoming messages
		var forwardToNoChange []int = make([]int, 0)
		for _, recipient := range eventSourcingStructure.RecipientIDs {
			hasProgram, err := userHasProgram(recipient, "recv", db)
			if err != nil || !hasProgram {
				if err != nil {
					fmt.Printf("[ warn ] userHasProgram(reciever) err output: %v\n", err)
				}
				fmt.Printf("[ info ] Recipient %d has no program assigned, adding to forward as-is list.\n", recipient)
				forwardToNoChange = append(forwardToNoChange, recipient)
				continue
			} else {
				// User has program, run in new goroutine
				go handleReciever(recipient, eventSourcingStructure)
			}
		}

		// Send to everyone who didn't use user programs
		eventSourcingStructure.RecipientIDs = forwardToNoChange
		dispatchMessage(eventSourcingStructure)

	}
}
