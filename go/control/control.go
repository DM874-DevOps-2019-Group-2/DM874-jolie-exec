package control // import "DM874-jolie-exec/control"

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

/*ConfigMessageStruct used when parsing control message JSON*/
type ConfigMessageStruct struct {
	ActionType *string `json:"actionType"`
	UserID     *int    `json:"userId"`
	Target     *string `json:"target"`
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func parseControlMessage(jsonBytes []byte) (*ConfigMessageStruct, error) {
	cms := new(ConfigMessageStruct)

	validTargets := make([]string, 2)
	validActionTypes := make([]string, 2)
	validTargets = append(validTargets, "send", "recv")
	validActionTypes = append(validActionTypes, "enable", "disable")

	jsonDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	jsonDecoder.DisallowUnknownFields()

	err := jsonDecoder.Decode(cms)
	if err != nil {
		return nil, errors.New("error decoding control message")
	}

	if cms.ActionType == nil ||
		cms.UserID == nil ||
		cms.Target == nil {
		return nil, errors.New("missing field while decoding Control Message")
	}

	if !stringInSlice(*cms.ActionType, validActionTypes) || !stringInSlice(*cms.Target, validTargets) {
		return nil, errors.New("error parsing control message: invalid action type or target")
	}

	return cms, nil
}

func enableUserScript(userID int, direction string, db *sql.DB) error {
	queryString := "INSERT INTO jolie_exec VALUES ($1, $2);"
	_, err := db.Exec(queryString, userID, direction)
	return err
}

func disableUserScript(userID int, direction string, db *sql.DB) error {
	queryString := "DELETE from jolie_exec WHERE user_id=$1 AND direction=$2;"
	_, err := db.Exec(queryString, userID, direction)
	return err
}

func handleParsedConfigMessage(confMsg *ConfigMessageStruct, db *sql.DB) {
	var err error
	switch *confMsg.ActionType {
	case "enable":
		err = enableUserScript(*confMsg.UserID, *confMsg.Target, db)
		break
	case "disable":
		err = disableUserScript(*confMsg.UserID, *confMsg.Target, db)
		break
	default:
		err = errors.New("invalid control message")
	}
	if err != nil {
		fmt.Printf("[ error ] Unable to correctly handle control message. Error info: \n%v\n", err)
	}
}

/*ConfigManager continuously reads and handles configuration messages from kafka*/
func ConfigManager(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	for {
		fmt.Println("[ info ] Waiting for control message")
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("error reading from configuration topic: \n%v\n", err)
		}

		parsedMessage, err := parseControlMessage(message.Value)
		if err != nil {
			fmt.Printf("error parsing control message: \n%v\n", err)
		}

		handleParsedConfigMessage(parsedMessage, db)

		time.Sleep(time.Millisecond * 50)
	}
}
