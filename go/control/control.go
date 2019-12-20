package control // import "DM874-jolie-exec/control"

import (
	"context"
	"database/sql"
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

func parseControlMessage(jsonBytes []byte) (*ConfigMessageStruct, error) {
	cms := new(ConfigMessageStruct)

	return cms, nil
}

func enableUserScript(userID int, direction string, db *sql.DB) {
	queryString := "INSERT INOT jolie_exec VALUES ($1, $2);"
	db.Query(queryString, userID, direction)
}

func disableUserScript(userID int, direction string, db *sql.DB) {
	queryString := "DELETE from jolie_exec WHERE user_id=$1 AND direction=$2;"
	db.Query(queryString, userID, direction)
}

func handleParsedConfigMessage(confMsg *ConfigMessageStruct, db *sql.DB) {
	switch *confMsg.ActionType {
	case "enable":
		switch *confMsg.Target {
		case "recv":
			break
		case "send":
			break
		}
		break
	case "disable":
		disableUserScript(*confMsg.UserID, *confMsg.Target, db)
		break
	}
}

/*ConfigManager continuously reads and handles configuration messages from kafka*/
func ConfigManager(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	for {
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
