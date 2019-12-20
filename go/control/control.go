package control // import "DM874-jolie-exec/control"

import (
	"context"
	"database/sql"
	"time"

	"github.com/segmentio/kafka-go"
)

func parseControlMessage(sourceTopic *kafka.Reader) {

}

func ConfigManager(reader *kafka.Reader, db *sql.DB) {
	ctx := context.Background()
	for {
		reader.ReadMessage(ctx)

		time.Sleep(time.Millisecond * 50)
	}
}
