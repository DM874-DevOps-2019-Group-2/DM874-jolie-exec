package main

//go:generate protoc -I pb --go_out=plugins=grpc:pb pb/pb.proto

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var producerMapLock sync.RWMutex

// func produce(writer *kafka.Writer, data *pb.Message) {
// 	time.Sleep(time.Second * 2)

// 	asBa, _ := proto.Marshal(data)

// 	_ = writer.WriteMessages(context.Background(),
// 		kafka.Message{
// 			Value: asBa,
// 		},
// 	)
// }

func applyLanguageFilter(data string) string {
	var languageFilter = [...]string{
		"curseword",
	}

	message := data // copy

	// Apply language filter
	for _, replacementString := range languageFilter {
		message = strings.Replace(
			message,
			replacementString,
			strings.Repeat(
				replacementString,
				len(replacementString),
			),
			-1,
		)
	}

	return message
}

// Either gets the element of the map using a read many lock or creates a new using a write one lock
func readOrInit(m map[string]*kafka.Writer, topic string, listedBrokers []string) *kafka.Writer {
	producerMapLock.RLock()
	writer, ok := m[topic]
	producerMapLock.RUnlock()

	if !ok {
		producerMapLock.Lock()
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  listedBrokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		})
		m[topic] = writer
		producerMapLock.Unlock()
	}

	return writer
}

func main() {
	inTopic := os.Getenv("PROFANITY_FILTER_CONSUMER_TOPIC")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	listedBrokers := strings.Split(kafkaBrokers, ",")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     listedBrokers,
		Topic:       inTopic,
		Partition:   0,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     time.Millisecond * 100,
		StartOffset: kafka.LastOffset,
	})

	// writerMap := make(map[string]*kafka.Writer)

	for {
		// m, err := reader.ReadMessage(context.Background())
		// if err != nil {
		// 	break
		// }

		// newData := &pb.Message{}
		// _ = proto.Unmarshal(m.Value, newData)

		// minimum := int32(^uint(0) >> 1)
		// for c := range newData.Tasks {
		// 	if minimum < c {
		// 		minimum = c
		// 	}
		// }

		// elem, ok := newData.Tasks[minimum]

		// outTopic := elem.Topic

		// if !ok {
		// 	break
		// }

		// newData.Statefuldata = applyLanguageFilter(newData.Statefuldata)

		// Its expensive to create a new writer connection on each message, we we use a type of cached map of topic to producer
		// writer := readOrInit(writerMap, outTopic, listedBrokers)

		// delete(newData.Tasks, minimum) // "pop" the job we are doing, off the stack

		//Produce the next message
		// go produce(writer, newData)
	}

	ce := reader.Close()
	fmt.Print(ce)
}
