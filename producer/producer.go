package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"time"
)

type RecordValue struct {
	Count int `json:"count"`
}

func main() {
	fmt.Printf("Go Kafka Producer\n\n")

	args := parseArgs()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	defer producer.Close()

	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	createTopic(producer, *args["topic"])

	// Handle delivery reports
	go func() {
		for e := range producer.Events() {
			switch event := e.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", event.TopicPartition)

					continue
				}

				fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
					*event.TopicPartition.Topic, event.TopicPartition.Partition, event.TopicPartition.Offset)
			}
		}
	}()

	for n := 0; n < 10; n++ {
		payload := RecordValue{Count: n}

		key := "even"
		if n%2 != 0 {
			key = "odd"
		}

		value, _ := json.Marshal(payload)

		fmt.Printf("Preparing to produce record: %s\t%s\n", key, value)

		_ = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: args["topic"], Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          value,
		}, nil)
	}

	producer.Flush(15 * 1000)

	fmt.Printf("10 messages were produced to topic %s!\n", *args["topic"])
}

// createTopic creates a Kafka topic.
// If the topic already exists, this function exits gracefully.
func createTopic(producer *kafka.Producer, topic string) {
	adminClient, err := kafka.NewAdminClientFromProducer(producer)
	defer adminClient.Close()

	if err != nil {
		log.Fatalf("Failed to create admin client: %s\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDuration, _ := time.ParseDuration("60s")

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}, kafka.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		log.Fatalf("Admin client request error: %s\n", err)
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			fmt.Println("Topic already exists")

			continue
		}

		if result.Error.Code() != kafka.ErrNoError {
			log.Fatalf("Failed to create topic: %v\n", result.Error)
		}

		fmt.Printf("%v\n", result)
	}
}

func parseArgs() map[string]*string {
	topic := flag.String("t", "", "Topic name")

	flag.Parse()

	if *topic == "" {
		flag.Usage()

		os.Exit(2)
	}

	return map[string]*string{
		"topic": topic,
	}
}
