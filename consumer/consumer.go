package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type RecordValue struct {
	Count int `json:"count"`
}

func main() {
	fmt.Printf("Go Kafka Consumer\n\n")

	args := parseArgs()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          *args["consumerGroup"],
		"auto.offset.reset": "earliest",
	})
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()

		if err != nil {
			log.Fatalf("Failed to close consumer: %s\n", err)
		}
	}(consumer)

	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	err = consumer.SubscribeTopics([]string{*args["topic"]}, nil)

	// Handle SIGINT (Ctrl-C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	total := 0
	run := true

	for run {
		select {
		case sig := <-sigChan:
			fmt.Printf("Caught signal %v, terminating\n", sig)

			run = false
		default:
			message, err := consumer.ReadMessage(100 * time.Millisecond)

			if err != nil {
				continue
			}

			key := string(message.Key)
			value := message.Value

			payload := RecordValue{}
			err = json.Unmarshal(value, &payload)

			if err != nil {
				fmt.Printf("Failed to decode JSON at offset %d: %v", message.TopicPartition.Offset, err)

				continue
			}

			count := payload.Count
			total += count

			fmt.Printf("Consumed record with key \"%s\" and value %s, and update total count to %d\n",
				key, value, total)
		}
	}

	fmt.Printf("Closing consumer...")
}

func parseArgs() map[string]*string {
	topic := flag.String("t", "", "Topic name")
	consumerGroup := flag.String("g", "", "Consumer group identifier")

	flag.Parse()

	if *topic == "" || *consumerGroup == "" {
		flag.Usage()

		os.Exit(2)
	}

	return map[string]*string{
		"topic":         topic,
		"consumerGroup": consumerGroup,
	}
}
