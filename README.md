# Apache Kafka Go

Testing Apache Kafka using Go.

## Instructions

1. Provision the single node Kafka cluster using Docker:

    ```bash
    docker-compose -p apache-kafka-go up -d
    ```

2. Add the following entry to the `hosts` file on your machine (e.g. /etc/hosts):

    ```
    127.0.0.1   kafka
    ```
   
3. Play around with the `producer.go` and `consumer.go` sample applications:

    ```bash
    go run producer/producer.go -t topic
    go run consumer/consumer.go -t topic -g group
    ```
