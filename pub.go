package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ghiac/go-commons/log"
	helloworld "github.com/ghiac/go-kafka/api/protobuf"
	"github.com/ghiac/go-kafka/internal/models"
	"github.com/ghiac/go-kafka/internal/publisher"
	"github.com/golang/protobuf/proto"
	"strconv"
)

func main() {
	log.Initialize("info")

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "172.17.0.2"})
	if err != nil {
		panic(err)
	}

	pub := publisher.NewPublisherProcessor(p)

	pub.Start(1)

	defer p.Close()

	var topics []string

	for i := 0; i < 200; i++ {
		topics = append(topics, strconv.Itoa(i))
	}

	for _, v := range topics {
		for i := 0; i < 10; i++ {
			hello := helloworld.HelloRequest{
				Name: "masoud",
			}
			by, _ := proto.Marshal(&hello)
			pub.Tell(models.Envelop{
				Message: by,
				Topic:   v,
			})
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 100000)
}
