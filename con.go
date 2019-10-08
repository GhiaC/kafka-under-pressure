package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ghiac/go-commons/log"
	"github.com/ghiac/go-commons/signal"
	"github.com/ghiac/go-kafka/internal/subscriber"
	"strconv"
)

func main() {
	log.Initialize("info")

	var topics []string

	for i := 0; i < 100; i++ {
		topics = append(topics, strconv.Itoa(i))
	}

	var topics2 []string

	for i := 100; i < 200; i++ {
		topics2 = append(topics2, strconv.Itoa(i))
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "172.17.0.2",
		"group.id":          "2",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		panic(err)
	}

	sub := subscriber.NewSubscriberProcessor(c)
	sub.Start(3)

	err = c.SubscribeTopics(topics2, nil)

	if err != nil {
		panic(err)
	}

	sub2 := subscriber.NewSubscriberProcessor(c)
	sub2.Start(3)

	var sig = signal.Signal
	sig.Wait()
}
