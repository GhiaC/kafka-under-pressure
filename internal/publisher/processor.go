package publisher

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ghiac/go-commons/log"
	"github.com/ghiac/go-commons/processor"
	"github.com/ghiac/go-kafka/internal/models"
	"strconv"
)

type Processor struct {
	processor.Processor
	envelopChannel chan models.Envelop
	producer       *kafka.Producer
}

func NewPublisherProcessor(producer *kafka.Producer) *Processor {
	return &Processor{
		envelopChannel: make(chan models.Envelop),
		producer:       producer,
		Processor:      processor.New(),
	}
}

func (g *Processor) Start(size int) {
	go func() {
		for e := range g.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	g.RunPool(g, size)
}

func (g *Processor) Tell(envelop models.Envelop) {
	g.envelopChannel <- envelop
}

func (g *Processor) Worker(workerId int) {
	log.Logger.Info("Worker started:" + strconv.Itoa(workerId))
	for {
		envelop := <-g.envelopChannel
		g.process(envelop)
	}
}

func (g *Processor) process(envelop models.Envelop) {
	err := g.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &envelop.Topic, Partition: kafka.PartitionAny},
		Value:          envelop.Message,
	}, nil)
	if err != nil {
		panic(err)
	}
}
