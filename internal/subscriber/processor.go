package subscriber

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ghiac/go-commons/log"
	"github.com/ghiac/go-commons/processor"
	"github.com/ghiac/go-commons/utils"
	helloworld "github.com/ghiac/go-kafka/api/protobuf"
	"github.com/golang/protobuf/proto"
	"strconv"
	"time"
)

type Processor struct {
	processor.Processor
	channel  chan string
	consumer *kafka.Consumer
	topics   *utils.ConcurrentStringMap
	counter  *utils.ConcurrentCounter
}

func NewSubscriberProcessor(consumer *kafka.Consumer) *Processor {
	return &Processor{
		channel:   make(chan string),
		Processor: processor.New(),
		consumer:  consumer,
		topics:    utils.NewConcurrentStringMap(),
		counter:   utils.NewConcurrentCounter(),
	}
}

func (g *Processor) Start(size int) {
	go func() {
		for {
			time.Sleep(time.Second * 1)
			fmt.Print(g.topics.Count(), " ")
			fmt.Println(g.counter.Count)
		}

	}()
	g.RunPool(g, size)
}

func (g *Processor) Tell(envelop string) {
	g.channel <- envelop
}

func (g *Processor) Worker(workerId int) {
	log.Logger.Info("Worker started:" + strconv.Itoa(workerId))
	//for {
	//	envelop := <-g.channel
	g.process("ss")
	//}
}

func (g *Processor) process(envelop string) {
	for {
		msg, err := g.consumer.ReadMessage(-1)
		if err == nil {
			hello := helloworld.HelloRequest{}
			_ = proto.Unmarshal(msg.Value, &hello)

			g.topics.Add(*msg.TopicPartition.Topic)
			g.counter.Increase()

			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
