package volc

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"

	consumerLibrary "github.com/volcengine/volc-sdk-golang/service/tls/consumer"
	"github.com/volcengine/volc-sdk-golang/service/tls/pb"
)

type Consumer struct {
	conf     *consumerLibrary.Config
	handler  func(ts int64, tns int64, log map[string]string)
	instance consumerLibrary.Consumer
	signal   chan os.Signal
}

func NewConsumer(conf *consumerLibrary.Config) *Consumer {
	return &Consumer{
		conf: conf,
	}
}

func (c *Consumer) Provider() string {
	return cloud.VolcProvider()
}

func (c *Consumer) Project() string {
	return c.conf.ProjectID
}

func (c *Consumer) Store() string {
	if len(c.conf.TopicIDList) > 0 {
		return c.conf.TopicIDList[0]
	}
	return ""
}

func (c *Consumer) Consumer() string {
	return c.conf.ConsumerName
}

func (c *Consumer) ConsumeGroup() string {
	return c.conf.ConsumerGroupName
}

func (c *Consumer) Init(handler func(ts int64, tns int64, log map[string]string)) {
	c.handler = handler
}

func (c *Consumer) Start() {
	consumerWorker, err := consumerLibrary.NewConsumer(context.Background(), c.conf, c.Handler)
	if err != nil {
		log.Fatalf("new volc log consumer failed: %v", err)
	}
	c.instance = consumerWorker

	c.signal = make(chan os.Signal)
	signal.Notify(c.signal, syscall.SIGTERM, os.Interrupt)

	c.instance.Start()
	go c.listen()
}

func (c *Consumer) listen() {
	if _, ok := <-c.signal; ok {
		c.Close()
	}
}

func (c *Consumer) Close() {
	c.instance.Stop()
}

func (c *Consumer) Handler(topicID string, shardID int, logGroupList *pb.LogGroupList) {
	for _, logGroup := range logGroupList.LogGroups {
		for _, log := range logGroup.Logs {
			mp := map[string]string{}
			for _, content := range log.Contents {
				mp[content.GetKey()] = content.GetValue()
			}
			c.handler(log.GetTime(), int64(log.GetTimeNs()), mp)
		}
	}
}
