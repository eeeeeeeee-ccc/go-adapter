package aliyun

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
)

type Consumer struct {
	conf     *consumerLibrary.LogHubConfig
	handler  func(ts int64, tns int64, log map[string]string)
	instance *consumerLibrary.ConsumerWorker
	signal   chan os.Signal
}

func NewConsumer(conf *consumerLibrary.LogHubConfig) *Consumer {
	return &Consumer{
		conf: conf,
	}
}

func (c *Consumer) Provider() string {
	return cloud.AliyunProvider()
}

func (c *Consumer) Project() string {
	return c.conf.Project
}

func (c *Consumer) Store() string {
	return c.conf.Logstore
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
	consumerWorker := consumerLibrary.InitConsumerWorker(*c.conf, c.Handler)
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
	c.instance.StopAndWait()
}

func (c *Consumer) Handler(shardId int, logGroupList *sls.LogGroupList) string {
	for _, logGroup := range logGroupList.LogGroups {
		for _, log := range logGroup.Logs {
			mp := map[string]string{}
			for _, content := range log.Contents {
				mp[content.GetKey()] = content.GetValue()
			}
			c.handler(int64(log.GetTime()), int64(log.GetTimeNs()), mp)
		}
	}
	return ""
}
