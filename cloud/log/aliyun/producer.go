package aliyun

import (
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/biandoucheng/go-cloud-adapter/cloud"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/producer"
)

type Producer struct {
	instance *producer.Producer
	signal   chan os.Signal
	project  string
	logstore string
}

func NewProducer(conf *Config, logLevel string, project string, logstore string) (*Producer, error) {
	if project == "" || logstore == "" {
		return nil, errors.New("project or logstore is empty")
	}
	p := &Producer{
		project:  project,
		logstore: logstore,
	}

	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Endpoint = conf.Endpoint
	producerConfig.AccessKeyID = conf.AccessKeyId
	producerConfig.AccessKeySecret = conf.AccessKeySecret
	producerConfig.AllowLogLevel = logLevel

	producerInstance, err := producer.NewProducer(producerConfig)
	if err != nil {
		return nil, err
	}
	p.instance = producerInstance
	return p, nil
}

func (p *Producer) Provider() string {
	return cloud.AliyunProvider()
}

func (p *Producer) Project() string {
	return p.project
}

func (p *Producer) Store() string {
	return p.logstore
}

func (p *Producer) Start() {
	p.signal = make(chan os.Signal)
	signal.Notify(p.signal, syscall.SIGTERM, os.Interrupt)
	p.instance.Start()
	go p.listen()
}

func (p *Producer) listen() {
	if _, ok := <-p.signal; ok {
		p.Close(5000)
	}
}

func (p *Producer) Close(ms int64) {
	if ms <= 0 {
		p.instance.SafeClose()
	} else {
		p.instance.Close(ms)
	}
}

func (p *Producer) SendLog(ts int64, source string, content map[string]string, extra map[string]any) error {
	if len(content) == 0 {
		return nil
	}

	topic := ""
	if extra != nil {
		if _topic, ok := extra["topic"].(string); ok {
			topic = _topic
		}
	}

	logData := producer.GenerateLog(uint32(ts), content)
	return p.instance.SendLog(p.project, p.logstore, topic, source, logData)
}

func (p *Producer) SendLogs(ts int64, source string, contents []map[string]string, extra map[string]any) error {
	if len(contents) == 0 {
		return nil
	}

	topic := ""
	if extra != nil {
		if _topic, ok := extra["topic"].(string); ok {
			topic = _topic
		}
	}

	logDatas := make([]*sls.Log, 0, len(contents))
	for _, content := range contents {
		if len(content) == 0 {
			continue
		}
		logDatas = append(logDatas, producer.GenerateLog(uint32(ts), content))
	}
	if len(logDatas) == 0 {
		return nil
	}
	return p.instance.SendLogList(p.project, p.logstore, topic, source, logDatas)
}
