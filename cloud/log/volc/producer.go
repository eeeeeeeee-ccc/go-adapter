package volc

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/biandoucheng/go-cloud-adapter/cloud"

	"github.com/pkg/errors"
	"github.com/volcengine/volc-sdk-golang/service/tls/pb"
	"github.com/volcengine/volc-sdk-golang/service/tls/producer"
)

type Producer struct {
	instance  producer.Producer
	signal    chan os.Signal
	projectId string
	topicId   string
}

func NewProducer(conf *Config, logLevel string, projectId string, topicId string) (*Producer, error) {
	if topicId == "" {
		return nil, errors.New("topicId is empty")
	}
	p := &Producer{
		projectId: projectId,
		topicId:   topicId,
	}

	producerConfig := producer.GetDefaultProducerConfig()
	producerConfig.Region = conf.Region
	producerConfig.Endpoint = conf.Endpoint
	producerConfig.AccessKeyID = conf.AccessKeyId
	producerConfig.AccessKeySecret = conf.AccessKeySecret
	producerConfig.LogLevel = logLevel

	producerInstance := producer.NewProducer(producerConfig)
	p.instance = producerInstance
	return p, nil
}

func (p *Producer) Provider() string {
	return cloud.VolcProvider()
}

func (p *Producer) Project() string {
	return p.projectId
}

func (p *Producer) Store() string {
	return p.topicId
}

func (p *Producer) Start() {
	p.signal = make(chan os.Signal)
	signal.Notify(p.signal, syscall.SIGTERM, os.Interrupt)
	p.instance.Start()
	go p.listen()
}

func (p *Producer) listen() {
	signal.Notify(p.signal, syscall.SIGTERM, os.Interrupt)
	if _, ok := <-p.signal; ok {
		p.Close(5000)
	}
}

func (p *Producer) Close(ms int64) {
	p.instance.Close()
}

func (p *Producer) SendLog(ts int64, source string, content map[string]string, extra map[string]any) error {
	if len(content) == 0 {
		return nil
	}

	shardHash := ""
	filename := ""
	var callback producer.CallBack

	if extra != nil {
		if _shardHash, ok := extra["shardHash"].(string); ok {
			shardHash = _shardHash
		}

		if _filename, ok := extra["filename"].(string); ok {
			filename = _filename
		}

		if _callback, ok := extra["callback"].(producer.CallBack); ok {
			callback = _callback
		}
	}

	logData := producer.GenerateLog(ts, content)
	return p.instance.SendLog(shardHash, p.topicId, source, filename, logData, callback)
}

func (p *Producer) SendLogs(ts int64, source string, contents []map[string]string, extra map[string]any) error {
	if len(contents) == 0 {
		return nil
	}

	shardHash := ""
	filename := ""
	var callback producer.CallBack

	if extra != nil {
		if _shardHash, ok := extra["shardHash"].(string); ok {
			shardHash = _shardHash
		}

		if _filename, ok := extra["filename"].(string); ok {
			filename = _filename
		}

		if _callback, ok := extra["callback"].(producer.CallBack); ok {
			callback = _callback
		}
	}

	logDatas := make([]*pb.Log, 0, len(contents))
	for _, content := range contents {
		if len(content) == 0 {
			continue
		}
		logDatas = append(logDatas, producer.GenerateLog(ts, content))
	}
	if len(logDatas) == 0 {
		return nil
	}

	logDataGroup := &pb.LogGroup{
		Logs:     logDatas,
		Source:   source,
		FileName: filename,
	}

	return p.instance.SendLogs(shardHash, p.topicId, source, filename, logDataGroup, callback)
}
