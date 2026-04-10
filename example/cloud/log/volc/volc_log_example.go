package volc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/volcengine/volc-sdk-golang/service/tls"
	log_consumer "github.com/volcengine/volc-sdk-golang/service/tls/consumer"
	"github.com/volcengine/volc-sdk-golang/service/tls/pb"
	"github.com/volcengine/volc-sdk-golang/service/tls/producer"
)

// 生产日志
func ProduceExample() {
	// 初始化客户端，推荐通过环境变量动态获取火山引擎密钥等身份认证信息，以免 AccessKey 硬编码引发数据安全风险。详细说明请参考https://www.volcengine.com/docs/6470/1166455
	// 使用 STS 时，ak 和 sk 均使用临时密钥，且设置 VOLCENGINE_TOKEN；不使用 STS 时，VOLCENGINE_TOKEN 部分传空
	tlsProducerCfg := producer.GetDefaultProducerConfig()
	tlsProducerCfg.Endpoint = os.Getenv("VOLCENGINE_ENDPOINT")
	tlsProducerCfg.Region = os.Getenv("VOLCENGINE_REGION")
	tlsProducerCfg.AccessKeyID = os.Getenv("VOLCENGINE_ACCESS_KEY_ID")
	tlsProducerCfg.AccessKeySecret = os.Getenv("VOLCENGINE_ACCESS_KEY_SECRET")

	// 初始化并启动Producer
	tlsProducer := producer.NewProducer(tlsProducerCfg)
	tlsProducer.Start()

	// 请根据您的需要，填写topicId、source、filename。具体说明，请参考https://www.volcengine.com/docs/6470/112191。
	topicID := "your-topic-id"
	source := "your-log-source"
	filename := "your-log-filename"

	// 调用Producer SendLog接口，一次提交一条日志
	// 您可根据实际需要，自行定义实现用于业务处理的CallBack，传入SendLog接口
	err := tlsProducer.SendLog("", topicID, source, filename, &pb.Log{
		Contents: []*pb.LogContent{
			{
				Key:   "key1",
				Value: "value1",
			},
			{
				Key:   "key2",
				Value: "value2",
			},
		},
		Time: time.Now().Unix(),
	}, nil)
	if err != nil {
		// 处理错误
		fmt.Println(err.Error())
	}

	// 调用Producer SendLogs接口，一次提交多条日志
	// 您可根据实际需要，自行定义实现用于业务处理的CallBack，传入SendLogs接口

	err = tlsProducer.SendLogs("", topicID, source, filename, &pb.LogGroup{
		Source:   source,
		FileName: filename,
		Logs: []*pb.Log{
			{
				Contents: []*pb.LogContent{
					{
						Key:   "key1",
						Value: "value1-1",
					},
					{
						Key:   "key2",
						Value: "value2-1",
					},
				},
				Time: time.Now().Unix(),
			},
			{
				Contents: []*pb.LogContent{
					{
						Key:   "key1",
						Value: "value1-2",
					},
					{
						Key:   "key2",
						Value: "value2-2",
					},
				},
				Time: time.Now().Unix(),
			},
		},
	}, nil)
	if err != nil {
		// 处理错误
		fmt.Println(err.Error())
	}

	// 关闭Producer
	tlsProducer.Close()
}

// 消费日志
func ConsumeExample() error {
	// 获取消费组的默认配置
	consumerCfg := log_consumer.GetDefaultConsumerConfig()
	// 请配置您的Endpoint、Region、AccessKeyID、AccessKeySecret等基本信息
	consumerCfg.Endpoint = os.Getenv("VOLCENGINE_ENDPOINT")
	consumerCfg.Region = os.Getenv("VOLCENGINE_REGION")
	consumerCfg.AccessKeyID = os.Getenv("VOLCENGINE_ACCESS_KEY_ID")
	consumerCfg.AccessKeySecret = os.Getenv("VOLCENGINE_ACCESS_KEY_SECRET")
	// 请配置您的日志项目ID和日志主题ID列表
	consumerCfg.ProjectID = "<YOUR-PROJECT-ID>"
	consumerCfg.TopicIDList = []string{"<YOUR-TOPIC-ID>"}
	// 请配置您的消费组名称（若您未创建过消费组，SDK将默认为您创建指定名称的消费组）
	consumerCfg.ConsumerGroupName = "<CONSUMER-GROUP-NAME>"
	// 请配置消费者名称（同一个消费组的不同消费者需要保证不同名）
	consumerCfg.ConsumerName = "<CONSUMER_NAME>"

	// 定义日志消费函数，您可根据业务需要，自行实现处理LogGroupList的日志消费函数
	// 下面展示了逐个打印消费到的每条日志的每个键值对的代码实现示例
	var handleLogs = func(topicID string, shardID int, l *pb.LogGroupList) {
		fmt.Printf("received new logs from topic: %s, shard: %d\n", topicID, shardID)
		for _, logGroup := range l.LogGroups {
			for _, log := range logGroup.Logs {
				for _, content := range log.Contents {
					fmt.Printf("%s: %s\n", content.Key, content.Value)
				}
			}
		}
	}

	// 创建消费者
	consumer, err := log_consumer.NewConsumer(context.TODO(), consumerCfg, handleLogs)
	if err != nil {
		return errors.Wrap(err, "get new consumer failed: ")
	}

	// 启动消费者消费
	if err := consumer.Start(); err != nil {
		return errors.Wrap(err, "start consumer failed: ")
	}

	// 等待消费
	<-time.After(time.Second * 60)

	// 停止消费
	consumer.Stop()

	return nil
}

// 查询日志
func GetLogsExample() {
	// 初始化客户端，推荐通过环境变量动态获取火山引擎密钥等身份认证信息，以免 AccessKey 硬编码引发数据安全风险。详细说明请参考https://www.volcengine.com/docs/6470/1166455
	// 使用 STS 时，ak 和 sk 均使用临时密钥，且设置 VOLCENGINE_TOKEN；不使用 STS 时，VOLCENGINE_TOKEN 部分传空
	client := tls.NewClient(os.Getenv("VOLCENGINE_ENDPOINT"), os.Getenv("VOLCENGINE_ACCESS_KEY_ID"),
		os.Getenv("VOLCENGINE_ACCESS_KEY_SECRET"), os.Getenv("VOLCENGINE_TOKEN"), os.Getenv("VOLCENGINE_REGION"))

	// 查询分析日志数据
	// 请根据您的需要，填写TopicId、Query、StartTime、EndTime、Limit等参数值
	// SearchLogs API的请求参数规范和限制请参阅https://www.volcengine.com/docs/6470/112195
	resp, err := client.SearchLogsV2(&tls.SearchLogsRequest{
		TopicID:   "TopicId",
		Query:     "*",
		StartTime: 1346457600000,
		EndTime:   1630454400000,
		Limit:     20,
	})
	if err != nil {
		// 处理错误
		fmt.Println(err.Error())
	}

	// 打印SearchLogs接口返回值中的部分基本信息
	// 请根据您的需要，自行处理返回值中的其他信息
	fmt.Println(resp.Status)
	fmt.Println(resp.Count)
	fmt.Println(resp.Analysis)
}

// 提交日志
func PostLogsExample() {
	// 初始化客户端，推荐通过环境变量动态获取火山引擎密钥等身份认证信息，以免 AccessKey 硬编码引发数据安全风险。详细说明请参考https://www.volcengine.com/docs/6470/1166455
	// 使用 STS 时，ak 和 sk 均使用临时密钥，且设置 VOLCENGINE_TOKEN；不使用 STS 时，VOLCENGINE_TOKEN 部分传空
	client := tls.NewClient(os.Getenv("VOLCENGINE_ENDPOINT"), os.Getenv("VOLCENGINE_ACCESS_KEY_ID"),
		os.Getenv("VOLCENGINE_ACCESS_KEY_SECRET"), os.Getenv("VOLCENGINE_TOKEN"), os.Getenv("VOLCENGINE_REGION"))

	// （不推荐）本文档以PutLogs接口同步请求的方式上传日志为例
	// （推荐）在实际生产环境中，为了提高数据写入效率，建议通过Go Producer方式写入日志数据

	// 如果选择使用PutLogs上传日志的方式，建议您一次性聚合多条日志后调用一次PutLogs接口，以提升吞吐率并避免触发限流
	// 请根据您的需要，填写TopicId、Source、FileName和Logs列表，建议您使用lz4压缩
	// PutLogs API的请求参数规范和限制请参阅 https://www.volcengine.com/docs/6470/112191
	_, _ = client.PutLogsV2(&tls.PutLogsV2Request{
		TopicID:      "topicID",
		CompressType: "lz4",
		Source:       "your-log-source",
		FileName:     "your-log-filename",
		Logs: []tls.Log{
			{
				Contents: []tls.LogContent{
					{
						Key:   "key1",
						Value: "value1-1",
					},
					{
						Key:   "key2",
						Value: "value2-1",
					},
				},
			},
			{
				Contents: []tls.LogContent{
					{
						Key:   "key1",
						Value: "value1-2",
					},
					{
						Key:   "key2",
						Value: "value2-2",
					},
				},
			},
		},
	})
}
