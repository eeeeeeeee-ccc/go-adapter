package transaction

import (
	"log"
	"os"
	"time"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/aliyun"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/volc"

	aliyunslsconsumer "github.com/aliyun/aliyun-log-go-sdk/consumer"
	volctlsconsumer "github.com/volcengine/volc-sdk-golang/service/tls/consumer"
)

var LogConsumer clog.Consumer

func init() {
	provider := os.Getenv("BUSINESS_SHOP_LOG_PROVIDER")
	if cloud.IsAliyunProvider(provider) {
		InitAliyunConsumer()
	} else if cloud.IsVolcProvider(provider) {
		InitVolcConsumer()
	} else {
		log.Fatalf("invalid log provider: %s", provider)
	}
	if LogConsumer != nil {
		LogConsumer.Init(PrintTransactionLog)
		LogConsumer.Start()
	}
}

func InitAliyunConsumer() {
	LogConsumer = aliyun.NewConsumer(&aliyunslsconsumer.LogHubConfig{
		Region:            os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_REGION"),
		Endpoint:          os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ENDPOINT"),
		AccessKeyID:       os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_ID"),
		AccessKeySecret:   os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_SECRET"),
		SecurityToken:     os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_SECURITY_TOKEN"),
		Project:           os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_PROJECT"),
		Logstore:          os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_LOGSTORE"),
		ConsumerGroupName: os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_CONSUMER_GROUP"),
		ConsumerName:      CunsumerName(),
		CursorPosition:    aliyunslsconsumer.END_CURSOR,
		CursorStartTime:   time.Now().Unix(),
		AllowLogLevel:     "error",
	})
}

func InitVolcConsumer() {
	conf := volctlsconsumer.GetDefaultConsumerConfig()
	conf.Region = os.Getenv("BUSINESS_SHOP_LOG_VOLC_REGION")
	conf.Endpoint = os.Getenv("BUSINESS_SHOP_LOG_VOLC_ENDPOINT")
	conf.AccessKeyID = os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_ID")
	conf.AccessKeySecret = os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_SECRET")
	conf.SecurityToken = os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_SECURITY_TOKEN")
	conf.ProjectID = os.Getenv("BUSINESS_SHOP_LOG_VOLC_PROJECT_ID")
	conf.TopicIDList = []string{os.Getenv("BUSINESS_SHOP_LOG_VOLC_TOPIC_ID")}
	conf.ConsumerGroupName = os.Getenv("BUSINESS_SHOP_LOG_VOLC_CONSUMER_GROUP")
	conf.ConsumerName = CunsumerName()
	conf.ConsumeFrom = volctlsconsumer.ConsumeFromEnd
	LogConsumer = volc.NewConsumer(conf)
}

func CunsumerName() string {
	return os.Getenv("SERVICE_ENV") + ":your-serice-hostname"
}
