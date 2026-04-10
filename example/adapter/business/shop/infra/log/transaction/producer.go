package transaction

import (
	"log"
	"os"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/aliyun"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/volc"
)

var LogProducer clog.Producer

func init() {
	provider := os.Getenv("BUSINESS_SHOP_LOG_PROVIDER")
	if cloud.IsAliyunProvider(provider) {
		InitAliyunProducer()
	} else if cloud.IsVolcProvider(provider) {
		InitVolcProducer()
	} else {
		log.Fatalf("invalid log provider: %s", provider)
	}

	if LogProducer != nil {
		LogProducer.Start()
	}
}

func InitAliyunProducer() {
	p, err := aliyun.NewProducer(&aliyun.Config{
		Region:          os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_REGION"),
		Endpoint:        os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_SECURITY_TOKEN"),
	}, "error", os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_PROJECT"), os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_LOGSTORE"))
	if err != nil {
		log.Fatalf("new aliyun log producer failed: %v", err)
	}
	LogProducer = p
}

func InitVolcProducer() {
	p, err := volc.NewProducer(&volc.Config{
		Region:          os.Getenv("BUSINESS_SHOP_LOG_VOLC_REGION"),
		Endpoint:        os.Getenv("BUSINESS_SHOP_LOG_VOLC_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_SECURITY_TOKEN"),
	}, "error", os.Getenv("BUSINESS_SHOP_LOG_VOLC_PROJECT_ID"), os.Getenv("BUSINESS_SHOP_LOG_VOLC_TOPIC_ID"))

	if err != nil {
		log.Fatalf("new volc log producer failed: %v", err)
	}
	LogProducer = p
}
