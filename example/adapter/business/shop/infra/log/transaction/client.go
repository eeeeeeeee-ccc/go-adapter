package transaction

import (
	"log"
	"os"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/aliyun"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/log/volc"
)

var Client clog.Client

func init() {
	provider := os.Getenv("BUSINESS_SHOP_LOG_PROVIDER")
	if cloud.IsAliyunProvider(provider) {
		NewAliyunClient()
		ResigterAliyunSql()
	} else if cloud.IsVolcProvider(provider) {
		NewVolcClient()
		RegisterVolcSql()
	} else {
		log.Fatalf("invalid log provider: %s", provider)
	}
}

func NewAliyunClient() {
	Client = aliyun.NewClient(&aliyun.Config{
		Endpoint:        os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_SECURITY_TOKEN"),
	}, os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_PROJECT"), os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_LOGSTORE"))

}

func ResigterAliyunSql() {
	Client.RegistSql(SQL_READ_TRANSACTION_LOGS, clog.Sql{
		Name:     SQL_READ_TRANSACTION_LOGS,
		Template: SQL_ALIYUN_READ_TRANSACTION_LOGS,
		Project:  os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_PROJECT"),
		Store:    os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_LOGSTORE"),
	})

	Client.RegistSql(SQL_STAT_TRANSACTION_COUNT_BY_HOUY, clog.Sql{
		Name:     SQL_STAT_TRANSACTION_COUNT_BY_HOUY,
		Template: SQL_ALIYUN_STAT_TRANSACTION_COUNT_BY_HOUY,
		Project:  os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_PROJECT"),
		Store:    os.Getenv("BUSINESS_SHOP_LOG_ALIYUN_LOGSTORE"),
	})
}

func NewVolcClient() {
	Client = volc.NewClient(&volc.Config{
		Region:          os.Getenv("BUSINESS_SHOP_LOG_VOLC_REGION"),
		Endpoint:        os.Getenv("BUSINESS_SHOP_LOG_VOLC_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_LOG_VOLC_ACCESS_SECURITY_TOKEN"),
	}, os.Getenv("BUSINESS_SHOP_LOG_VOLC_PROJECT_ID"), os.Getenv("BUSINESS_SHOP_LOG_VOLC_TOPIC_ID"))
}

func RegisterVolcSql() {
	Client.RegistSql(SQL_READ_TRANSACTION_LOGS, clog.Sql{
		Name:     SQL_READ_TRANSACTION_LOGS,
		Template: SQL_VOLC_READ_TRANSACTION_LOGS,
		Project:  os.Getenv("BUSINESS_SHOP_LOG_VOLC_PROJECT_ID"),
		Store:    os.Getenv("BUSINESS_SHOP_LOG_VOLC_TOPIC_ID"),
	})

	Client.RegistSql(SQL_STAT_TRANSACTION_COUNT_BY_HOUY, clog.Sql{
		Name:     SQL_STAT_TRANSACTION_COUNT_BY_HOUY,
		Template: SQL_VOLC_STAT_TRANSACTION_COUNT_BY_HOUY,
		Project:  os.Getenv("BUSINESS_SHOP_LOG_VOLC_PROJECT_ID"),
		Store:    os.Getenv("BUSINESS_SHOP_LOG_VOLC_TOPIC_ID"),
	})
}
