package transaction

import (
	"log"
	"os"
	"time"

	"github.com/biandoucheng/go-cloud-adapter/cloud"
	cstorage "github.com/biandoucheng/go-cloud-adapter/cloud/storage"
	"github.com/biandoucheng/go-cloud-adapter/cloud/storage/aliyun"
	"github.com/biandoucheng/go-cloud-adapter/cloud/storage/volc"
)

var Client cstorage.Client
var bucket string

func init() {
	provider := os.Getenv("BUSINESS_SHOP_STORAGE_PROVIDER")
	if cloud.IsAliyunProvider(provider) {
		NewAliyunClient()
		bucket = os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_BUCKET")
	} else {
		NewVolcClient()
		bucket = os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_BUCKET")
	}
}

func GetBucket() string {
	return bucket
}

func NewAliyunClient() {
	client, err := aliyun.NewClient(&aliyun.Config{
		Region:          os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_REGION"),
		Endpoint:        os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_STORAGE_ALIYUN_ACCESS_SECURITY_TOKEN"),
	})

	if err != nil {
		log.Fatalf("failed to create aliyun client: %v", err)
	}
	Client = client
}

func NewVolcClient() {
	client, err := volc.NewClient(&volc.Config{
		Endpoint:        os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_ENDPOINT"),
		AccessKeyId:     os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_ACCESS_SECURITY_TOKEN"),
		Region:          os.Getenv("BUSINESS_SHOP_STORAGE_VOLC_REGION"),
	}, false, time.Minute*2)

	if err != nil {
		log.Fatalf("failed to create volc client: %v", err)
	}
	Client = client
}
