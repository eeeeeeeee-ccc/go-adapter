package transaction

import (
	"time"

	"github.com/biandoucheng/go-cloud-adapter/example/adapter/business/shop/model"
)

func WriteToLog(transaction *model.Transaction) error {
	mp := transaction.ToLogMap()
	return LogProducer.SendLog(time.Now().Unix(), "your-service-hostname", mp, nil)
}
