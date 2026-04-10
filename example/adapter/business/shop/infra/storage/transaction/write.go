package transaction

import (
	"encoding/json"
	"time"

	"github.com/eeeeeeeee-ccc/go-adapter/example/adapter/business/shop/model"
)

const (
	TransactionRecordDir        = "go-cloud-adapter/business-shop"
	TransactionRecordFilePrefix = "transaction-record-"
	TransactionRecordFileSuffix = ".txt"
)

func NewTransactionRecordObjectKey() string {
	return TransactionRecordDir + "/" + TransactionRecordFilePrefix + time.Now().Format("2006010215") + TransactionRecordFileSuffix
}

func NewTransactionRecordObjectKeyFromName(name string) string {
	return TransactionRecordDir + "/" + TransactionRecordFilePrefix + name + TransactionRecordFileSuffix
}

func WriteTransactionRecord(transaction *model.Transaction) error {
	byts, _ := json.Marshal(transaction)
	objectKey := NewTransactionRecordObjectKey()
	_, err := Client.AppendObjectFromContent(GetBucket(), objectKey, string(byts)+"\n", -1, nil, 1024*512, nil)
	return err
}
