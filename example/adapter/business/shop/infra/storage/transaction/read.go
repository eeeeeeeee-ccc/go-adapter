package transaction

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/biandoucheng/go-cloud-adapter/example/adapter/business/shop/model"
)

func ReadTransactionRecord(ymdh string) ([]model.Transaction, error) {
	transactions := []model.Transaction{}
	objectKey := NewTransactionRecordObjectKeyFromName(ymdh)
	log.Printf("reading transaction record from object key: %s \n", objectKey)
	content, err := Client.GetObjectContent(GetBucket(), objectKey, 0, nil)
	if err != nil {
		return nil, err
	}

	contents := strings.Split(content, "\n")
	for _, line := range contents {
		if line == "" {
			continue
		}
		var transaction model.Transaction
		err := json.Unmarshal([]byte(line), &transaction)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}
