package transaction

import (
	"log"
	"strconv"

	"github.com/eeeeeeeee-ccc/go-adapter/example/adapter/business/shop/model"
)

func ReadTransactionLogs(ymd int, hour int, st int64, et int64) ([]model.Transaction, error) {
	extra := map[string]any{}
	result, err := Client.GetLogs(SQL_READ_TRANSACTION_LOGS, []any{ymd, hour}, st, et, 100, false, extra)
	if err != nil {
		return nil, err
	}
	transactions := make([]model.Transaction, 0, len(result.Logs))
	for _, log := range result.Logs {
		transaction := model.Transaction{}
		transaction.ParseFromLogMap(log)
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}

func StatTransactionCountHourly(ymd int, st int64, et int64) ([]map[int]int64, error) {
	result, err := Client.SelectLogs(SQL_STAT_TRANSACTION_COUNT_BY_HOUY, []any{ymd}, st, et, 0, 100, nil)
	if err != nil {
		return nil, err
	}
	log.Printf("stat transaction count hourly: %+v \n", result)
	transactions := make([]map[int]int64, 0, len(result.Logs))
	for _, log := range result.Logs {
		hourStr := log["hour"]
		hour, _ := strconv.Atoi(hourStr)
		countStr := log["number"]
		count, _ := strconv.ParseInt(countStr, 10, 64)
		transaction := map[int]int64{
			hour: count,
		}
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}
