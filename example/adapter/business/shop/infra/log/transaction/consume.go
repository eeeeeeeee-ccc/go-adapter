package transaction

import "log"

func PrintTransactionLog(ts int64, tns int64, data map[string]string) {
	log.Printf("consumer get transaction log: %+v \n", data)
}
