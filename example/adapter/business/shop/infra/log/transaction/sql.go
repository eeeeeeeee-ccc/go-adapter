package transaction

// SQL
const (
	SQL_READ_TRANSACTION_LOGS          = "read_transaction_logs"
	SQL_STAT_TRANSACTION_COUNT_BY_HOUY = "stat_transaction_count_hourly"
)

// 阿里云SQL
const (
	SQL_ALIYUN_READ_TRANSACTION_LOGS          = `* AND ymd : %d AND hour : %d`
	SQL_ALIYUN_STAT_TRANSACTION_COUNT_BY_HOUY = `* AND ymd : %d  | SELECT hour,category,productName, COUNT(*) AS number GROUP BY hour,category,productName ORDER BY hour DESC`
)

// 火山云SQL
const (
	SQL_VOLC_READ_TRANSACTION_LOGS          = `* AND ymd : %d AND hour : %d`
	SQL_VOLC_STAT_TRANSACTION_COUNT_BY_HOUY = `* AND ymd : %d  | SELECT hour,category,productName, COUNT(*) AS number GROUP BY hour,category,productName ORDER BY hour DESC`
)
