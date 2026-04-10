package log

import "fmt"

// 日志生产者接口
type Producer interface {
	Provider() string
	Project() string
	Store() string
	Start()
	Close(ms int64)
	SendLog(ts int64, source string, content map[string]string, extra map[string]any) error
	SendLogs(ts int64, source string, contents []map[string]string, extra map[string]any) error
}

// 日志消费者接口
type Consumer interface {
	Provider() string
	Init(handler func(ts int64, tns int64, log map[string]string))
	Project() string
	Store() string
	Consumer() string
	ConsumeGroup() string
	Start()
	Close()
}

// 日志服务客户端接口
type Client interface {
	Provider() string
	Project() string
	Store() string
	RegistSql(name string, sql Sql) error                                                                                               // 注册SQL模版
	FormatSql(name string, formats []any) (string, bool)                                                                                // 格式化sql模版
	GetLogs(sqlName string, formats []any, st int64, et int64, limit int64, reverse bool, extra map[string]any) (*LogsResult, error)    // 查询原始日志
	SelectLogs(sqlName string, formats []any, st int64, et int64, offset int64, limit int64, extra map[string]any) (*LogsResult, error) // 查询分析日志
}

type Sql struct {
	Name     string `json:"name"`     // SQL名称
	Template string `json:"template"` // 模版
	Project  string `json:"project"`  // 项目
	Store    string `json:"store"`    // 存储/主题
}

func (s *Sql) Format(formats []any) string {
	return fmt.Sprintf(s.Template, formats...)
}

type LogsResult struct {
	Complete bool                `json:"complete"` // 是否完成,如果为false说明查询完成但是结果未返回完全(通常是查询耗时过久或者消耗资源过大或者量级过吧),需要手动拆分查询时间的窗口(短时间多次查询)以再次请求以获取完整结果
	Count    int64               `json:"count"`    // 本次返回数据量
	Logs     []map[string]string `json:"logs"`     // 日志列表
	More     bool                `json:"more"`     // 是否有更多数据
}
