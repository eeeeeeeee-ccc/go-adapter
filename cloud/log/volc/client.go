package volc

import (
	"fmt"
	"strings"

	"github.com/biandoucheng/go-cloud-adapter/cloud"
	clog "github.com/biandoucheng/go-cloud-adapter/cloud/log"
	"github.com/biandoucheng/go-cloud-adapter/pkg/util"

	"errors"

	"github.com/volcengine/volc-sdk-golang/service/tls"
)

type Client struct {
	clog.BaseClient
	instance  tls.Client
	projectId string
	topicId   string
}

func NewClient(conf *Config, projectId string, topicId string) *Client {
	return &Client{
		BaseClient: clog.BaseClient{
			Sqls: make(map[string]clog.Sql),
		},
		instance:  tls.NewClient(conf.Endpoint, conf.AccessKeyId, conf.AccessKeySecret, conf.SecurityToken, conf.Region),
		projectId: projectId,
		topicId:   topicId,
	}
}

func (c *Client) Provider() string {
	return cloud.VolcProvider()
}

func (c *Client) Project() string {
	return c.projectId
}

func (c *Client) Store() string {
	return c.topicId
}

func (c *Client) GetLogs(sqlName string, formats []any, st int64, et int64, limit int64, reverse bool, extra map[string]any) (*clog.LogsResult, error) {
	if limit <= 0 {
		return &clog.LogsResult{
			Complete: true,
			More:     false,
			Logs:     make([]map[string]string, 0),
		}, nil
	}

	sql, ok := c.FormatSql(sqlName, formats)
	if !ok {
		return nil, errors.New("sql not found")
	}

	request := &tls.SearchLogsRequest{
		TopicID:   c.topicId,
		Query:     sql,
		StartTime: st,
		EndTime:   et,
		Limit:     int(limit),
	}

	if extra != nil {
		if _context, ok := extra["context"].(string); ok {
			request.Context = _context
		}
	}

	if reverse {
		request.Sort = "desc"
	} else {
		request.Sort = "asc"
	}

	resp, err := c.instance.SearchLogs(request)
	if err != nil {
		return nil, err
	}

	result := &clog.LogsResult{
		Complete: strings.EqualFold(resp.Status, "complete"),
		Count:    int64(resp.HitCount),
		Logs:     make([]map[string]string, 0, len(resp.Logs)),
	}
	result.More = result.Count >= limit

	// WARINIG: 火山云的连续查询依赖 context 所以这里会对传入的 extra进行写操作
	// 使用该方法时候需要确保 extra 的写安全
	if extra != nil {
		extra["context"] = resp.Context
	}

	for _, it := range resp.Logs {
		tmp := map[string]string{}

		for k, v := range it {
			vStr, _err := util.AnyToString(v)
			if _err != nil {
				_err = fmt.Errorf("log value convert failed: k:%s, val:%v, err:%v \n", k, v, _err.Error())
				return nil, _err
			}
			tmp[k] = vStr
		}
		result.Logs = append(result.Logs, tmp)
	}

	switch strings.ToLower(resp.Status) {
	case "incomplete":
		err = errors.New("logs not complete")
	case "error":
		err = errors.New("logs search err")
	case "time_out":
		err = errors.New("logs search timeout")
	}

	return result, err
}

func (c *Client) SelectLogs(sqlName string, formats []any, st int64, et int64, offset int64, limit int64, extra map[string]any) (*clog.LogsResult, error) {
	if limit <= 0 {
		return &clog.LogsResult{
			Complete: true,
			More:     false,
			Logs:     make([]map[string]string, 0),
		}, nil
	}

	sql, ok := c.FormatSql(sqlName, formats)
	if !ok {
		return nil, errors.New("sql not found")
	}

	sql = c.WithLimit(sql, offset, limit)

	request := &tls.SearchLogsRequest{
		TopicID:   c.topicId,
		Query:     sql,
		StartTime: st,
		EndTime:   et,
		Limit:     int(limit),
	}

	resp, err := c.instance.SearchLogs(request)
	if err != nil {
		return nil, err
	}

	if resp.AnalysisResult == nil {
		resp.AnalysisResult = &tls.AnalysisResult{
			Schema: []string{},
			Type:   make(map[string]string, 0),
			Data:   make([]map[string]interface{}, 0),
		}
	}

	result := &clog.LogsResult{
		Complete: strings.EqualFold(resp.Status, "complete"),
		Count:    int64(resp.HitCount),
		Logs:     make([]map[string]string, 0, len(resp.AnalysisResult.Data)),
	}
	result.More = result.Count >= limit

	for _, logs := range resp.AnalysisResult.Data {
		tmp := map[string]string{}
		for k, v := range logs {
			vStr, _err := util.AnyToString(v)
			if _err != nil {
				_err = fmt.Errorf("log value convert failed: k:%s, err:%v", k, _err.Error())
				return nil, _err
			}
			tmp[k] = vStr
		}
		result.Logs = append(result.Logs, tmp)
	}

	switch strings.ToLower(resp.Status) {
	case "incomplete":
		err = errors.New("logs not complete")
	case "error":
		err = errors.New("logs search err")
	case "time_out":
		err = errors.New("logs search timeout")
	}

	return result, err
}
