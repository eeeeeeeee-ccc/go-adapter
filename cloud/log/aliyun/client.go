package aliyun

import (
	"errors"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

type Client struct {
	clog.BaseClient
	instance sls.ClientInterface
	project  string
	store    string
}

func NewClient(conf *Config, project string, store string) *Client {
	return &Client{
		BaseClient: clog.BaseClient{
			Sqls: make(map[string]clog.Sql),
		},
		instance: sls.CreateNormalInterface(conf.Endpoint, conf.AccessKeyId, conf.AccessKeySecret, conf.SecurityToken),
		project:  project,
		store:    store,
	}
}

func (c *Client) Provider() string {
	return cloud.AliyunProvider()
}

func (c *Client) Project() string {
	return c.project
}

func (c *Client) Store() string {
	return c.store
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

	request := &sls.GetLogRequest{
		From:    st,
		To:      et,
		Lines:   limit,
		Reverse: reverse,
		Query:   sql,
	}

	hasOffset := false
	if extra != nil {
		if _powerSql, ok := extra["power_sql"].(bool); ok {
			request.PowerSQL = _powerSql
		}

		if _offset, ok := extra["offset"].(int64); ok && _offset >= 0 {
			hasOffset = true
			request.Offset = _offset
		}
	}

	resp, err := c.instance.GetLogsV2(c.project, c.store, request)
	if err != nil {
		return nil, err
	}

	result := &clog.LogsResult{
		Complete: resp.IsComplete(),
		Count:    int64(len(resp.Logs)),
		Logs:     resp.Logs,
	}
	result.More = resp.Count >= limit

	if result.Logs == nil {
		result.Logs = make([]map[string]string, 0)
	}

	if !resp.IsComplete() {
		err = errors.New("logs not complete")
	} else {
		if hasOffset {
			extra["offset"] = request.Offset + resp.Count
		}
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

	request := &sls.GetLogRequest{
		From:  st,
		To:    et,
		Query: sql,
	}

	if extra != nil {
		if _powerSql, ok := extra["power_sql"].(bool); ok {
			request.PowerSQL = _powerSql
		}

		if _topic, ok := extra["topic"].(string); ok {
			request.Topic = _topic
		}
	}

	resp, err := c.instance.GetLogsV2(c.project, c.store, request)
	if err != nil {
		return nil, err
	}

	result := &clog.LogsResult{
		Complete: resp.IsComplete(),
		Count:    int64(len(resp.Logs)),
		Logs:     resp.Logs,
	}
	result.More = resp.Count >= limit

	if result.Logs == nil {
		result.Logs = make([]map[string]string, 0)
	}

	if !resp.IsComplete() {
		err = errors.New("logs not complete")
	}

	return result, err
}
