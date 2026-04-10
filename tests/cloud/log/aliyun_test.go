package log

import (
	"strings"
	"testing"

	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"
	logaliyun "github.com/eeeeeeeee-ccc/go-adapter/cloud/log/aliyun"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerlib "github.com/aliyun/aliyun-log-go-sdk/consumer"
)

var (
	_ clog.Client   = (*logaliyun.Client)(nil)
	_ clog.Producer = (*logaliyun.Producer)(nil)
	_ clog.Consumer = (*logaliyun.Consumer)(nil)
)

func TestAliyunClientQueryGuards(t *testing.T) {
	client := logaliyun.NewClient(&logaliyun.Config{}, "project-a", "store-a")
	if client == nil {
		t.Fatalf("NewClient should not return nil")
	}

	extra := map[string]any{}
	res, err := client.GetLogs("any", nil, 0, 0, 0, false, extra)
	if err != nil {
		t.Fatalf("GetLogs(limit<=0) should return nil err, got=%v", err)
	}
	if !res.Complete || res.More || len(res.Logs) != 0 {
		t.Fatalf("GetLogs(limit<=0) unexpected result: %+v", res)
	}

	res, err = client.SelectLogs("any", nil, 0, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("SelectLogs(limit<=0) should return nil err, got=%v", err)
	}
	if !res.Complete || res.More || len(res.Logs) != 0 {
		t.Fatalf("SelectLogs(limit<=0) unexpected result: %+v", res)
	}

	_, err = client.GetLogs("missing", nil, 0, 0, 10, false, extra)
	if err == nil || !strings.Contains(err.Error(), "sql not found") {
		t.Fatalf("GetLogs should return sql not found, got=%v", err)
	}

	_, err = client.SelectLogs("missing", nil, 0, 0, 0, 10, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "sql not found") {
		t.Fatalf("SelectLogs should return sql not found, got=%v", err)
	}
}

func TestAliyunProducerValidationAndEmptySend(t *testing.T) {
	_, err := logaliyun.NewProducer(&logaliyun.Config{}, "info", "", "")
	if err == nil || !strings.Contains(err.Error(), "project or logstore is empty") {
		t.Fatalf("NewProducer should validate empty project/logstore, got=%v", err)
	}

	p := &logaliyun.Producer{}
	if err = p.SendLog(0, "", map[string]string{}, nil); err != nil {
		t.Fatalf("SendLog(empty content) should return nil, got=%v", err)
	}
	if err = p.SendLogs(0, "", nil, nil); err != nil {
		t.Fatalf("SendLogs(nil contents) should return nil, got=%v", err)
	}
	if err = p.SendLogs(0, "", []map[string]string{{}}, nil); err != nil {
		t.Fatalf("SendLogs(all empty contents) should return nil, got=%v", err)
	}
}

func TestAliyunConsumerInitAndHandler(t *testing.T) {
	c := logaliyun.NewConsumer(&consumerlib.LogHubConfig{})

	called := 0
	received := map[string]string{}
	c.Init(func(ts int64, tns int64, log map[string]string) {
		called++
		for k, v := range log {
			received[k] = v
		}
	})

	k := "level"
	v := "INFO"
	groupList := &sls.LogGroupList{
		LogGroups: []*sls.LogGroup{
			{
				Logs: []*sls.Log{
					{
						Contents: []*sls.LogContent{
							{
								Key:   &k,
								Value: &v,
							},
						},
					},
				},
			},
		},
	}

	ret := c.Handler(0, groupList)
	if ret != "" {
		t.Fatalf("Handler return should be empty string, got=%q", ret)
	}
	if called != 1 {
		t.Fatalf("handler should be called once, got=%d", called)
	}
	if received["level"] != "INFO" {
		t.Fatalf("handler converted log mismatch, got=%v", received)
	}
}
