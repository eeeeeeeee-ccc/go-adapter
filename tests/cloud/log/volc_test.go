package log

import (
	"strings"
	"testing"

	clog "github.com/biandoucheng/go-cloud-adapter/cloud/log"
	logvolc "github.com/biandoucheng/go-cloud-adapter/cloud/log/volc"

	consumerlib "github.com/volcengine/volc-sdk-golang/service/tls/consumer"
	"github.com/volcengine/volc-sdk-golang/service/tls/pb"
)

var (
	_ clog.Client   = (*logvolc.Client)(nil)
	_ clog.Producer = (*logvolc.Producer)(nil)
	_ clog.Consumer = (*logvolc.Consumer)(nil)
)

func TestVolcClientQueryGuards(t *testing.T) {
	client := logvolc.NewClient(&logvolc.Config{}, "project-a", "topic-a")
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

func TestVolcProducerValidationAndEmptySend(t *testing.T) {
	_, err := logvolc.NewProducer(&logvolc.Config{}, "info", "", "")
	if err == nil || !strings.Contains(err.Error(), "topicId is empty") {
		t.Fatalf("NewProducer should validate empty topicId, got=%v", err)
	}

	p := &logvolc.Producer{}
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

func TestVolcConsumerInitAndHandler(t *testing.T) {
	c := logvolc.NewConsumer(&consumerlib.Config{})

	called := 0
	received := map[string]string{}
	c.Init(func(ts int64, tns int64, log map[string]string) {
		called++
		for k, v := range log {
			received[k] = v
		}
	})

	groupList := &pb.LogGroupList{
		LogGroups: []*pb.LogGroup{
			{
				Logs: []*pb.Log{
					{
						Contents: []*pb.LogContent{
							{
								Key:   "level",
								Value: "INFO",
							},
						},
					},
				},
			},
		},
	}

	c.Handler("topic-a", 0, groupList)
	if called != 1 {
		t.Fatalf("handler should be called once, got=%d", called)
	}
	if received["level"] != "INFO" {
		t.Fatalf("handler converted log mismatch, got=%v", received)
	}
}
