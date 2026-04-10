package log

import (
	"testing"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	clog "github.com/eeeeeeeee-ccc/go-adapter/cloud/log"
)

func TestSqlFormat(t *testing.T) {
	sql := clog.Sql{
		Name:     "byLevel",
		Template: "select * from log where level = '%s' and code = %d",
	}

	got := sql.Format([]any{"ERROR", 500})
	want := "select * from log where level = 'ERROR' and code = 500"
	if got != want {
		t.Fatalf("sql format mismatch, want=%q got=%q", want, got)
	}
}

func TestBaseClientRegistAndFormatSql(t *testing.T) {
	base := &clog.BaseClient{
		Sqls: map[string]clog.Sql{},
	}

	err := base.RegistSql("q1", clog.Sql{
		Name:     "q1",
		Template: "select * from log where host = '%s'",
	})
	if err != nil {
		t.Fatalf("RegistSql error: %v", err)
	}

	got, ok := base.FormatSql("q1", []any{"api-01"})
	if !ok {
		t.Fatalf("FormatSql should return ok=true")
	}

	want := "select * from log where host = 'api-01'"
	if got != want {
		t.Fatalf("FormatSql mismatch, want=%q got=%q", want, got)
	}
}

func TestBaseClientFormatSqlNotFound(t *testing.T) {
	base := &clog.BaseClient{
		Sqls: map[string]clog.Sql{},
	}
	got, ok := base.FormatSql("missing", nil)
	if ok {
		t.Fatalf("FormatSql should return ok=false")
	}
	if got != "" {
		t.Fatalf("FormatSql should return empty string for missing sql, got=%q", got)
	}
}

func TestBaseClientWithLimitAndOffset(t *testing.T) {
	base := &clog.BaseClient{}
	sql := "select * from log"

	withLimit := base.WithLimit(sql, 20, 100)
	if withLimit != "select * from log offset 20 limit 100" {
		t.Fatalf("WithLimit mismatch: %q", withLimit)
	}
}

func TestProviderHelpers(t *testing.T) {
	if !cloud.IsAliyunProvider("aliyun") {
		t.Fatalf("IsAliyunProvider should be case-insensitive")
	}
	if cloud.IsAliyunProvider("volc") {
		t.Fatalf("IsAliyunProvider should be false for volc")
	}
	if !cloud.IsVolcProvider("VoLc") {
		t.Fatalf("IsVolcProvider should be case-insensitive")
	}
	if cloud.IsVolcProvider("aliyun") {
		t.Fatalf("IsVolcProvider should be false for aliyun")
	}
}
