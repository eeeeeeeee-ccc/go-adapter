package log

import (
	"fmt"
	"sync"
)

type BaseClient struct {
	sync.RWMutex
	Sqls map[string]Sql
}

func (b *BaseClient) RegistSql(name string, sql Sql) error {
	b.Lock()
	defer b.Unlock()
	b.Sqls[name] = sql
	return nil
}

func (b *BaseClient) FormatSql(name string, formats []any) (string, bool) {
	b.RLock()
	defer b.RUnlock()
	sql, ok := b.Sqls[name]
	if !ok {
		return "", false
	}
	return sql.Format(formats), true
}

func (b *BaseClient) WithLimit(sql string, offset int64, limit int64) string {
	if limit <= 0 || offset < 0 {
		return sql
	}

	sql += fmt.Sprintf(" limit %d, %d", offset, limit)
	return sql
}
