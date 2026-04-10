package aliyun

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

type Credentials struct {
	conf *Config
}

func NewCredentials(conf *Config) *Credentials {
	return &Credentials{
		conf: conf,
	}
}

func (c *Credentials) GetCredentials(ctx context.Context) (credentials.Credentials, error) {
	return credentials.Credentials{
		AccessKeyID:     c.conf.AccessKeyId,
		AccessKeySecret: c.conf.AccessKeySecret,
		SecurityToken:   c.conf.SecurityToken,
	}, nil
}
