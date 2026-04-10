package storage

import "time"

type HeadInfo struct {
	ContentLength             int64             `json:"contentLength,omitempty"`
	ContentType               string            `json:"contentType,omitempty"`
	ETag                      string            `json:"eTag,omitempty"`
	LastModified              time.Time         `json:"lastModified,omitempty"`
	StorageClass              string            `json:"storageClass,omitempty"`
	ContentMD5                string            `json:"contentMD5,omitempty"`
	ServerSideEncryption      string            `json:"serverSideEncryption,omitempty"`
	ServerSideEncryptionKeyID string            `json:"serverSideEncryptionKeyID,omitempty"`
	Metadata                  map[string]string `json:"metadata,omitempty"`
	ObjectType                string            `json:"objectType,omitempty"`
	Expires                   time.Time         `json:"expires,omitempty"`
	VersionID                 string            `json:"versionID,omitempty"`
	CacheControl              string            `json:"cacheControl,omitempty"`
	ContentDisposition        string            `json:"contentDisposition,omitempty"`
	ContentEncoding           string            `json:"contentEncoding,omitempty"`
}
