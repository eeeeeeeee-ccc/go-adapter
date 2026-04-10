package storage

import "io"

type Client interface {
	Provider() string

	PutObjectFromFile(bucketName, objectName, filePath string, meta map[string]string, traffic int64, extra map[string]any) error
	PutObjectFromContent(bucketName, objectName, content string, meta map[string]string, traffic int64, extra map[string]any) error
	PutObjectFromBytes(bucketName, objectName string, content []byte, meta map[string]string, traffic int64, extra map[string]any) error
	PutObjectFromReader(bucketName, objectName string, reader io.Reader, meta map[string]string, traffic int64, extra map[string]any) error

	AppendObjectFromFile(bucketName, objectName string, filePath string, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error)
	AppendObjectFromContent(bucketName, objectName string, content string, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error)
	AppendObjectFromBytes(bucketName, objectName string, content []byte, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error)
	AppendObjectFromReader(bucketName, objectName string, reader io.Reader, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error)

	UploadFileMultipart(bucketName, objectName, filePath string, partSize int64, meta map[string]string, traffic int64, extra map[string]any) error
	UploadFileBreakpoint(bucketName, objectName, filePath string, partSize int64, taskNum int, meta map[string]string, traffic int64, extra map[string]any) error

	ReadObjectHead(bucketName, objectName string) (*HeadInfo, bool, error)
	ReadObjectSelfMetas(bucketName, objectName string) (map[string]string, bool, error)
	UpdateObjectMetas(bucketName, objectName string, meta map[string]string) error
	SetObjectNxMeta(bucketName, objectName string, field, val string, except map[string]string, abs bool) (bool, map[string]string, error)
	ReadObjectPosition(bucketName, objectName string) (int64, bool, error)

	GetObjectToFile(bucketName, objectName, filePath string, traffic int64, extra map[string]any) error
	GetObjectContent(bucketName, objectName string, traffic int64, extra map[string]any) (string, error)
	GetObjectBytes(bucketName, objectName string, traffic int64, extra map[string]any) ([]byte, error)
	GetObjectBytesRange(bucketName, objectName string, start, end int64, traffic int64, extra map[string]any) ([]byte, error)
	GetObjectToWriter(bucketName, objectName string, writer io.Writer, traffic int64, extra map[string]any) error

	CheckObjectExist(bucketName, objectName string) (bool, error)
	ListObjects(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error)
	ListObjectFiles(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error)
	ListObjectDirectories(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error)

	CopyObject(sourceBucketName, sourceObject, destBucketName, destObject string, meta map[string]string, traffic int64, extra map[string]any) error

	MkIfNxObject(bucketName, objectName string, meta map[string]string, extra map[string]any) (bool, error)
	MkIfNxAppendableObject(bucketName, objectName string, meta map[string]string, extra map[string]any) (bool, error)

	DeleteObject(bucketName, objectName string) error
	DeleteObjects(bucketName string, objectNames []string) error
	DeleteObjectsFromDirectory(bucketName, directoryName string) error
	DeleteDirectory(bucketName, directoryName string, clearFile bool) (bool, error)
}
