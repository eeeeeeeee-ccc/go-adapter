package storage

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	cstorage "github.com/biandoucheng/go-cloud-adapter/cloud/storage"
	aliyun "github.com/biandoucheng/go-cloud-adapter/cloud/storage/aliyun"
)

// 测试用的阿里云存储桶名称和对象基础目录
var (
	AliyunBucketName    = "private-material-center"
	AliyunObjectBaseDir = "github.com/biandoucheng/go-cloud-adapter/unit-test"
)

func NewAliyunClient() (cstorage.Client, error) {
	return aliyun.NewClient(&aliyun.Config{
		Region:          os.Getenv("STORAGE_ALIYUN_REGION"),
		Endpoint:        os.Getenv("STORAGE_ALIYUN_ENDPOINT"),
		AccessKeyId:     os.Getenv("STORAGE_ALIYUN_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("STORAGE_ALIYUN_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("STORAGE_ALIYUN_ACCESS_SECURITY_TOKEN"),
	})
}

// 生成唯一的对象名称
func generateObjectName(prefix string) string {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	return filepath.Join(AliyunObjectBaseDir, fmt.Sprintf("%s-%d.txt", prefix, timestamp))
}

func generateObjectDir(prefix string) string {
	return filepath.Join(AliyunObjectBaseDir, fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()))
}

// 创建临时测试文件
func createTempFile(content ...string) (string, error) {
	file, err := os.CreateTemp("", "test-*.txt")
	if err != nil {
		return "", err
	}
	defer file.Close()

	for i := 0; i < len(content); i++ {
		if _, err := file.WriteString(content[i]); err != nil {
			os.Remove(file.Name())
			return "", err
		}
	}

	return file.Name(), nil
}

// go test -timeout 120s -run ^TestPutObjectFromFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestPutObjectFromFile(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content from file"
	filePath, err := createTempFile(testContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	objectName := generateObjectName("put-from-file")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromFile(AliyunBucketName, objectName, filePath, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromFile failed: %v", err)
	}

	// 验证对象是否存在
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromFile")
	}

	t.Logf("Object %s created successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestPutObjectFromContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestPutObjectFromContent(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("put-from-content")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromContent")
	}

	t.Logf("Object %s created successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestPutObjectFromBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestPutObjectFromBytes(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := []byte("test bytes content")
	objectName := generateObjectName("put-from-bytes")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromBytes(AliyunBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromBytes failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromBytes")
	}

	t.Logf("Object %s created successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestPutObjectFromReader$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestPutObjectFromReader(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test reader content"
	reader := strings.NewReader(testContent)
	objectName := generateObjectName("put-from-reader")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromReader(AliyunBucketName, objectName, reader, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromReader failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromReader")
	}

	t.Logf("Object %s created successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestAppendObjectFromFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestAppendObjectFromFile(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 先创建一个可追加的对象
	extra := map[string]any{}
	initialContent := "initial content"
	objectName := generateObjectName("append-from-file")
	position, err := client.AppendObjectFromContent(AliyunBucketName, objectName, initialContent, -1, nil, 0, extra)
	if err != nil {
		t.Fatalf("AppendObjectFromContent init failed: %v", err)
	}

	// 准备追加内容
	appendContent := " appended content from file"
	filePath, err := createTempFile(appendContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	// 执行追加
	position, err = client.AppendObjectFromFile(AliyunBucketName, objectName, filePath, position, nil, 0, extra)
	if err != nil {
		t.Fatalf("AppendObjectFromFile failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestAppendObjectFromContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestAppendObjectFromContent(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateObjectName("append-from-content")
	position, err := client.AppendObjectFromContent(AliyunBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := " appended content"
	position, err = client.AppendObjectFromContent(AliyunBucketName, objectName, appendContent, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestAppendObjectFromBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestAppendObjectFromBytes(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateObjectName("append-from-bytes")
	position, err := client.AppendObjectFromContent(AliyunBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := []byte(" appended bytes content")
	position, err = client.AppendObjectFromBytes(AliyunBucketName, objectName, appendContent, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromBytes failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + string(appendContent)
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestAppendObjectFromReader$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestAppendObjectFromReader(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateObjectName("append-from-reader")
	position, err := client.AppendObjectFromContent(AliyunBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := " appended reader content"
	reader := strings.NewReader(appendContent)
	position, err = client.AppendObjectFromReader(AliyunBucketName, objectName, reader, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromReader failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestUploadFileMultipart$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestUploadFileMultipart(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建一个较大的测试文件（超过50MB）
	largeContents := make([]string, 0, 50)
	largeContent := strings.Repeat("x", 2*1024*1024) // 2MB
	for i := 0; i < 50; i++ {
		largeContents = append(largeContents, largeContent)
	}
	filePath, err := createTempFile(largeContents...)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	objectName := generateObjectName("multipart-upload")
	meta := map[string]string{"test-key": "test-value"}

	// 执行分片上传
	err = client.UploadFileMultipart(AliyunBucketName, objectName, filePath, 1024*1024, meta, 1024*1024*5, nil)
	if err != nil {
		t.Fatalf("UploadFileMultipart failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after UploadFileMultipart")
	}

	log.Printf("Object %s  multipart uploaded successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestUploadFileBreakpoint$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestUploadFileBreakpoint(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建一个较大的测试文件（超过50MB）
	largeContents := make([]string, 0, 50)
	largeContent := strings.Repeat("x", 2*1024*1024) // 2MB
	for i := 0; i < 50; i++ {
		largeContents = append(largeContents, largeContent)
	}
	filePath, err := createTempFile(largeContents...)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	objectName := generateObjectName("breakpoint-upload")
	meta := map[string]string{"test-key": "test-value"}

	// 执行断点上传
	err = client.UploadFileBreakpoint(AliyunBucketName, objectName, filePath, 2*1024*1024, 2, meta, 5*1024*1024, nil)
	if err != nil {
		t.Fatalf("UploadFileBreakpoint failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after UploadFileBreakpoint")
	}

	log.Printf("Object %s breakpoint uploaded successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestReadObjectSelfMetas$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestReadObjectSelfMetas(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("read-metas")
	meta := map[string]string{"test-key": "test-value", "another-key": "another-value"}

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 读取元数据
	retrievedMeta, exists, err := client.ReadObjectSelfMetas(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("ReadObjectSelfMetas failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}

	// 验证元数据
	if retrievedMeta["test-key"] != "test-value" {
		t.Errorf("Expected meta 'test-key' to be 'test-value', got '%s'", retrievedMeta["test-key"])
	}
	if retrievedMeta["another-key"] != "another-value" {
		t.Errorf("Expected meta 'another-key' to be 'another-value', got '%s'", retrievedMeta["another-key"])
	}

	log.Printf("Object %s metas read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestUpdateObjectMetas$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestUpdateObjectMetas(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("update-metas")
	initialMeta := map[string]string{"test-key": "test-value"}

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, initialMeta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 更新元数据
	updatedMeta := map[string]string{"updated-key": "updated-value"}
	err = client.UpdateObjectMetas(AliyunBucketName, objectName, updatedMeta)
	if err != nil {
		t.Fatalf("UpdateObjectMetas failed: %v", err)
	}

	// 验证更新后的元数据
	retrievedMeta, exists, err := client.ReadObjectSelfMetas(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("ReadObjectSelfMetas failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}
	if retrievedMeta["updated-key"] != "updated-value" {
		t.Errorf("Expected meta 'updated-key' to be 'updated-value', got '%s'", retrievedMeta["updated-key"])
	}

	log.Printf("Object %s metas updated successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestSetObjectNxMeta$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestSetObjectNxMeta(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("set-nx-meta")
	initialMeta := map[string]string{"test-key": "test-value"}

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, initialMeta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 设置条件元数据（失败情况）
	success, _, err := client.SetObjectNxMeta(AliyunBucketName, objectName, "new-key", "new-value", map[string]string{"test-key": "test-value"}, true)
	if err != nil {
		t.Fatalf("SetObjectNxMeta failed: %v", err)
	}
	if success {
		t.Error("SetObjectNxMeta should fail when condition is met")
	}

	// 设置条件元数据成功情况）
	success, _, err = client.SetObjectNxMeta(AliyunBucketName, objectName, "another-key", "another-value", map[string]string{"non-existent-key": "value"}, true)
	if err != nil {
		t.Fatalf("SetObjectNxMeta failed: %v", err)
	}
	if !success {
		t.Error("SetObjectNxMeta should succeed when condition is not met")
	}

	log.Printf("Object %s nx metas set successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestReadObjectPosition$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestReadObjectPosition(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("read-position")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 读取对象位置
	position, exists, err := client.ReadObjectPosition(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("ReadObjectPosition failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}
	if position != int64(len(testContent)) {
		t.Errorf("Expected position %d, got %d", len(testContent), position)
	}

	log.Printf("Object %s position read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestGetObjectToFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestGetObjectToFile(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content for download"
	objectName := generateObjectName("get-to-file")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载到文件
	destPath, err := os.CreateTemp("", "download-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	destPath.Close()
	defer os.Remove(destPath.Name())

	err = client.GetObjectToFile(AliyunBucketName, objectName, destPath.Name(), 0, nil)
	if err != nil {
		t.Fatalf("GetObjectToFile failed: %v", err)
	}

	// 验证内容
	downloadedContent, err := os.ReadFile(destPath.Name())
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if string(downloadedContent) != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, string(downloadedContent))
	}

	log.Printf("Object %s downloaded successfully to %s \n", objectName, destPath.Name())

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestGetObjectContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestGetObjectContent(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("get-content")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if content != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, content)
	}

	log.Printf("Object %s content read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestGetObjectBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestGetObjectBytes(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test bytes"
	objectName := generateObjectName("get-bytes")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载字节
	bytes, err := client.GetObjectBytes(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectBytes failed: %v", err)
	}
	if string(bytes) != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, string(bytes))
	}

	log.Printf("Object %s bytes read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestGetObjectBytesRange$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestGetObjectBytesRange(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "0123456789"
	objectName := generateObjectName("get-bytes-range")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载指定范围（0-4）
	bytes, err := client.GetObjectBytesRange(AliyunBucketName, objectName, 0, 4, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectBytesRange failed: %v", err)
	}
	expectedRange := "01234"
	if string(bytes) != expectedRange {
		t.Errorf("Expected range: %q, got: %q", expectedRange, string(bytes))
	}

	log.Printf("Object %s bytes range read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestGetObjectToWriter$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestGetObjectToWriter(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test writer content"
	objectName := generateObjectName("get-to-writer")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载到Writer
	var buffer bytes.Buffer
	err = client.GetObjectToWriter(AliyunBucketName, objectName, &buffer, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectToWriter failed: %v", err)
	}
	if buffer.String() != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, buffer.String())
	}

	log.Printf("Object %s content read successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestCheckObjectExist$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestCheckObjectExist(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("check-exist")

	// 检查不存在的对象
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist before upload")
	}

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 检查存在的对象
	exists, err = client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after upload")
	}

	log.Printf("Object %s checked successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestListObjects$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestListObjects(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建测试对象前缀
	prefix := generateObjectDir("list-objects") + "/"

	// 创建多个测试对象
	objectNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		objectNames[i] = prefix + "object-" + strconv.Itoa(i)
		err = client.PutObjectFromContent(AliyunBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出对象
	objects, err := client.ListObjects(AliyunBucketName, prefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	// 验证至少有一个对象
	if len(objects) == 0 {
		t.Error("ListObjects should return at least one object")
	}

	for _, obj := range objects {
		log.Printf("Object %s listed successfully \n", obj)
	}

	// 清理
	// for _, objName := range objectNames {
	// 	client.DeleteObject(AliyunBucketName, objName)
	// }
}

// go test -timeout 120s -run ^TestListObjectFiles$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestListObjectFiles(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建测试目录前缀
	dirPrefix := generateObjectDir("list-files") + "/"

	// 创建多个测试文件
	fileNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		fileNames[i] = dirPrefix + "file-" + strconv.Itoa(i)
		err = client.PutObjectFromContent(AliyunBucketName, fileNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出文件
	files, err := client.ListObjectFiles(AliyunBucketName, dirPrefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjectFiles failed: %v", err)
	}

	// 验证至少有一个文件
	if len(files) == 0 {
		t.Error("ListObjectFiles should return at least one file")
	}

	for _, file := range files {
		log.Printf("File %s listed successfully \n", file)
	}

	// 清理
	// for _, fileName := range fileNames {
	// 	client.DeleteObject(AliyunBucketName, fileName)
	// }
}

// go test -timeout 120s -run ^TestListObjectDirectories$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestListObjectDirectories(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建测试目录结构
	basePrefix := generateObjectDir("list-dirs") + "/"
	dirNames := []string{"dir1/", "dir2/", "dir3/"}

	// 创建目录（通过上传目录下的文件）
	for _, dir := range dirNames {
		objName := basePrefix + dir + "file.txt"
		err = client.PutObjectFromContent(AliyunBucketName, objName, "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出目录
	dirs, err := client.ListObjectDirectories(AliyunBucketName, basePrefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjectDirectories failed: %v", err)
	}

	// 验证至少有一个目录
	if len(dirs) == 0 {
		t.Error("ListObjectDirectories should return at least one directory")
	}

	for _, dir := range dirs {
		log.Printf("Directory %s listed successfully \n", dir)
	}

	// 清理
	// for _, dir := range dirNames {
	// 	objName := basePrefix + dir + "file.txt"
	// 	client.DeleteObject(AliyunBucketName, objName)
	// }
}

// go test -timeout 120s -run ^TestCopyObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestCopyObject(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content for copy"
	sourceName := generateObjectName("copy-source")
	destName := generateObjectName("copy-dest")

	// 上传源对象
	err = client.PutObjectFromContent(AliyunBucketName, sourceName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 复制对象
	err = client.CopyObject(AliyunBucketName, sourceName, AliyunBucketName, destName, nil, 0, nil)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// 验证目标对象存在
	exists, err := client.CheckObjectExist(AliyunBucketName, destName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Destination object should exist after copy")
	}

	// 验证内容一致
	destContent, err := client.GetObjectContent(AliyunBucketName, destName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if destContent != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, destContent)
	}

	log.Printf("Object %s copied successfully to %s \n", sourceName, destName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, sourceName)
	// defer client.DeleteObject(AliyunBucketName, destName)
}

// go test -timeout 120s -run ^TestMkIfNxObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestMkIfNxObject(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	objectName := generateObjectName("mk-if-nx")
	meta := map[string]string{"test-key": "test-value"}

	// 创建不存在的对象（应该成功）
	created, err := client.MkIfNxObject(AliyunBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxObject failed: %v", err)
	}
	if !created {
		t.Error("MkIfNxObject should return true when object is created")
	}
	// 再次创建同一对象（应该失败）
	created, err = client.MkIfNxObject(AliyunBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxObject failed: %v", err)
	}

	if created {
		t.Error("MkIfNxObject should fail when object already exists")
	}

	log.Printf("Object %s created if not exist successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestMkIfNxAppendableObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestMkIfNxAppendableObject(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	objectName := generateObjectName("mk-if-nx-appendable")
	meta := map[string]string{"test-key": "test-value"}

	// 创建可追加对象
	created, err := client.MkIfNxAppendableObject(AliyunBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxAppendableObject failed: %v", err)
	}
	if !created {
		t.Error("MkIfNxAppendableObject should return true when object is created")
	}

	// 再次创建同一对象（应该失败）
	created, err = client.MkIfNxAppendableObject(AliyunBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxAppendableObject failed: %v", err)
	}

	if created {
		t.Error("MkIfNxAppendableObject should fail when object already exists")
	}

	// 追加内容
	appendContent := "append content"
	_, err = client.AppendObjectFromContent(AliyunBucketName, objectName, appendContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(AliyunBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if content != appendContent {
		t.Errorf("Expected content: %q, got: %q", appendContent, content)
	}

	log.Printf("Object %s created if not exist successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(AliyunBucketName, objectName)
}

// go test -timeout 120s -run ^TestDeleteObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestDeleteObject(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	testContent := "test content"
	objectName := generateObjectName("delete-object")

	// 上传对象
	err = client.PutObjectFromContent(AliyunBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 删除对象
	err = client.DeleteObject(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// 验证对象不存在
	exists, err := client.CheckObjectExist(AliyunBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist after delete")
	}

	log.Printf("Object %s deleted successfully \n", objectName)
}

// go test -timeout 120s -run ^TestDeleteObjects$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestDeleteObjects(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 上传多个对象
	objectNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		objectNames[i] = generateObjectName("delete-objects-") + strconv.Itoa(i)
		err = client.PutObjectFromContent(AliyunBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 批量删除对象
	err = client.DeleteObjects(AliyunBucketName, objectNames)
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	// 验证所有对象都不存在
	for _, objName := range objectNames {
		exists, err := client.CheckObjectExist(AliyunBucketName, objName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("Object %s should not exist after delete", objName)
		}
	}

	log.Printf("Objects %s deleted successfully \n", objectNames)
}

// go test -timeout 120s -run ^TestDeleteObjectsFromDirectory$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestDeleteObjectsFromDirectory(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建测试目录和文件
	dirName := generateObjectDir("delete-dir-objects") + "/"
	objectNames := make([]string, 3)
	deleteObjectNames := make([]string, 2)
	for i := 0; i < 3; i++ {
		objectNames[i] = dirName + "file-" + strconv.Itoa(i) + ".txt"
		if i < 2 {
			deleteObjectNames[i] = objectNames[i]
		}
		err = client.PutObjectFromContent(AliyunBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 删除目录中的对象（保留目录）
	err = client.DeleteObjectsFromDirectory(AliyunBucketName, dirName)
	if err != nil {
		t.Fatalf("DeleteObjectsFromDirectory failed: %v", err)
	}

	// 验证所有对象都不存在
	for _, objName := range objectNames {
		exists, err := client.CheckObjectExist(AliyunBucketName, objName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("Object %s should not exist after delete", objName)
		}
	}

	log.Printf("Objects %+v deleted successfully \n", objectNames)
}

// go test -timeout 120s -run ^TestDeleteDirectory$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestDeleteDirectory(t *testing.T) {
	client, err := NewAliyunClient()
	if err != nil {
		t.Fatalf("Failed to create aliyun client: %v", err)
	}

	// 创建测试目录和文件
	dirName := generateObjectDir("delete-directory") + "/"
	fileNames := make([]string, 2)
	for i := 0; i < 2; i++ {
		fileNames[i] = dirName + "file-" + strconv.Itoa(i) + ".txt"
		err = client.PutObjectFromContent(AliyunBucketName, fileNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 删除目录（不包括文件）
	success, err := client.DeleteDirectory(AliyunBucketName, dirName, false)
	if err != nil {
		t.Errorf("DeleteDirectory failed: %v", err)
	}
	if success {
		t.Error("Directory should not be deleted")
	}

	// 验证所有文件都存在
	for _, fileName := range fileNames {
		exists, err := client.CheckObjectExist(AliyunBucketName, fileName)
		if err != nil {
			t.Errorf("CheckObjectExist failed: %v", err)
		}
		if !exists {
			t.Errorf("File %s should exist after directory delete", fileName)
		}
	}

	// 删除目录（包括文件）
	success, err = client.DeleteDirectory(AliyunBucketName, dirName, true)
	if err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}

	// 验证所有文件都不存在
	for _, fileName := range fileNames {
		exists, err := client.CheckObjectExist(AliyunBucketName, fileName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("File %s should not exist after directory delete", fileName)
		}
	}

	t.Logf("Directory %s deleted successfully \n", dirName)
}
