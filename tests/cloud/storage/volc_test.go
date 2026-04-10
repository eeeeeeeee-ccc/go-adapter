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

	cstorage "github.com/eeeeeeeee-ccc/go-adapter/cloud/storage"
	volc "github.com/eeeeeeeee-ccc/go-adapter/cloud/storage/volc"
)

// 测试用的火山引擎存储桶名称和对象基础目录
var (
	VolcBucketName    = "ad-private-bucket"
	VolcObjectBaseDir = "github.com/eeeeeeeee-ccc/go-adapter/unit-test"
)

func NewVolcClient() (cstorage.Client, error) {
	return volc.NewClient(&volc.Config{
		Endpoint:        os.Getenv("STORAGE_VOLC_ENDPOINT"),
		AccessKeyId:     os.Getenv("STORAGE_VOLC_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("STORAGE_VOLC_ACCESS_KEY_SECRET"),
		SecurityToken:   os.Getenv("STORAGE_VOLC_SECURITY_TOKEN"),
		Region:          os.Getenv("STORAGE_VOLC_REGION"),
	}, false, time.Minute*2)
}

// 生成唯一的对象名称
func generateVolcObjectName(prefix string) string {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	return filepath.Join(VolcObjectBaseDir, fmt.Sprintf("%s-%d.txt", prefix, timestamp))
}

func generateVolcDir(prefix string) string {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	return filepath.Join(VolcObjectBaseDir, fmt.Sprintf("%s-%d", prefix, timestamp))
}

// go test -timeout 120s -run ^TestVolcPutObjectFromFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcPutObjectFromFile(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content from file"
	filePath, err := createTempFile(testContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	objectName := generateVolcObjectName("put-from-file")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromFile(VolcBucketName, objectName, filePath, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromFile failed: %v", err)
	}

	// 验证对象是否存在
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromFile")
	}

	log.Printf("Object %s put from file successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcPutObjectFromContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcPutObjectFromContent(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("put-from-content")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromContent")
	}

	log.Printf("Object %s put from content successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcPutObjectFromBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcPutObjectFromBytes(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := []byte("test bytes content")
	objectName := generateVolcObjectName("put-from-bytes")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromBytes(VolcBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromBytes failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromBytes")
	}

	log.Printf("Object %s put from bytes successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcPutObjectFromReader$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcPutObjectFromReader(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test reader content"
	reader := strings.NewReader(testContent)
	objectName := generateVolcObjectName("put-from-reader")
	meta := map[string]string{"test-key": "test-value"}

	// 执行测试
	err = client.PutObjectFromReader(VolcBucketName, objectName, reader, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromReader failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after PutObjectFromReader")
	}

	log.Printf("Object %s put from reader successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcAppendObjectFromFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcAppendObjectFromFile(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 先创建一个可追加的对象
	initialContent := "initial content"
	objectName := generateVolcObjectName("append-from-file")
	position, err := client.AppendObjectFromContent(VolcBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 准备追加内容
	appendContent := " appended content from file"
	filePath, err := createTempFile(appendContent)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(filePath)

	// 执行追加
	position, err = client.AppendObjectFromFile(VolcBucketName, objectName, filePath, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromFile failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended from file successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcAppendObjectFromContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcAppendObjectFromContent(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateVolcObjectName("append-from-content")
	position, err := client.AppendObjectFromContent(VolcBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := " appended content"
	position, err = client.AppendObjectFromContent(VolcBucketName, objectName, appendContent, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended from content successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcAppendObjectFromBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcAppendObjectFromBytes(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateVolcObjectName("append-from-bytes")
	position, err := client.AppendObjectFromContent(VolcBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := []byte(" appended bytes content")
	position, err = client.AppendObjectFromBytes(VolcBucketName, objectName, appendContent, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromBytes failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + string(appendContent)
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended from bytes successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcAppendObjectFromReader$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcAppendObjectFromReader(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 先创建一个对象
	initialContent := "initial content"
	objectName := generateVolcObjectName("append-from-reader")
	position, err := client.AppendObjectFromContent(VolcBucketName, objectName, initialContent, -1, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 执行追加
	appendContent := " appended reader content"
	reader := strings.NewReader(appendContent)
	position, err = client.AppendObjectFromReader(VolcBucketName, objectName, reader, position, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromReader failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	expectedContent := initialContent + appendContent
	if content != expectedContent {
		t.Errorf("Expected content: %q, got: %q", expectedContent, content)
	}

	log.Printf("Object %s appended from reader successfully", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcUploadFileMultipart$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcUploadFileMultipart(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
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

	objectName := generateVolcObjectName("multipart-upload")
	meta := map[string]string{"test-key": "test-value"}

	// 执行分片上传
	err = client.UploadFileMultipart(VolcBucketName, objectName, filePath, 4*1024*1024, meta, 10*1024*1024, nil)
	if err != nil {
		t.Fatalf("UploadFileMultipart failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after UploadFileMultipart")
	}

	log.Printf("Object %s  multipart uploaded successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcUploadFileBreakpoint$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcUploadFileBreakpoint(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
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

	objectName := generateVolcObjectName("breakpoint-upload")
	meta := map[string]string{"test-key": "test-value"}

	// 执行断点上传
	err = client.UploadFileBreakpoint(VolcBucketName, objectName, filePath, 5*1024*1024, 2, meta, 1024*1024*12, nil)
	if err != nil {
		t.Fatalf("UploadFileBreakpoint failed: %v", err)
	}

	// 验证
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after UploadFileBreakpoint")
	}

	log.Printf("Object %s  breakpoint uploaded successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcReadObjectSelfMetas$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcReadObjectSelfMetas(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("read-metas")
	meta := map[string]string{"test-key": "test-value", "another-key": "another-value"}

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, meta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 读取元数据
	retrievedMeta, exists, err := client.ReadObjectSelfMetas(VolcBucketName, objectName)
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

	log.Printf("Object %s  self metas read successfully, retrievedMeta:%+v \n", objectName, retrievedMeta)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcUpdateObjectMetas$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcUpdateObjectMetas(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("update-metas")
	initialMeta := map[string]string{"test-key": "test-value"}

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, initialMeta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 更新元数据
	updatedMeta := map[string]string{"updated-key": "updated-value"}
	err = client.UpdateObjectMetas(VolcBucketName, objectName, updatedMeta)
	if err != nil {
		t.Fatalf("UpdateObjectMetas failed: %v", err)
	}

	// 验证更新后的元数据
	retrievedMeta, exists, err := client.ReadObjectSelfMetas(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("ReadObjectSelfMetas failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}
	if retrievedMeta["updated-key"] != "updated-value" {
		t.Errorf("Expected meta 'updated-key' to be 'updated-value', got '%s'", retrievedMeta["updated-key"])
	}

	log.Printf("Object %s  self metas updated successfully, retrievedMeta:%+v \n", objectName, retrievedMeta)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcSetObjectNxMeta$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcSetObjectNxMeta(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("set-nx-meta")
	initialMeta := map[string]string{"test-key": "test-value"}

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, initialMeta, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 设置条件元数据（失败情况）
	success, _, err := client.SetObjectNxMeta(VolcBucketName, objectName, "new-key", "new-value", map[string]string{"test-key": "test-value"}, true)
	if err != nil {
		t.Fatalf("SetObjectNxMeta failed: %v", err)
	}
	if success {
		t.Error("SetObjectNxMeta should not succeed when condition is met")
	}

	// 设置条件元数据（成功情况）
	success, _, err = client.SetObjectNxMeta(VolcBucketName, objectName, "another-key", "another-value", map[string]string{"non-existent-key": "value"}, true)
	if err != nil {
		t.Fatalf("SetObjectNxMeta failed: %v", err)
	}
	if !success {
		t.Error("SetObjectNxMeta should succeed when condition is not met")
	}

	log.Printf("Object %s  nx meta set successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcReadObjectPosition$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcReadObjectPosition(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("read-position")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 读取对象位置
	position, exists, err := client.ReadObjectPosition(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("ReadObjectPosition failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}
	if position != int64(len(testContent)) {
		t.Errorf("Expected position %d, got %d", len(testContent), position)
	}

	log.Printf("Object %s  position read successfully, position:%d \n", objectName, position)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcGetObjectToFile$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcGetObjectToFile(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content for download"
	objectName := generateVolcObjectName("get-to-file")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
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

	err = client.GetObjectToFile(VolcBucketName, objectName, destPath.Name(), 0, nil)
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

	log.Printf("Object %s  downloaded to file successfully, destPath:%s \n", objectName, destPath.Name())

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcGetObjectContent$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcGetObjectContent(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("get-content")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if content != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, content)
	}

	log.Printf("Object %s  content read successfully, content:%s \n", objectName, content)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcGetObjectBytes$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcGetObjectBytes(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test bytes"
	objectName := generateVolcObjectName("get-bytes")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载字节
	bytes, err := client.GetObjectBytes(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectBytes failed: %v", err)
	}
	if string(bytes) != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, string(bytes))
	}

	log.Printf("Object %s  bytes read successfully, bytes:%s \n", objectName, string(bytes))

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcGetObjectBytesRange$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcGetObjectBytesRange(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "0123456789"
	objectName := generateVolcObjectName("get-bytes-range")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载指定范围（0-4）
	bytes, err := client.GetObjectBytesRange(VolcBucketName, objectName, 0, 4, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectBytesRange failed: %v", err)
	}
	expectedRange := "01234"
	if string(bytes) != expectedRange {
		t.Errorf("Expected range: %q, got: %q", expectedRange, string(bytes))
	}

	log.Printf("Object %s  bytes range read successfully, bytes:%s \n", objectName, string(bytes))

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcGetObjectToWriter$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcGetObjectToWriter(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test writer content"
	objectName := generateVolcObjectName("get-to-writer")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 下载到Writer
	var buffer bytes.Buffer
	err = client.GetObjectToWriter(VolcBucketName, objectName, &buffer, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectToWriter failed: %v", err)
	}
	if buffer.String() != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, buffer.String())
	}

	log.Printf("Object %s  content read successfully, content:%s \n", objectName, buffer.String())

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcCheckObjectExist$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcCheckObjectExist(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("check-exist")

	// 检查不存在的对象
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist before upload")
	}

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 检查存在的对象
	exists, err = client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist after upload")
	}

	log.Printf("Object %s  checked exist successfully, exists:%v \n", objectName, exists)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcListObjects$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcListObjects(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 创建测试对象前缀
	prefix := generateVolcDir("list-objects") + "/"

	// 创建多个测试对象
	objectNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		objectNames[i] = prefix + "object-" + strconv.Itoa(i) + ".txt"
		err = client.PutObjectFromContent(VolcBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出对象
	objects, err := client.ListObjects(VolcBucketName, prefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	// 验证至少有一个对象
	if len(objects) == 0 {
		t.Error("ListObjects should return at least one object")
	}

	for _, objName := range objects {
		log.Printf("Object %s  listed successfully \n", objName)
	}

	// 清理
	// for _, objName := range objectNames {
	// 	client.DeleteObject(VolcBucketName, objName)
	// }
}

// go test -timeout 120s -run ^TestVolcListObjectFiles$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcListObjectFiles(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 创建测试目录前缀
	dirPrefix := generateVolcDir("list-files") + "/"

	// 创建多个测试文件
	fileNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		fileNames[i] = dirPrefix + "file-" + strconv.Itoa(i) + ".txt"
		err = client.PutObjectFromContent(VolcBucketName, fileNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出文件
	files, err := client.ListObjectFiles(VolcBucketName, dirPrefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjectFiles failed: %v", err)
	}

	// 验证至少有一个文件
	if len(files) == 0 {
		t.Error("ListObjectFiles should return at least one file")
	}

	for _, fileName := range files {
		log.Printf("File %s  listed successfully \n", fileName)
	}

	// 清理
	// for _, fileName := range fileNames {
	// 	client.DeleteObject(VolcBucketName, fileName)
	// }
}

// go test -timeout 120s -run ^TestVolcListObjectDirectories$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcListObjectDirectories(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 创建测试目录结构
	basePrefix := generateVolcDir("list-dirs") + "/"
	dirNames := []string{"dir1/", "dir2/", "dir3/"}

	// 创建目录（通过上传目录下的文件）
	for _, dir := range dirNames {
		objName := basePrefix + dir + "file.txt"
		err = client.PutObjectFromContent(VolcBucketName, objName, "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 列出目录
	dirs, err := client.ListObjectDirectories(VolcBucketName, basePrefix, "", "", 10, 10)
	if err != nil {
		t.Fatalf("ListObjectDirectories failed: %v", err)
	}

	// 验证至少有一个目录
	if len(dirs) == 0 {
		t.Error("ListObjectDirectories should return at least one directory")
	}

	for _, dir := range dirs {
		log.Printf("Directory %s  listed successfully \n", dir)
	}

	// 清理
	// for _, dir := range dirNames {
	// 	objName := basePrefix + dir + "file.txt"
	// 	client.DeleteObject(VolcBucketName, objName)
	// }
}

// go test -timeout 120s -run ^TestVolcCopyObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcCopyObject(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content for copy"
	sourceName := generateVolcObjectName("copy-source")
	destName := generateVolcObjectName("copy-dest")

	// 上传源对象
	err = client.PutObjectFromContent(VolcBucketName, sourceName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 复制对象
	err = client.CopyObject(VolcBucketName, sourceName, VolcBucketName, destName, nil, 0, nil)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// 验证目标对象存在
	exists, err := client.CheckObjectExist(VolcBucketName, destName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if !exists {
		t.Error("Destination object should exist after copy")
	}

	// 验证内容一致
	destContent, err := client.GetObjectContent(VolcBucketName, destName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if destContent != testContent {
		t.Errorf("Expected content: %q, got: %q", testContent, destContent)
	}

	log.Printf("Object %s  copied successfully, destName:%s \n", sourceName, destName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, sourceName)
	// defer client.DeleteObject(VolcBucketName, destName)
}

// go test -timeout 120s -run ^TestVolcMkIfNxObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcMkIfNxObject(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	objectName := generateVolcObjectName("mk-if-nx")
	meta := map[string]string{"test-key": "test-value"}

	// 创建不存在的对象（应该成功）
	created, err := client.MkIfNxObject(VolcBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxObject failed: %v", err)
	}
	if !created {
		t.Error("MkIfNxObject should return true when object is created")
	}

	// 再次创建同一对象（应该失败）
	created, err = client.MkIfNxObject(VolcBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxObject failed: %v", err)
	}
	if created {
		t.Error("MkIfNxObject should fail when object already exists")
	}

	log.Printf("Object %s  mk-if-nx successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcMkIfNxAppendableObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcMkIfNxAppendableObject(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	objectName := generateVolcObjectName("mk-if-nx-appendable")
	meta := map[string]string{"test-key": "test-value"}

	// 创建可追加对象
	created, err := client.MkIfNxAppendableObject(VolcBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxAppendableObject failed: %v", err)
	}
	if !created {
		t.Error("MkIfNxAppendableObject should return true when object is created")
	}

	// 再次创建同一对象（应该失败）
	created, err = client.MkIfNxAppendableObject(VolcBucketName, objectName, meta, nil)
	if err != nil {
		t.Fatalf("MkIfNxAppendableObject failed: %v", err)
	}

	if created {
		t.Error("MkIfNxAppendableObject should return false when object already exists")
	}

	// 追加内容
	appendContent := "append content"
	_, err = client.AppendObjectFromContent(VolcBucketName, objectName, appendContent, 0, nil, 0, nil)
	if err != nil {
		t.Fatalf("AppendObjectFromContent failed: %v", err)
	}

	// 验证内容
	content, err := client.GetObjectContent(VolcBucketName, objectName, 0, nil)
	if err != nil {
		t.Fatalf("GetObjectContent failed: %v", err)
	}
	if content != appendContent {
		t.Errorf("Expected content: %q, got: %q", appendContent, content)
	}

	log.Printf("Object %s  mk-if-nx-appendable successfully \n", objectName)

	// 清理
	// defer client.DeleteObject(VolcBucketName, objectName)
}

// go test -timeout 120s -run ^TestVolcDeleteObject$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcDeleteObject(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	testContent := "test content"
	objectName := generateVolcObjectName("delete-object")

	// 上传对象
	err = client.PutObjectFromContent(VolcBucketName, objectName, testContent, nil, 0, nil)
	if err != nil {
		t.Fatalf("PutObjectFromContent failed: %v", err)
	}

	// 删除对象
	err = client.DeleteObject(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// 验证对象不存在
	exists, err := client.CheckObjectExist(VolcBucketName, objectName)
	if err != nil {
		t.Fatalf("CheckObjectExist failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist after delete")
	}

	log.Printf("Object %s  deleted successfully \n", objectName)
}

// go test -timeout 120s -run ^TestVolcDeleteObjects$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcDeleteObjects(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 上传多个对象
	objectNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		objectNames[i] = generateVolcObjectName("delete-objects-") + strconv.Itoa(i)
		err = client.PutObjectFromContent(VolcBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 批量删除对象
	err = client.DeleteObjects(VolcBucketName, objectNames)
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	// 验证所有对象都不存在
	for _, objName := range objectNames {
		exists, err := client.CheckObjectExist(VolcBucketName, objName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("Object %s should not exist after delete", objName)
		}
	}

	log.Printf("Objects %s  deleted successfully \n", strings.Join(objectNames, ", "))
}

// go test -timeout 120s -run ^TestVolcDeleteObjectsFromDirectory$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcDeleteObjectsFromDirectory(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 创建测试目录和文件
	dirName := generateVolcDir("delete-dir-objects") + "/"
	objectNames := make([]string, 3)
	for i := 0; i < 3; i++ {
		objectNames[i] = dirName + "file-" + strconv.Itoa(i)
		err = client.PutObjectFromContent(VolcBucketName, objectNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 删除目录中的对象（保留目录）
	err = client.DeleteObjectsFromDirectory(VolcBucketName, dirName)
	if err != nil {
		t.Fatalf("DeleteObjectsFromDirectory failed: %v", err)
	}

	// 验证所有对象都不存在
	for _, objName := range objectNames {
		exists, err := client.CheckObjectExist(VolcBucketName, objName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("Object %s should not exist after delete", objName)
		}
	}

	log.Printf("DeleteObjectsFromDirectory successful, dirName:%s \n", dirName)
}

// go test -timeout 120s -run ^TestVolcDeleteDirectory$ go-cloud-adapter/tests/cloud/storage  -count=1 -v
// PASS
func TestVolcDeleteDirectory(t *testing.T) {
	client, err := NewVolcClient()
	if err != nil {
		t.Fatalf("Failed to create volc client: %v", err)
	}

	// 创建测试目录和文件
	dirName := generateObjectDir("delete-directory") + "/"
	fileNames := make([]string, 2)
	for i := 0; i < 2; i++ {
		fileNames[i] = dirName + "file-" + strconv.Itoa(i)
		err = client.PutObjectFromContent(VolcBucketName, fileNames[i], "test content", nil, 0, nil)
		if err != nil {
			t.Fatalf("PutObjectFromContent failed: %v", err)
		}
	}

	// 删除目录（不包括文件）
	success, err := client.DeleteDirectory(VolcBucketName, dirName, false)
	if err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	if success {
		t.Error("Directory should not be deleted")
	}

	// 验证所有文件都存在
	for _, fileName := range fileNames {
		exists, err := client.CheckObjectExist(VolcBucketName, fileName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if !exists {
			t.Errorf("File %s should exist after directory delete", fileName)
		}
	}

	// 删除目录（包括文件）
	success, err = client.DeleteDirectory(VolcBucketName, dirName, true)
	if err != nil {
		t.Fatalf("DeleteDirectory failed: %v", err)
	}
	if !success {
		t.Error("Directory should be deleted")
	}

	// 验证所有文件都不存在
	for _, fileName := range fileNames {
		exists, err := client.CheckObjectExist(VolcBucketName, fileName)
		if err != nil {
			t.Fatalf("CheckObjectExist failed: %v", err)
		}
		if exists {
			t.Errorf("File %s should not exist after directory delete", fileName)
		}
	}

	t.Logf("Directory %s deleted successfully", dirName)
}
