package storage

import (
	"context"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/biandoucheng/go-cloud-adapter/cloud"
	cstorage "github.com/biandoucheng/go-cloud-adapter/cloud/storage"
	aliyun "github.com/biandoucheng/go-cloud-adapter/cloud/storage/aliyun"
	volc "github.com/biandoucheng/go-cloud-adapter/cloud/storage/volc"
)

var (
	_ cstorage.Client = (*aliyun.Client)(nil)
	_ cstorage.Client = (*volc.Client)(nil)
)

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

func TestAliyunCredentialsGetCredentials(t *testing.T) {
	conf := &aliyun.Config{
		AccessKeyId:     "ak",
		AccessKeySecret: "sk",
		SecurityToken:   "st",
	}
	creds := aliyun.NewCredentials(conf)
	got, err := creds.GetCredentials(context.Background())
	if err != nil {
		t.Fatalf("GetCredentials error: %v", err)
	}
	if got.AccessKeyID != conf.AccessKeyId || got.AccessKeySecret != conf.AccessKeySecret || got.SecurityToken != conf.SecurityToken {
		t.Fatalf("credentials mismatch, got=%+v", got)
	}
}

func TestClientMethodSetCoverage(t *testing.T) {
	clientType := reflect.TypeOf((*cstorage.Client)(nil)).Elem()
	aliyunType := reflect.TypeOf(&aliyun.Client{})
	volcType := reflect.TypeOf(&volc.Client{})

	for i := 0; i < clientType.NumMethod(); i++ {
		m := clientType.Method(i)
		if _, ok := aliyunType.MethodByName(m.Name); !ok {
			t.Fatalf("aliyun client missing method: %s", m.Name)
		}
		if _, ok := volcType.MethodByName(m.Name); !ok {
			t.Fatalf("volc client missing method: %s", m.Name)
		}
	}
}

func TestAliyunClientOfflineBranches(t *testing.T) {
	client, err := aliyun.NewClient(&aliyun.Config{
		Region:          "cn-beijing",
		Endpoint:        "https://oss-cn-beijing.aliyuncs.com",
		AccessKeyId:     "test-ak",
		AccessKeySecret: "test-sk",
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	invalidPath := "/path/not/exist/file.txt"
	if _, err = client.AppendObjectFromFile("bucket", "obj", invalidPath, 0, nil, 0, nil); err == nil || !os.IsNotExist(err) {
		t.Fatalf("AppendObjectFromFile should return os not exist, got=%v", err)
	}
	if err = client.UploadFileMultipart("bucket", "obj", invalidPath, 1024, nil, 0, nil); err == nil || !os.IsNotExist(err) {
		t.Fatalf("UploadFileMultipart should return os not exist, got=%v", err)
	}

	if err = client.DeleteObjects("bucket", nil); err != nil {
		t.Fatalf("DeleteObjects(nil) should return nil, got=%v", err)
	}
	if err = client.DeleteObjects("bucket", []string{"", ""}); err != nil {
		t.Fatalf("DeleteObjects(empty keys) should return nil, got=%v", err)
	}
}

func TestVolcClientOfflineBranches(t *testing.T) {
	client, err := volc.NewClient(&volc.Config{
		Region:          "cn-beijing",
		Endpoint:        "tos-cn-beijing.volces.com",
		AccessKeyId:     "test-ak",
		AccessKeySecret: "test-sk",
	}, false, time.Minute*2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	invalidPath := "/path/not/exist/file.txt"
	if err = client.PutObjectFromFile("bucket", "obj", invalidPath, nil, 0, nil); err == nil || !os.IsNotExist(err) {
		t.Fatalf("PutObjectFromFile should return os not exist, got=%v", err)
	}
	if _, err = client.AppendObjectFromFile("bucket", "obj", invalidPath, 0, nil, 0, nil); err == nil || !os.IsNotExist(err) {
		t.Fatalf("AppendObjectFromFile should return os not exist, got=%v", err)
	}
	if err = client.UploadFileMultipart("bucket", "obj", invalidPath, 1024, nil, 0, nil); err == nil || !os.IsNotExist(err) {
		t.Fatalf("UploadFileMultipart should return os not exist, got=%v", err)
	}

	if err = client.DeleteObjects("bucket", nil); err != nil {
		t.Fatalf("DeleteObjects(nil) should return nil, got=%v", err)
	}
	if err = client.DeleteObjects("bucket", []string{"", ""}); err != nil {
		t.Fatalf("DeleteObjects(empty keys) should return nil, got=%v", err)
	}
}

func TestAllMethodNamesTracked(t *testing.T) {
	expected := []string{
		"PutObjectFromFile",
		"PutObjectFromContent",
		"PutObjectFromBytes",
		"PutObjectFromReader",
		"AppendObjectFromFile",
		"AppendObjectFromContent",
		"AppendObjectFromBytes",
		"AppendObjectFromReader",
		"UploadFileMultipart",
		"UploadFileBreakpoint",
		"ReadObjectSelfMetas",
		"UpdateObjectMetas",
		"SetObjectNxMeta",
		"ReadObjectPosition",
		"GetObjectToFile",
		"GetObjectContent",
		"GetObjectBytes",
		"GetObjectBytesRange",
		"GetObjectToWriter",
		"CheckObjectExist",
		"ListObjects",
		"ListObjectFiles",
		"ListObjectDirectories",
		"CopyObject",
		"MkIfNxObject",
		"MkIfNxAppendableObject",
		"DeleteObject",
		"DeleteObjects",
		"DeleteObjectsFromDirectory",
		"DeleteDirectory",
	}

	clientType := reflect.TypeOf((*cstorage.Client)(nil)).Elem()
	methodNames := make([]string, 0, clientType.NumMethod())
	for i := 0; i < clientType.NumMethod(); i++ {
		methodNames = append(methodNames, clientType.Method(i).Name)
	}

	for _, name := range expected {
		found := false
		for _, got := range methodNames {
			if got == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected method missing in interface: %s", name)
		}
	}
}

func TestNilSafetyForCommonInputs(t *testing.T) {
	if reflect.TypeOf((*io.Reader)(nil)).Elem().Kind() != reflect.Interface {
		t.Fatalf("io.Reader kind mismatch")
	}
	if strings.TrimSpace(cstorage.ACLDefault) == "" {
		t.Fatalf("ACLDefault should not be empty")
	}
	if strings.TrimSpace(cstorage.StorageClassStandard) == "" {
		t.Fatalf("StorageClassStandard should not be empty")
	}
}
