package aliyun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/storage"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

type Client struct {
	instance *oss.Client
}

func NewClient(conf *Config) (*Client, error) {
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(NewCredentials(conf)).
		WithRegion(conf.Region)
	client := &Client{}
	client.instance = oss.NewClient(cfg)
	return client, nil
}

func (c *Client) Provider() string {
	return cloud.AliyunProvider()
}

func (c *Client) PutObjectFromFile(bucketName, objectName, filePath string, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	_, err := c.instance.PutObjectFromFile(context.TODO(), request, filePath)
	return err
}

func (c *Client) PutObjectFromContent(bucketName, objectName, content string, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	request.Body = strings.NewReader(content)
	_, err := c.instance.PutObject(context.TODO(), request)
	return err
}

func (c *Client) PutObjectFromBytes(bucketName, objectName string, content []byte, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	request.Body = bytes.NewReader(content)
	_, err := c.instance.PutObject(context.TODO(), request)
	return err
}

func (c *Client) PutObjectFromReader(bucketName, objectName string, reader io.Reader, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	request.Body = reader
	_, err := c.instance.PutObject(context.TODO(), request)
	return err
}

func (c *Client) ReadObjectSelfMetas(bucketName, objectName string) (map[string]string, bool, error) {
	// 创建HeadObject请求
	headRequest := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObject(context.TODO(), headRequest)
	if err != nil {
		var serverErr *oss.ServiceError
		if errors.As(err, &serverErr) {
			if serverErr.StatusCode == http.StatusNotFound {
				return nil, false, nil
			}
		}
		return nil, false, err
	}

	// 返回对象的元数据
	return headResult.Metadata, true, nil
}

func (c *Client) ReadObjectPosition(bucketName, objectName string) (int64, bool, error) {
	// 创建HeadObject请求
	headRequest := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObject(context.TODO(), headRequest)
	if err != nil {
		var serverErr *oss.ServiceError
		if errors.As(err, &serverErr) {
			if serverErr.StatusCode == http.StatusNotFound {
				return 0, false, nil
			}
		}
		return 0, false, err
	}

	// 打印对象的ContentLength，即对象的内容长度
	return headResult.ContentLength, true, nil
}

func (c *Client) ReadObjectHead(bucketName, objectName string) (*storage.HeadInfo, bool, error) {
	// 创建HeadObject请求
	headRequest := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObject(context.TODO(), headRequest)
	if err != nil {
		var serverErr *oss.ServiceError
		if errors.As(err, &serverErr) {
			if serverErr.StatusCode == http.StatusNotFound {
				return nil, false, nil
			}
		}
		return nil, false, err
	}

	// 构造HeadInfo返回
	headInfo := &storage.HeadInfo{
		ContentLength:             headResult.ContentLength,
		ContentType:               oss.ToString(headResult.ContentType),
		ETag:                      oss.ToString(headResult.ETag),
		LastModified:              oss.ToTime(headResult.LastModified),
		StorageClass:              oss.ToString(headResult.StorageClass),
		ContentMD5:                oss.ToString(headResult.ContentMD5),
		ServerSideEncryption:      oss.ToString(headResult.ServerSideEncryption),
		ServerSideEncryptionKeyID: oss.ToString(headResult.ServerSideEncryptionKeyId),
		Metadata:                  headResult.Metadata,
		ObjectType:                oss.ToString(headResult.ObjectType),
		VersionID:                 oss.ToString(headResult.VersionId),
		CacheControl:              oss.ToString(headResult.CacheControl),
		ContentDisposition:        oss.ToString(headResult.ContentDisposition),
		ContentEncoding:           oss.ToString(headResult.ContentEncoding),
	}

	if headResult.Expires != nil && len(*headResult.Expires) > 0 {
		expire, err := time.Parse("", *headResult.Expires)
		if err != nil {
			return nil, true, err
		}
		headInfo.Expires = expire
	}

	return headInfo, true, nil
}

func (c *Client) AppendObjectFromFile(bucketName, objectName string, filePath string, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error) {
	// 打开本地文件
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// 获取位置信息
	if position < 0 {
		_position, has, err := c.ReadObjectPosition(bucketName, objectName)
		if err != nil {
			return 0, err
		}
		if !has {
			position = 0
		} else {
			position = _position
		}
	}

	request := &oss.AppendObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Position:     oss.Ptr(position),
		Metadata:     meta,
		TrafficLimit: traffic,
		Body:         file,
	}

	resp, err := c.instance.AppendObject(context.TODO(), request)
	if err != nil {
		return 0, err
	}
	return resp.NextPosition, nil
}

func (c *Client) AppendObjectFromContent(bucketName, objectName string, content string, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error) {
	// 获取位置信息
	if position < 0 {
		_position, has, err := c.ReadObjectPosition(bucketName, objectName)
		if err != nil {
			return 0, err
		}
		if !has {
			position = 0
		} else {
			position = _position
		}
	}

	request := &oss.AppendObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Position:     oss.Ptr(position),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	request.Body = strings.NewReader(content)
	resp, err := c.instance.AppendObject(context.TODO(), request)
	if err != nil {
		return 0, err
	}

	return resp.NextPosition, nil
}

func (c *Client) AppendObjectFromBytes(bucketName, objectName string, content []byte, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error) {
	// 获取位置信息
	if position < 0 {
		_position, has, err := c.ReadObjectPosition(bucketName, objectName)
		if err != nil {
			return 0, err
		}
		if !has {
			position = 0
		} else {
			position = _position
		}
	}

	request := &oss.AppendObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Position:     oss.Ptr(position),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	request.Body = bytes.NewReader(content)
	resp, err := c.instance.AppendObject(context.TODO(), request)
	if err != nil {
		return 0, err
	}
	return resp.NextPosition, nil
}

func (c *Client) AppendObjectFromReader(bucketName, objectName string, reader io.Reader, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error) {
	// 获取位置信息
	if position < 0 {
		_position, has, err := c.ReadObjectPosition(bucketName, objectName)
		if err != nil {
			return 0, err
		}
		if !has {
			position = 0
		} else {
			position = _position
		}
	}

	request := &oss.AppendObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Position:     oss.Ptr(position),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	request.Body = reader
	resp, err := c.instance.AppendObject(context.TODO(), request)
	if err != nil {
		return 0, err
	}
	return resp.NextPosition, nil
}

func (c *Client) UploadFileMultipart(bucketName, objectName, filePath string, partSize int64, meta map[string]string, traffic int64, extra map[string]any) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	initReq := &oss.InitiateMultipartUploadRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		Metadata: meta,
	}

	if extra != nil {
		if _storageClass, ok := extra["storageClass"].(string); ok {
			initReq.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	initResult, err := c.instance.InitiateMultipartUpload(context.TODO(), initReq)
	if err != nil {
		return err
	}

	// 修正分片大小
	if partSize < 1024*100 {
		partSize = 1024 * 100
	}

	fileSize := fileInfo.Size()
	partNumber := int32(1)
	parts := make([]oss.UploadPart, 0)
	for offset := int64(0); offset < fileSize; offset += partSize {
		currentPartSize := partSize
		if offset+partSize > fileSize {
			currentPartSize = fileSize - offset
		}

		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			return err
		}

		partResult, err := c.instance.UploadPart(context.TODO(), &oss.UploadPartRequest{
			Bucket:        oss.Ptr(bucketName),
			Key:           oss.Ptr(objectName),
			UploadId:      initResult.UploadId,
			PartNumber:    partNumber,
			Body:          io.LimitReader(file, currentPartSize),
			ContentLength: oss.Ptr(currentPartSize),
			TrafficLimit:  traffic,
		})
		if err != nil {
			return err
		}

		parts = append(parts, oss.UploadPart{
			PartNumber: partNumber,
			ETag:       partResult.ETag,
		})
		partNumber++
	}

	completeReq := &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		UploadId: initResult.UploadId,
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: parts,
		},
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			completeReq.Acl = oss.ObjectACLType(_acl)
		}
	}

	_, err = c.instance.CompleteMultipartUpload(context.TODO(), completeReq)
	return err
}

func (c *Client) UploadFileBreakpoint(bucketName, objectName, filePath string, partSize int64, taskNum int, meta map[string]string, traffic int64, extra map[string]any) error {
	// 修正分片大小
	if partSize < 1024*100 {
		partSize = 1024 * 100
	}

	uploader := c.instance.NewUploader(func(uo *oss.UploaderOptions) {
		if partSize > 0 {
			uo.PartSize = partSize
		}
		if taskNum > 0 {
			uo.ParallelNum = taskNum
		}
		uo.EnableCheckpoint = true

		if extra != nil {
			if checkpointDir, ok := extra["checkpointDir"].(string); ok {
				uo.CheckpointDir = checkpointDir
			}
		}
	})

	request := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Metadata:     meta,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}

	}

	_, err := uploader.UploadFile(context.TODO(), request, filePath)
	return err
}

func (c *Client) GetObjectToFile(bucketName, objectName, filePath string, traffic int64, extra map[string]any) error {
	request := &oss.GetObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		TrafficLimit: traffic,
	}
	_, err := c.instance.GetObjectToFile(context.TODO(), request, filePath)
	return err
}

func (c *Client) GetObjectContent(bucketName, objectName string, traffic int64, extra map[string]any) (string, error) {
	content, err := c.GetObjectBytes(bucketName, objectName, traffic, extra)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (c *Client) GetObjectBytes(bucketName, objectName string, traffic int64, extra map[string]any) ([]byte, error) {
	request := &oss.GetObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObject(context.TODO(), request)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	return io.ReadAll(result.Body)
}

func (c *Client) GetObjectBytesRange(bucketName, objectName string, start, end int64, traffic int64, extra map[string]any) ([]byte, error) {
	request := &oss.GetObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		Range:        oss.Ptr(fmt.Sprintf("bytes=%d-%d", start, end)),
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObject(context.TODO(), request)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	return io.ReadAll(result.Body)
}

func (c *Client) GetObjectToWriter(bucketName, objectName string, writer io.Writer, traffic int64, extra map[string]any) error {
	request := &oss.GetObjectRequest{
		Bucket:       oss.Ptr(bucketName),
		Key:          oss.Ptr(objectName),
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObject(context.TODO(), request)
	if err != nil {
		return err
	}
	defer result.Body.Close()

	_, err = io.Copy(writer, result.Body)
	return err
}

func (c *Client) CheckObjectExist(bucketName, objectName string) (bool, error) {
	request := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}
	_, err := c.instance.HeadObject(context.TODO(), request)
	if err != nil {
		var serverErr *oss.ServiceError
		if errors.As(err, &serverErr) {
			if serverErr.StatusCode == http.StatusNotFound {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (c *Client) ListObjects(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	pageSize := perPage
	if pageSize <= 0 {
		pageSize = 1000
	}

	request := &oss.ListObjectsV2Request{
		Bucket:  oss.Ptr(bucketName),
		MaxKeys: pageSize,
	}
	if prefix != "" {
		request.Prefix = oss.Ptr(prefix)
	}
	if after != "" {
		request.StartAfter = oss.Ptr(after)
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	p := c.instance.NewListObjectsV2Paginator(request)
	for p.HasNext() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := oss.ToString(obj.Key)
			if key == "" || (suffix != "" && !strings.HasSuffix(key, suffix)) {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
			if limit > 0 && int32(len(out)) >= limit {
				return out, nil
			}
		}
		for _, cp := range page.CommonPrefixes {
			key := oss.ToString(cp.Prefix)
			if key == "" || (suffix != "" && !strings.HasSuffix(key, suffix)) {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
			if limit > 0 && int32(len(out)) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func (c *Client) ListObjectFiles(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	pageSize := perPage
	if pageSize <= 0 {
		pageSize = 1000
	}

	request := &oss.ListObjectsV2Request{
		Bucket:  oss.Ptr(bucketName),
		MaxKeys: pageSize,
	}
	if prefix != "" {
		request.Prefix = oss.Ptr(prefix)
	}
	if after != "" {
		request.StartAfter = oss.Ptr(after)
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	p := c.instance.NewListObjectsV2Paginator(request)
	for p.HasNext() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := oss.ToString(obj.Key)
			if key == "" || strings.HasSuffix(key, "/") || (suffix != "" && !strings.HasSuffix(key, suffix)) {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
			if limit > 0 && int32(len(out)) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func (c *Client) ListObjectDirectories(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	if suffix != "" && !strings.HasSuffix(suffix, "/") {
		suffix += "/"
	}

	pageSize := perPage
	if pageSize <= 0 {
		pageSize = 1000
	}

	request := &oss.ListObjectsV2Request{
		Bucket:    oss.Ptr(bucketName),
		Delimiter: oss.Ptr("/"),
		MaxKeys:   pageSize,
	}
	if prefix != "" {
		request.Prefix = oss.Ptr(prefix)
	}
	if after != "" {
		request.StartAfter = oss.Ptr(after)
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	p := c.instance.NewListObjectsV2Paginator(request)
	for p.HasNext() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			key := oss.ToString(cp.Prefix)
			if key == "" || (suffix != "" && !strings.HasSuffix(key, suffix)) {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
			if limit > 0 && int32(len(out)) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func (c *Client) CopyObject(sourceBucketName, sourceObject, destBucketName, destObject string, meta map[string]string, traffic int64, extra map[string]any) error {
	oldMeta, _, err := c.ReadObjectSelfMetas(sourceBucketName, sourceObject)
	if err != nil {
		return err
	}

	mergedMeta := make(map[string]string, len(oldMeta)+len(meta))
	for k, v := range oldMeta {
		mergedMeta[k] = v
	}
	for k, v := range meta {
		mergedMeta[k] = v
	}

	request := &oss.CopyObjectRequest{
		Bucket:            oss.Ptr(destBucketName),
		Key:               oss.Ptr(destObject),
		SourceBucket:      oss.Ptr(sourceBucketName),
		SourceKey:         oss.Ptr(sourceObject),
		MetadataDirective: oss.Ptr("REPLACE"),
		Metadata:          mergedMeta,
		TrafficLimit:      traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.Acl = oss.ObjectACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = oss.StorageClassType(_storageClass)
		}
	}

	_, err = c.instance.CopyObject(context.TODO(), request)
	return err
}

func (c *Client) MkIfNxObject(bucketName, objectName string, meta map[string]string, extra map[string]any) (bool, error) {
	// 检查对象是否存在
	exist, err := c.CheckObjectExist(bucketName, objectName)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}

	// 创建
	request := &oss.PutObjectRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		Metadata: meta,
		Body:     nil,
	}
	request.Headers = map[string]string{
		"x-oss-if-none-match": "*",
	}

	_, err = c.instance.PutObject(context.TODO(), request)
	if err != nil {
		if serviceErr, ok := err.(*oss.ServiceError); ok && serviceErr.StatusCode == http.StatusPreconditionFailed {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) MkIfNxAppendableObject(bucketName, objectName string, meta map[string]string, extra map[string]any) (bool, error) {
	// 检查对象是否存在
	exist, err := c.CheckObjectExist(bucketName, objectName)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}

	// 创建
	position := int64(0)
	request := &oss.AppendObjectRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		Position: &position,
		Metadata: meta,
		Body:     nil,
	}
	request.Headers = map[string]string{
		"x-oss-if-none-match": "*",
	}

	_, err = c.instance.AppendObject(context.TODO(), request)
	if err != nil {
		if serviceErr, ok := err.(*oss.ServiceError); ok && serviceErr.StatusCode == http.StatusPreconditionFailed {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) UpdateObjectMetas(bucketName, objectName string, meta map[string]string) error {
	if meta == nil {
		meta = make(map[string]string)
	}

	metaDirective := "REPLACE"
	_, err := c.instance.CopyObject(context.TODO(), &oss.CopyObjectRequest{
		SourceBucket:      oss.Ptr(bucketName),
		SourceKey:         oss.Ptr(objectName),
		Bucket:            oss.Ptr(bucketName),
		Key:               oss.Ptr(objectName),
		Metadata:          meta,
		MetadataDirective: &metaDirective,
	})
	return err
}

func (c *Client) SetObjectNxMeta(bucketName, objectName string, field, val string, except map[string]string, abs bool) (bool, map[string]string, error) {
	meta, _, err := c.ReadObjectSelfMetas(bucketName, objectName)
	if err != nil {
		return false, nil, err
	}
	if meta == nil {
		meta = make(map[string]string)
	}

	if !abs {
		if _, has := meta[field]; has {
			return false, meta, nil
		}
	}

	for k, v := range except {
		if meta[k] == v {
			return false, meta, nil
		}
	}

	meta[field] = val
	if err = c.CopyObject(bucketName, objectName, bucketName, objectName, meta, 0, nil); err != nil {
		return false, nil, err
	}

	resultMeta, _, err := c.ReadObjectSelfMetas(bucketName, objectName)
	if err != nil {
		return false, nil, err
	}
	if resultMeta == nil {
		resultMeta = make(map[string]string)
	}
	return resultMeta[field] == val, resultMeta, nil
}

func (c *Client) DeleteObject(bucketName, objectName string) error {
	request := &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}
	_, err := c.instance.DeleteObject(context.TODO(), request)
	return err
}

func (c *Client) DeleteObjects(bucketName string, objectNames []string) error {
	if len(objectNames) == 0 {
		return nil
	}

	objects := make([]oss.DeleteObject, 0, len(objectNames))
	for _, objectName := range objectNames {
		if objectName == "" {
			continue
		}
		objects = append(objects, oss.DeleteObject{
			Key: oss.Ptr(objectName),
		})
	}
	if len(objects) == 0 {
		return nil
	}

	request := &oss.DeleteMultipleObjectsRequest{
		Bucket: oss.Ptr(bucketName),
		Delete: &oss.Delete{
			Objects: objects,
		},
	}
	_, err := c.instance.DeleteMultipleObjects(context.TODO(), request)
	return err
}

func (c *Client) DeleteObjectsFromDirectory(bucketName, directoryName string) error {
	dirKey := directoryName
	if dirKey != "" && !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}

	files, err := c.ListObjectFiles(bucketName, dirKey, "", "", 0, 1000)
	if err != nil {
		return err
	}
	if err = c.DeleteObjects(bucketName, files); err != nil {
		return err
	}
	return nil
}

func (c *Client) DeleteDirectory(bucketName, directoryName string, clearFile bool) (bool, error) {
	dirKey := directoryName
	if dirKey != "" && !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}

	files, err := c.ListObjectFiles(bucketName, dirKey, "", "", 0, 1000)
	if err != nil {
		return false, err
	}

	if len(files) > 0 && !clearFile {
		return false, nil
	}
	if clearFile {
		err = c.DeleteObjectsFromDirectory(bucketName, dirKey)
		if err != nil {
			return false, err
		}
	}
	if dirKey != "" {
		c.DeleteObject(bucketName, dirKey)
	}
	return true, nil
}
