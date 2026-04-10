package volc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/eeeeeeeee-ccc/go-adapter/cloud"
	"github.com/eeeeeeeee-ccc/go-adapter/cloud/storage"

	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
)

type Client struct {
	instance   *tos.ClientV2
	crcEnabled bool
}

func NewClient(conf *Config, crcEnabled bool, reqTimeout time.Duration) (*Client, error) {
	credentials := tos.WithCredentials(tos.NewStaticCredentials(conf.AccessKeyId, conf.AccessKeySecret))
	instance, err := tos.NewClientV2(conf.Endpoint, tos.WithRegion(conf.Region), credentials, tos.WithEnableCRC(crcEnabled), tos.WithRequestTimeout(reqTimeout))
	if err != nil {
		return nil, err
	}
	return &Client{
		instance:   instance,
		crcEnabled: crcEnabled,
	}, nil
}

func (c *Client) Provider() string {
	return cloud.VolcProvider()
}

func (c *Client) PutObjectFromFile(bucketName, objectName, filePath string, meta map[string]string, traffic int64, extra map[string]any) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	request := &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket:       bucketName,
			Key:          objectName,
			TrafficLimit: traffic,
			Meta:         meta,
		},
		Content: f,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}
	}
	_, err = c.instance.PutObjectV2(context.Background(), request)
	return err
}

func (c *Client) PutObjectFromContent(bucketName, objectName, content string, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket:       bucketName,
			Key:          objectName,
			TrafficLimit: traffic,
			Meta:         meta,
		},
		Content: strings.NewReader(content),
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}
	}

	_, err := c.instance.PutObjectV2(context.Background(), request)
	return err
}

func (c *Client) PutObjectFromBytes(bucketName, objectName string, content []byte, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket:       bucketName,
			Key:          objectName,
			TrafficLimit: traffic,
			Meta:         meta,
		},
		Content: bytes.NewReader(content),
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}
	}

	_, err := c.instance.PutObjectV2(context.Background(), request)
	return err
}

func (c *Client) PutObjectFromReader(bucketName, objectName string, reader io.Reader, meta map[string]string, traffic int64, extra map[string]any) error {
	request := &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket:       bucketName,
			Key:          objectName,
			TrafficLimit: traffic,
			Meta:         meta,
		},
		Content: reader,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}
	}

	_, err := c.instance.PutObjectV2(context.Background(), request)
	return err
}

func (c *Client) ReadObjectSelfMetas(bucketName, objectName string) (map[string]string, bool, error) {
	// 创建HeadObject请求
	headRequest := &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectName,
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObjectV2(context.Background(), headRequest)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
			if serverErr.StatusCode == http.StatusNotFound {
				return nil, false, nil
			}
		}
		return nil, false, err
	}

	out := map[string]string{}
	handle := func(key, value string) bool {
		out[key] = value
		return true
	}
	headResult.Meta.Range(handle)
	return out, true, nil
}

func (c *Client) ReadObjectPosition(bucketName, objectName string) (int64, bool, error) {
	// 创建HeadObject请求
	headRequest := &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectName,
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObjectV2(context.Background(), headRequest)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
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
	headRequest := &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectName,
	}

	// 执行HeadObject请求并处理结果
	headResult, err := c.instance.HeadObjectV2(context.Background(), headRequest)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
			if serverErr.StatusCode == http.StatusNotFound {
				return nil, false, nil
			}
		}
		return nil, false, err
	}

	// 处理元数据
	metadata := make(map[string]string)
	headResult.Meta.Range(func(key, value string) bool {
		metadata[key] = value
		return true
	})

	// 构造HeadInfo返回
	headInfo := &storage.HeadInfo{
		ContentLength:             headResult.ContentLength,
		ContentType:               headResult.ContentType,
		ETag:                      headResult.ETag,
		LastModified:              headResult.LastModified,
		StorageClass:              string(headResult.StorageClass),
		ServerSideEncryption:      headResult.ServerSideEncryption,
		ServerSideEncryptionKeyID: headResult.ServerSideEncryptionKeyID,
		Metadata:                  metadata,
		ObjectType:                headResult.ObjectType,
		Expires:                   headResult.Expires,
		VersionID:                 headResult.VersionID,
		CacheControl:              headResult.CacheControl,
		ContentDisposition:        headResult.ContentDisposition,
		ContentEncoding:           headResult.ContentEncoding,
	}

	return headInfo, true, nil
}

func (c *Client) AppendObjectFromFile(bucketName, objectName string, filePath string, position int64, meta map[string]string, traffic int64, extra map[string]any) (int64, error) {
	// 打开本地文件
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

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

	// 创建追加对象请求
	request := &tos.AppendObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		Offset:       position,
		Content:      f,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _hashCrc64Ecma, ok := extra["hashCrc64Ecma"].(uint64); ok {
			request.PreHashCrc64ecma = _hashCrc64Ecma
		}
	}

	resp, err := c.instance.AppendObjectV2(context.Background(), request)
	if err != nil {
		return 0, err
	}

	if extra != nil && c.crcEnabled {
		extra["hashCrc64Ecma"] = resp.HashCrc64ecma
	}
	return resp.NextAppendOffset, nil
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

	// 创建追加对象请求
	request := &tos.AppendObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		Offset:       position,
		Content:      strings.NewReader(content),
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _hashCrc64Ecma, ok := extra["hashCrc64Ecma"].(uint64); ok {
			request.PreHashCrc64ecma = _hashCrc64Ecma
		}
	}

	resp, err := c.instance.AppendObjectV2(context.Background(), request)
	if err != nil {
		return 0, err
	}

	if extra != nil && c.crcEnabled {
		extra["hashCrc64Ecma"] = resp.HashCrc64ecma
	}
	return resp.NextAppendOffset, nil
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

	// 创建追加对象请求
	request := &tos.AppendObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		Offset:       position,
		Content:      bytes.NewReader(content),
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _hashCrc64Ecma, ok := extra["hashCrc64Ecma"].(uint64); ok {
			request.PreHashCrc64ecma = _hashCrc64Ecma
		}
	}

	resp, err := c.instance.AppendObjectV2(context.Background(), request)
	if err != nil {
		return 0, err
	}

	if extra != nil && c.crcEnabled {
		extra["hashCrc64Ecma"] = resp.HashCrc64ecma
	}
	return resp.NextAppendOffset, nil
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

	// 创建追加对象请求
	request := &tos.AppendObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		Offset:       position,
		Content:      reader,
		TrafficLimit: traffic,
	}

	if extra != nil {
		if _hashCrc64Ecma, ok := extra["hashCrc64Ecma"].(uint64); ok {
			request.PreHashCrc64ecma = _hashCrc64Ecma
		}
	}

	resp, err := c.instance.AppendObjectV2(context.Background(), request)
	if err != nil {
		return 0, err
	}

	if extra != nil && c.crcEnabled {
		extra["hashCrc64Ecma"] = resp.HashCrc64ecma
	}
	return resp.NextAppendOffset, nil
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

	createReq := &tos.CreateMultipartUploadV2Input{
		Bucket: bucketName,
		Key:    objectName,
		Meta:   meta,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			createReq.ACL = enum.ACLType(_acl)
		}
		if _storageClass, ok := extra["storageClass"].(string); ok {
			createReq.StorageClass = enum.StorageClassType(_storageClass)
		}
	}

	createResult, err := c.instance.CreateMultipartUploadV2(context.Background(), createReq)
	if err != nil {
		return err
	}

	// 修正partSize
	if partSize < 1024*1024*4 {
		partSize = 1024 * 1024 * 4
	}

	fileSize := fileInfo.Size()
	partNumber := 1
	parts := make([]tos.UploadedPartV2, 0)
	for offset := int64(0); offset < fileSize; offset += partSize {
		uploadSize := partSize
		if offset+partSize > fileSize {
			uploadSize = fileSize - offset
		}

		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			return err
		}

		partResult, err := c.instance.UploadPartV2(context.Background(), &tos.UploadPartV2Input{
			UploadPartBasicInput: tos.UploadPartBasicInput{
				Bucket:       bucketName,
				Key:          objectName,
				UploadID:     createResult.UploadID,
				PartNumber:   partNumber,
				TrafficLimit: traffic,
			},
			Content:       io.LimitReader(file, uploadSize),
			ContentLength: uploadSize,
		})
		if err != nil {
			return err
		}

		parts = append(parts, tos.UploadedPartV2{
			PartNumber: partNumber,
			ETag:       partResult.ETag,
		})
		partNumber++
	}

	_, err = c.instance.CompleteMultipartUploadV2(context.Background(), &tos.CompleteMultipartUploadV2Input{
		Bucket:   bucketName,
		Key:      objectName,
		UploadID: createResult.UploadID,
		Parts:    parts,
	})
	return err
}

func (c *Client) UploadFileBreakpoint(bucketName, objectName, filePath string, partSize int64, taskNum int, meta map[string]string, traffic int64, extra map[string]any) error {
	// 修正partSize
	if partSize < 1024*1024*5 {
		partSize = 1024 * 1024 * 5
	}

	request := &tos.UploadFileInput{
		CreateMultipartUploadV2Input: tos.CreateMultipartUploadV2Input{
			Bucket: bucketName,
			Key:    objectName,
			Meta:   meta,
		},
		FilePath:         filePath,
		PartSize:         partSize,
		TaskNum:          taskNum,
		EnableCheckpoint: true,
		TrafficLimit:     traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}

		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}

		if checkpointFile, ok := extra["checkpointFile"].(string); ok {
			request.CheckpointFile = checkpointFile
		}
	}

	_, err := c.instance.UploadFile(context.Background(), request)
	return err
}

func (c *Client) GetObjectToFile(bucketName, objectName, filePath string, traffic int64, extra map[string]any) error {
	request := &tos.GetObjectToFileInput{
		GetObjectV2Input: tos.GetObjectV2Input{
			Bucket:       bucketName,
			Key:          objectName,
			TrafficLimit: traffic,
		},
		FilePath: filePath,
	}
	_, err := c.instance.GetObjectToFile(context.Background(), request)
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
	request := &tos.GetObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObjectV2(context.Background(), request)
	if err != nil {
		return nil, err
	}
	defer result.Content.Close()
	return io.ReadAll(result.Content)
}

func (c *Client) GetObjectBytesRange(bucketName, objectName string, start, end int64, traffic int64, extra map[string]any) ([]byte, error) {
	request := &tos.GetObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		RangeStart:   start,
		RangeEnd:     end,
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObjectV2(context.Background(), request)
	if err != nil {
		return nil, err
	}
	defer result.Content.Close()
	return io.ReadAll(result.Content)
}

func (c *Client) GetObjectToWriter(bucketName, objectName string, writer io.Writer, traffic int64, extra map[string]any) error {
	request := &tos.GetObjectV2Input{
		Bucket:       bucketName,
		Key:          objectName,
		TrafficLimit: traffic,
	}
	result, err := c.instance.GetObjectV2(context.Background(), request)
	if err != nil {
		return err
	}
	defer result.Content.Close()

	_, err = io.Copy(writer, result.Content)
	return err
}

func (c *Client) CheckObjectExist(bucketName, objectName string) (bool, error) {
	request := &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectName,
	}
	_, err := c.instance.HeadObjectV2(context.Background(), request)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok && serverErr.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) ListObjects(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	pageSize := int(perPage)
	if pageSize <= 0 {
		pageSize = 1000
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	truncated := true
	continuationToken := ""
	for truncated {
		output, err := c.instance.ListObjectsType2(context.Background(), &tos.ListObjectsType2Input{
			Bucket:            bucketName,
			MaxKeys:           pageSize,
			ContinuationToken: continuationToken,
			Prefix:            prefix,
			StartAfter:        after,
		})
		if err != nil {
			return nil, err
		}
		after = ""
		for _, obj := range output.Contents {
			key := obj.Key
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
		for _, cp := range output.CommonPrefixes {
			key := cp.Prefix
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
		truncated = output.IsTruncated
		continuationToken = output.NextContinuationToken
	}
	return out, nil
}

func (c *Client) ListObjectFiles(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	pageSize := int(perPage)
	if pageSize <= 0 {
		pageSize = 1000
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	truncated := true
	continuationToken := ""
	for truncated {
		output, err := c.instance.ListObjectsType2(context.Background(), &tos.ListObjectsType2Input{
			Bucket:            bucketName,
			MaxKeys:           pageSize,
			ContinuationToken: continuationToken,
			Prefix:            prefix,
			StartAfter:        after,
		})
		if err != nil {
			return nil, err
		}
		after = ""
		for _, obj := range output.Contents {
			key := obj.Key
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
		truncated = output.IsTruncated
		continuationToken = output.NextContinuationToken
	}
	return out, nil
}

func (c *Client) ListObjectDirectories(bucketName string, prefix string, suffix string, after string, limit int32, perPage int32) ([]string, error) {
	if suffix != "" && !strings.HasSuffix(suffix, "/") {
		suffix += "/"
	}

	pageSize := int(perPage)
	if pageSize <= 0 {
		pageSize = 1000
	}

	out := make([]string, 0)
	seen := make(map[string]struct{})
	truncated := true
	continuationToken := ""
	for truncated {
		output, err := c.instance.ListObjectsType2(context.Background(), &tos.ListObjectsType2Input{
			Bucket:            bucketName,
			MaxKeys:           pageSize,
			ContinuationToken: continuationToken,
			Prefix:            prefix,
			StartAfter:        after,
			Delimiter:         "/",
		})
		if err != nil {
			return nil, err
		}
		after = ""
		for _, cp := range output.CommonPrefixes {
			key := cp.Prefix
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
		truncated = output.IsTruncated
		continuationToken = output.NextContinuationToken
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

	request := &tos.CopyObjectInput{
		Bucket:            destBucketName,
		Key:               destObject,
		SrcBucket:         sourceBucketName,
		SrcKey:            sourceObject,
		MetadataDirective: enum.MetadataDirectiveReplace,
		Meta:              mergedMeta,
		TrafficLimit:      traffic,
	}

	if extra != nil {
		if _acl, ok := extra["acl"].(string); ok {
			request.ACL = enum.ACLType(_acl)
		}
		if _storageClass, ok := extra["storageClass"].(string); ok {
			request.StorageClass = enum.StorageClassType(_storageClass)
		}
	}

	_, err = c.instance.CopyObject(context.Background(), request)
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
	request := &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket: bucketName,
			Key:    objectName,
			Meta:   meta,
		},
		Content: nil,
		GenericInput: tos.GenericInput{
			RequestHeader: map[string]string{
				"If-None-Match": "*",
			},
		},
	}

	_, err = c.instance.PutObjectV2(context.Background(), request)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok && serverErr.StatusCode == http.StatusPreconditionFailed {
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
	request := &tos.AppendObjectV2Input{
		Bucket:  bucketName,
		Key:     objectName,
		Offset:  0,
		Content: nil,
		Meta:    meta,
		GenericInput: tos.GenericInput{
			RequestHeader: map[string]string{
				"If-None-Match": "*",
			},
		},
	}

	_, err = c.instance.AppendObjectV2(context.Background(), request)
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok && serverErr.StatusCode == http.StatusPreconditionFailed {
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

	_, err := c.instance.CopyObject(context.Background(), &tos.CopyObjectInput{
		Bucket:            bucketName,
		Key:               objectName,
		SrcBucket:         bucketName,
		SrcKey:            objectName,
		MetadataDirective: enum.MetadataDirectiveReplace,
		Meta:              meta,
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
	request := &tos.DeleteObjectV2Input{
		Bucket: bucketName,
		Key:    objectName,
	}
	_, err := c.instance.DeleteObjectV2(context.Background(), request)
	return err
}

func (c *Client) DeleteObjects(bucketName string, objectNames []string) error {
	if len(objectNames) == 0 {
		return nil
	}

	objects := make([]tos.ObjectTobeDeleted, 0, len(objectNames))
	for _, objectName := range objectNames {
		if objectName == "" {
			continue
		}
		objects = append(objects, tos.ObjectTobeDeleted{
			Key: objectName,
		})
	}
	if len(objects) == 0 {
		return nil
	}

	request := &tos.DeleteMultiObjectsInput{
		Bucket:  bucketName,
		Objects: objects,
		Quiet:   false,
	}
	_, err := c.instance.DeleteMultiObjects(context.Background(), request)
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
