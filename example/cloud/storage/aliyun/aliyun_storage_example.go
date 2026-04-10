package aliyun

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

// 定义全局变量
var (
	region     string // 存储区域
	bucketName string // 存储空间名称
	objectName string // 对象名称
)

// init函数用于初始化命令行参数
func init() {
	flag.StringVar(&region, "region", "", "The region in which the bucket is located.")
	flag.StringVar(&bucketName, "bucket", "", "The name of the bucket.")
	flag.StringVar(&objectName, "object", "", "The name of the object.")
	// 解析命令行参数
	flag.Parse()
}

// 普通上传
func PutObjectFromFileExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 填写要上传的本地文件路径和文件名称，例如 /Users/localpath/exampleobject.txt
	localFile := "/Users/localpath/exampleobject.txt"

	// 创建上传对象的请求
	putRequest := &oss.PutObjectRequest{
		Bucket:       oss.Ptr(bucketName),      // 存储空间名称
		Key:          oss.Ptr(objectName),      // 对象名称
		StorageClass: oss.StorageClassStandard, // 指定对象的存储类型为标准存储
		Acl:          oss.ObjectACLPrivate,     // 指定对象的访问权限为私有访问
		Metadata: map[string]string{
			"yourMetadataKey1": "yourMetadataValue1", // 设置对象的元数据
		},
	}

	// 执行上传对象的请求
	result, err := client.PutObjectFromFile(context.TODO(), putRequest, localFile)
	if err != nil {
		log.Fatalf("failed to put object from file %v", err)
	}

	// 打印上传对象的结果
	log.Printf("put object from file result:%#v\n", result)
}

// 读取对象内容长度
func ReadObjectPosition() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 创建HeadObject请求
	headRequest := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}

	// 执行HeadObject请求并处理结果
	headResult, err := client.HeadObject(context.TODO(), headRequest)
	if err != nil {
		log.Fatalf("failed to head object %v", err)
	}

	// 打印对象的ContentLength，即对象的内容长度
	log.Printf("object %s position %d .\n", objectName, headResult.ContentLength)
}

// 追加上传
func AppendObjectExample() {
	// 加载默认配置并设置凭证提供者和region
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 定义要追加的内容
	content := "hi append object"

	position := int64(0)

	// 创建AppendObject请求
	request := &oss.AppendObjectRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		Position: oss.Ptr(position),
		Body:     strings.NewReader(content),
	}

	// 执行AppendObject请求并处理结果
	// 第一次追加上传的位置是0，返回值中包含下一次追加的位置
	result, err := client.AppendObject(context.TODO(), request)
	if err != nil {
		log.Fatalf("failed to append object %v", err)
	}

	// 创建第二次AppendObject请求
	request = &oss.AppendObjectRequest{
		Bucket:   oss.Ptr(bucketName),
		Key:      oss.Ptr(objectName),
		Position: oss.Ptr(result.NextPosition), //从第一次AppendObject返回值中获取NextPosition
		Body:     strings.NewReader("hi append object"),
	}

	// 执行第二次AppendObject请求并处理结果
	result, err = client.AppendObject(context.TODO(), request)
	if err != nil {
		log.Fatalf("failed to append object %v", err)
	}

	log.Printf("append object result:%#v\n", result)
}

// 分片上传
func MultipartPutObjectExample() {
	// 从环境变量获取访问凭证
	// 配置OSS客户端，设置凭证提供者和Endpoint
	config := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion("cn-hangzhou").
		WithEndpoint("oss-cn-hangzhou.aliyuncs.com")

	// 初始化OSS客户端
	client := oss.NewClient(config)

	// 配置Bucket和文件信息
	bucket := "example-bucket"
	key := "dest.jpg"
	filePath := "dest.jpg"

	// 步骤1：初始化分片上传
	initResult, err := client.InitiateMultipartUpload(context.TODO(), &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}

	uploadId := *initResult.UploadId
	fmt.Printf("初始化分片上传成功，上传ID: %s\n", uploadId)

	// 步骤2：上传分片
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fileSize := fileInfo.Size()
	partSize := int64(100 * 1024) // 每个分片100KB
	partNumber := int32(1)
	var parts []oss.UploadPart

	for offset := int64(0); offset < fileSize; offset += partSize {
		// 计算当前分片大小
		currentPartSize := partSize
		if offset+partSize > fileSize {
			currentPartSize = fileSize - offset
		}

		// 读取分片数据
		file.Seek(offset, 0)
		partData := io.LimitReader(file, currentPartSize)

		// 上传分片
		partResult, err := client.UploadPart(context.TODO(), &oss.UploadPartRequest{
			Bucket:     oss.Ptr(bucket),
			Key:        oss.Ptr(key),
			UploadId:   oss.Ptr(uploadId),
			PartNumber: partNumber,
			Body:       partData,
		})
		if err != nil {
			fmt.Printf("错误: %v\n", err)
			return
		}

		fmt.Printf("分片号: %d, ETag: %s\n", partNumber, *partResult.ETag)

		// 记录已上传的分片信息
		parts = append(parts, oss.UploadPart{
			PartNumber: partNumber,
			ETag:       partResult.ETag,
		})

		partNumber++
	}

	// 步骤3：完成分片上传
	completeResult, err := client.CompleteMultipartUpload(context.TODO(), &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(bucket),
		Key:      oss.Ptr(key),
		UploadId: oss.Ptr(uploadId),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fmt.Printf("完成分片上传，Bucket: %s, Key: %s, Location: %s, ETag: %s\n",
		*completeResult.Bucket, *completeResult.Key, *completeResult.Location, *completeResult.ETag)
}

// 断点续传
func UploadFileExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 创建上传器，并启用断点续传功能
	u := client.NewUploader(func(uo *oss.UploaderOptions) {
		uo.CheckpointDir = "/Users/yourLocalPath/checkpoint/" // 指定断点记录文件的保存路径
		uo.ParallelNum = 3
		uo.EnableCheckpoint = true // 开启断点续传
	})

	// 定义本地文件路径，需要替换为您的实际本地文件路径和文件名称
	localFile := "/Users/yourLocalPath/yourFileName"

	// 执行上传文件的操作
	result, err := u.UploadFile(context.TODO(),
		&oss.PutObjectRequest{
			Bucket: oss.Ptr(bucketName),
			Key:    oss.Ptr(objectName)},
		localFile)
	if err != nil {
		log.Fatalf("failed to upload file %v", err)
	}

	// 打印上传文件的结果
	log.Printf("upload file result:%#v\n", result)
}

// 下载对象到内存
func GetObjectToMemoryExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 读取文件
	request := oss.GetObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}
	resp, err := client.GetObject(context.Background(), &request)
	if err != nil {
		log.Fatalf("failed to get object %v", err)
	}

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("failed to read response body %v", err)
	}
	defer resp.Body.Close()

	log.Printf("get object body:%s\n", string(body))
}

// 下载到文件
func GetObjectToFileExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 指定本地文件路径
	localFile := "download.file"

	// 假设Object最后修改时间为2024年10月21日18:43:02，则填写的UTC早于该时间时，将满足IfModifiedSince的限定条件，并触发下载行为。
	date := time.Date(2024, time.October, 21, 18, 43, 2, 0, time.UTC)

	// 假设ETag为e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855，则填写的ETag与Object的ETag值相等时，将满足IfMatch的限定条件，并触发下载行为。
	etag := "\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\""

	// 创建下载对象到本地文件的请求
	getRequest := &oss.GetObjectRequest{
		Bucket:          oss.Ptr(bucketName),                   // 存储空间名称
		Key:             oss.Ptr(objectName),                   // 对象名称
		IfModifiedSince: oss.Ptr(date.Format(http.TimeFormat)), // 指定IfModifiedSince参数
		IfMatch:         oss.Ptr(etag),                         // 指定IfMatch参数
	}

	// 执行下载对象到本地文件的操作并处理结果
	result, err := client.GetObjectToFile(context.TODO(), getRequest, localFile)
	if err != nil {
		log.Fatalf("failed to get object to file %v", err)
	}

	log.Printf("get object to file result:%#v\n", result)
}

// 指定范围下载
func GetObjectByRangeExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 设置要下载的字节范围，格式为 bytes=start-end
	// 例如：下载前100字节：bytes=0-99
	// 下载从100字节开始到末尾：bytes=100-
	// 下载最后100字节：bytes=-100
	startByte := int64(0)
	endByte := int64(99)
	rangeStr := fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	// 创建下载对象的请求，指定Range参数
	getRequest := &oss.GetObjectRequest{
		Bucket: oss.Ptr(bucketName), // 存储空间名称
		Key:    oss.Ptr(objectName), // 对象名称
		Range:  oss.Ptr(rangeStr),   // 指定下载的字节范围
	}

	// 执行下载对象操作并处理结果
	result, err := client.GetObject(context.TODO(), getRequest)
	if err != nil {
		log.Fatalf("failed to get object %v", err)
	}
	defer result.Body.Close()

	// 读取下载的内容
	data, err := io.ReadAll(result.Body)
	if err != nil {
		log.Fatalf("failed to read object content %v", err)
	}

	// 打印下载结果
	log.Printf("get object by range result, status code: %d", result.StatusCode)
	log.Printf("content length: %d", len(data))
	log.Printf("content range: %s", oss.ToString(result.ContentRange))
	log.Printf("content: %s", string(data))
}

// 拷贝对象
func CopyObjectExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	destBucketName := "<yourDestBucketName>"
	destObjectName := "<yourDestObjectName>"
	srcBucketName := "<yourSrcBucketName>"
	srcObjectName := "<yourSrcObjectName>"

	// 创建复制对象请求
	request := &oss.CopyObjectRequest{
		Bucket:       oss.Ptr(destBucketName), // 目标存储空间名称
		Key:          oss.Ptr(destObjectName), // 目标对象名称
		SourceKey:    oss.Ptr(srcObjectName),  // 源对象名称
		SourceBucket: oss.Ptr(srcBucketName),  // 源存储空间名称
	}

	// 执行复制对象操作并处理结果
	result, err := client.CopyObject(context.TODO(), request)
	if err != nil {
		log.Fatalf("failed to copy object %v", err)
	}
	log.Printf("copy object result:%#v\n", result)
}

// 列出对象
/*
在 prefix 的基础上，增加 delimiter 参数（通常设为 /）。这会将返回结果分为 文件（objects） 和 子目录（commonPrefixes） 两部分。

delimiter的作用是按层级分组。当它存在时，OSS会对prefix筛选后的结果进行二次处理：

如果文件名在prefix之后不再包含/，则被视为文件。

如果文件名在prefix之后还包含/，则从开头到第一个/的部分会被视为一个子目录。
*/
func ListObjectsExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	// 创建列出对象的请求
	request := &oss.ListObjectsV2Request{
		Bucket: oss.Ptr(bucketName),
	}

	// 创建分页器
	p := client.NewListObjectsV2Paginator(request)

	// 初始化页码计数器
	var i int
	log.Println("Objects:")

	// 遍历分页器中的每一页
	for p.HasNext() {
		i++

		// 获取下一页的数据
		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		// 打印该页中的每个对象的信息
		for _, obj := range page.Contents {
			log.Printf("Object:%v, %v, %v\n", oss.ToString(obj.Key), obj.Size, oss.ToTime(obj.LastModified))
		}
	}
}

// 判断对象存在
func CheckObjectExistsExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	request := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}
	_, err := client.HeadObject(context.TODO(), request)
	if err != nil {
		var srvErr *oss.ServiceError
		if errors.As(err, &srvErr) {
			if srvErr.StatusCode == 404 {
				log.Println("object not exists")
			}
		}
	} else {
		log.Println("object exists")
	}
}

// 获取元信息
func GetObjectMetaExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	request := &oss.HeadObjectRequest{
		Bucket: oss.Ptr(bucketName),
		Key:    oss.Ptr(objectName),
	}
	resp, err := client.HeadObject(context.TODO(), request)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}

	fmt.Printf("object meta:%#v\n", resp.Metadata)
}

// 删除对象
func DeleteObjectExample() {
	// 加载默认配置并设置凭证提供者和区域
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(region)

	// 创建OSS客户端
	client := oss.NewClient(cfg)

	objects := "<yourObjectName1>,<yourObjectName2>"

	// 将对象名称列表转换为切片
	var DeleteObjects []oss.DeleteObject
	objectSlice := strings.Split(objects, ",")
	for _, name := range objectSlice {
		DeleteObjects = append(DeleteObjects, oss.DeleteObject{Key: oss.Ptr(strings.TrimSpace(name))})
	}

	// 创建删除多个对象的请求
	request := &oss.DeleteMultipleObjectsRequest{
		Bucket: oss.Ptr(bucketName), // 存储空间名称
		Delete: &oss.Delete{
			Objects: DeleteObjects, // 要删除的对象列表
		},
	}

	// 执行删除多个对象的操作并处理结果
	result, err := client.DeleteMultipleObjects(context.TODO(), request)
	if err != nil {
		log.Fatalf("failed to delete multiple objects %v", err)
	}

	// 打印删除多个对象的结果
	log.Printf("delete multiple objects result:%#v\n", result)
}
