package volc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
)

func checkErr(err error) {
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			fmt.Println("Error:", serverErr.Error())
			fmt.Println("Request ID:", serverErr.RequestID)
			fmt.Println("Response Status Code:", serverErr.StatusCode)
			fmt.Println("Response Header:", serverErr.Header)
			fmt.Println("Response Err Code:", serverErr.Code)
			fmt.Println("Response Err Msg:", serverErr.Message)
		} else if clientErr, ok := err.(*tos.TosClientError); ok {
			fmt.Println("Error:", clientErr.Error())
			fmt.Println("Client Cause Err:", clientErr.Cause.Error())
		} else {
			fmt.Println("Error:", err)
		}
		panic(err)
	}
}

// 普通上传
func PutObjectExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 将文件上传到 example_dir 目录下的 example.txt 文件
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 将字符串 “Hello TOS” 上传到指定 example_dir 目录下的 example.txt
	body := strings.NewReader("Hello TOS")
	output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
		Content: body,
	})
	checkErr(err)
	fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}

// 从本低文件上传
func PutObjectFromFileExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 将文件上传到 example_dir 目录下的 example.txt 文件
		objectKey = "example_dir/example.txt"
		fileName  = "/usr/local/test.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 读取本地文件数据
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
		Content: f,
	})
	checkErr(err)
	fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}

// 读取对象内容长度
func ReadObjectPosition() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 将文件上传到 example_dir 目录下的 example.txt 文件
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)

	output, err := client.HeadObjectV2(ctx, &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectKey,
	})
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
			if serverErr.StatusCode == http.StatusNotFound {
				fmt.Println("Object %s position 0 .\n")
			} else {
				fmt.Println("Error:", serverErr.Error())
				fmt.Println("Request ID:", serverErr.RequestID)
				fmt.Println("Response Status Code:", serverErr.StatusCode)
				fmt.Println("Response Header:", serverErr.Header)
				fmt.Println("Response Err Code:", serverErr.Code)
				fmt.Println("Response Err Msg:", serverErr.Message)
				panic(err)
			}
		} else {
			panic(err)
		}
		return
	}

	fmt.Printf("Object %s position %d .\n", objectKey, output.ContentLength)
}

// 追加上传
func AppendObjectExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 将文件上传到 example_dir 目录下的 example.txt 文件
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	body := strings.NewReader("your append object value")
	// 追加上传，监听自定义上传进度回调并在客户端限制上传速度
	// 上传字符流
	output, err := client.AppendObjectV2(ctx, &tos.AppendObjectV2Input{
		Bucket:  bucketName,
		Key:     objectKey,
		Content: body,
	})
	checkErr(err)
	fmt.Println("AppendObjectV2 Request ID:", output.RequestID)

	// 追加上传网络流
	res, err := http.Get("your file url")
	checkErr(err)
	defer res.Body.Close()
	if res.ContentLength > 0 {
		output, err = client.AppendObjectV2(ctx, &tos.AppendObjectV2Input{
			Bucket: bucketName,
			Key:    objectKey,
			// 指定下次 CRC 计算初始值
			PreHashCrc64ecma: output.HashCrc64ecma,
			// 指定下次 append offset
			Offset:        output.NextAppendOffset,
			Content:       res.Body,
			ContentLength: res.ContentLength,
		})
	}

	checkErr(err)
	fmt.Println("AppendObjectV2 Request ID:", output.RequestID)
}

// 分片上传
func MultipartPutObjectExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 指定的 ObjectKey
		objectKey = "*** Provide your object name ***"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 初始化分片，指定对象权限为私有，存储类型为标准存储并设置元数据信息

	createMultipartOutput, err := client.CreateMultipartUploadV2(ctx, &tos.CreateMultipartUploadV2Input{
		Bucket:       bucketName,
		Key:          objectKey,
		ACL:          enum.ACLPrivate,
		StorageClass: enum.StorageClassStandard,
		Meta:         map[string]string{"key": "value"},
	})
	checkErr(err)
	fmt.Println("CreateMultipartUploadV2 Request ID:", createMultipartOutput.RequestID)
	// 获取到上传的 UploadID
	fmt.Println("CreateMultipartUploadV2 Upload ID:", createMultipartOutput.UploadID)
	// 需要上传的文件路径
	localFile := "/root/example.txt"
	fd, err := os.Open(localFile)
	checkErr(err)
	defer fd.Close()
	stat, err := os.Stat(localFile)
	checkErr(err)
	fileSize := stat.Size()
	// partNumber 编号从 1 开始
	partNumber := 1
	// part size 大小设置为 20M
	partSize := int64(20 * 1024 * 1024)
	offset := int64(0)
	var parts []tos.UploadedPartV2
	for offset < fileSize {
		uploadSize := partSize
		// 最后一个分片
		if fileSize-offset < partSize {
			uploadSize = fileSize - offset
		}
		fd.Seek(offset, io.SeekStart)
		partOutput, err := client.UploadPartV2(ctx, &tos.UploadPartV2Input{
			UploadPartBasicInput: tos.UploadPartBasicInput{
				Bucket:     bucketName,
				Key:        objectKey,
				UploadID:   createMultipartOutput.UploadID,
				PartNumber: partNumber,
			},
			Content:       io.LimitReader(fd, uploadSize),
			ContentLength: uploadSize,
		})
		checkErr(err)
		fmt.Println("upload Request ID:", partOutput.RequestID)
		parts = append(parts, tos.UploadedPartV2{PartNumber: partNumber, ETag: partOutput.ETag})
		offset += uploadSize
		partNumber++
	}

	completeOutput, err := client.CompleteMultipartUploadV2(ctx, &tos.CompleteMultipartUploadV2Input{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: createMultipartOutput.UploadID,
		Parts:    parts,
	})
	checkErr(err)
	fmt.Println("CompleteMultipartUploadV2 Request ID:", completeOutput.RequestID)
}

// 断点续传
func UploadFileExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 将文件上传到 example_dir 目录下的 example.txt 文件
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
		// 本地文件完整路径，例如usr/local/testfile.txt
		fileName = "/usr/local/testfile.txt"
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 直接使用文件路径上传文件
	output, err := client.UploadFile(ctx, &tos.UploadFileInput{
		CreateMultipartUploadV2Input: tos.CreateMultipartUploadV2Input{
			Bucket: bucketName,
			Key:    objectKey,
		},
		// 上传的文件路径
		FilePath: fileName,
		// 上传时指定分片大小
		PartSize: tos.DefaultPartSize,
		// 分片上传任务并发数量
		TaskNum: 5,
		// 开启断点续传
		EnableCheckpoint: true,
	})
	checkErr(err)
	fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}

// 自定义进度回调，需要实现 tos.DataTransferStatusChange 接口
type listener struct {
}

func (l *listener) DataTransferStatusChange(event *tos.DataTransferStatus) {
	switch event.Type {
	case enum.DataTransferStarted:
		fmt.Println("Data transfer started")
	case enum.DataTransferRW:
		// Chunk 模式下 TotalBytes 值为 -1
		if event.TotalBytes != -1 {
			fmt.Printf("Once Read:%d,ConsumerBytes/TotalBytes: %d/%d,%d%%\n", event.RWOnceBytes, event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
		} else {
			fmt.Printf("Once Read:%d,ConsumerBytes:%d\n", event.RWOnceBytes, event.ConsumedBytes)
		}
	case enum.DataTransferSucceed:
		fmt.Printf("Data Transfer Succeed, ConsumerBytes/TotalBytes: %d/%d,%d%%\n", event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
	case enum.DataTransferFailed:
		fmt.Printf("Data Transfer Failed\n")
	}
}

type rateLimit struct {
	rate     int64
	capacity int64

	currentAmount int64
	sync.Mutex
	lastConsumeTime time.Time
}

func NewDefaultRateLimit(rate int64, capacity int64) tos.RateLimiter {
	return &rateLimit{
		rate:            rate,
		capacity:        capacity,
		lastConsumeTime: time.Now(),
		currentAmount:   capacity,
		Mutex:           sync.Mutex{},
	}
}

func (d *rateLimit) Acquire(want int64) (ok bool, timeToWait time.Duration) {
	d.Lock()
	defer d.Unlock()
	if want > d.capacity {
		want = d.capacity
	}
	increment := int64(time.Now().Sub(d.lastConsumeTime).Seconds() * float64(d.rate))
	if increment+d.currentAmount > d.capacity {
		d.currentAmount = d.capacity
	} else {
		d.currentAmount += increment
	}
	if want > d.currentAmount {
		timeToWaitSec := float64(want-d.currentAmount) / float64(d.rate)
		return false, time.Duration(timeToWaitSec * float64(time.Second))
	}
	d.lastConsumeTime = time.Now()
	d.currentAmount -= want
	return true, 0
}

// 下载对象到内存
func GetObjectToMemoryExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 下载对象
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	rateLimit1m := int64(1024 * 1024)
	// 下载数据到内存

	getOutput, err := client.GetObjectV2(ctx, &tos.GetObjectV2Input{
		Bucket: bucketName,
		Key:    objectKey,
		// 获取当前下载进度
		DataTransferListener: &listener{},
		// 配置客户端限制
		RateLimiter: NewDefaultRateLimit(rateLimit1m, rateLimit1m),
		// 下载时重写响应头
		ResponseContentType:     "application/json",
		ResponseContentEncoding: "deflate",
	})
	checkErr(err)
	fmt.Println("GetObjectV2 Request ID:", getOutput.RequestID)
	// 下载时前设置的 response content type
	fmt.Println("GetObjectV2 Response ContentType:", getOutput.ContentType)
	// 下载时前设置的 response content encoding
	fmt.Println("GetObjectV2 Response ContentEncoding:", getOutput.ContentEncoding)
	// 下载数据大小
	fmt.Println("GetObjectV2 Response ContentLength", getOutput.ContentLength)
	defer getOutput.Content.Close()
	body, err := io.ReadAll(getOutput.Content)
	checkErr(err)
	// 完成下载
	fmt.Println("Get Object Content:", body)
}

// 下载到文件
func GetObjectToFileExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 下载指定文件名的文件
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)

	// 下载文件到指定的路径，示例中下载文件到 example_dir/example.txt
	getObjectToFileOutput, err := client.GetObjectToFile(ctx, &tos.GetObjectToFileInput{
		GetObjectV2Input: tos.GetObjectV2Input{
			Bucket: bucketName,
			Key:    objectKey,
		},
		FilePath: "example_dir/example.txt",
	})
	checkErr(err)
	fmt.Println("GetObjectToFile Request ID:", getObjectToFileOutput.RequestID)
	fmt.Println("GetObjectToFile File Size:", getObjectToFileOutput.ContentLength)
}

// 指定范围下载
func GetObjectRangeExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 下载指定的对象
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	rateLimit1m := int64(1024 * 1024)
	// 获取 32-64 字节，包含 32 和 64 共 33 字节
	getOutput, err := client.GetObjectV2(ctx, &tos.GetObjectV2Input{
		Bucket:      bucketName,
		Key:         objectKey,
		RangeStart:  32,
		RangeEnd:    64,
		RateLimiter: NewDefaultRateLimit(rateLimit1m, rateLimit1m),
	})
	checkErr(err)
	fmt.Println("GetObjectV2 Request ID:", getOutput.RequestID)
	// 下载时前设置的 response content type
	fmt.Println("GetObjectV2 Response ContentType:", getOutput.ContentType)
	// 下载时前设置的 response content encoding
	fmt.Println("GetObjectV2 Response ContentEncoding:", getOutput.ContentEncoding)
	// 下载数据大小
	fmt.Println("GetObjectV2 Response ContentLength", getOutput.ContentLength)
	defer getOutput.Content.Close()
	body, err := io.ReadAll(getOutput.Content)
	checkErr(err)
	// 完成下载
	fmt.Println("Get Object Content:", body)
}

// 拷贝对象
func CopyObjectExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName    = "*** Provide your bucket name ***"
		dstBucketName = "*** Provide your dst bucket name ***"
		// 复制源对象 key
		srcObjectKey = "srcObjectKey"
		dstObjectKey = "objectKey"
		ctx          = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 同一个 Bucket 复制对象，复制对象使用原有的元数据数据
	output, err := client.CopyObject(ctx, &tos.CopyObjectInput{
		Bucket:            bucketName,
		Key:               dstObjectKey,
		SrcBucket:         bucketName,
		SrcKey:            srcObjectKey,
		MetadataDirective: enum.MetadataDirectiveCopy,
	})
	checkErr(err)
	fmt.Println("CopyObject Request ID:", output.RequestID)

	// 复制对象使用指定的元数据信息
	output, err = client.CopyObject(ctx, &tos.CopyObjectInput{
		Bucket:            dstBucketName,
		Key:               dstObjectKey,
		SrcBucket:         bucketName,
		SrcKey:            srcObjectKey,
		MetadataDirective: enum.MetadataDirectiveReplace,
		// 复制时指定对象为低频存储
		StorageClass: enum.StorageClassIa,
		// 复制时指定为私有权限
		ACL: enum.ACLPrivate,
	})
	checkErr(err)
	fmt.Println("CopyObject Request ID:", output.RequestID)
}

func printObjectContent(contents []tos.ListedObjectV2) {
	for _, obj := range contents {
		// 对象 Key
		fmt.Println("Object Key:", obj.Key)
		// 对象最后修改时间
		fmt.Println("Object LastModified:", obj.LastModified)
		// 对象 Etag
		fmt.Println("Object ETag:", obj.ETag)
		// 对象大小
		fmt.Println("Object Size:", obj.Size)
		// 对象 Owner
		fmt.Println("Object Owner:", obj.Owner)
		// 对象存储类型
		fmt.Println("Object StorageClass:", obj.StorageClass)
		// 对象 CRC64
		fmt.Println("Object HashCrc64ecma:", obj.HashCrc64ecma)
	}
}

// 列出对象
/*
TOS 只有对象的概念, 可通过创建一个大小为 0 并且以斜线 / 结尾的对象, 模拟目录的功能。
通过 Delimiter 和 Prefix 两个参数可以模拟目录的功能:

首先设置 Delimiter 为 / 同时设置 Prefix 为空, 可返回根目录下的对象和子目录信息。
在设置 Delimiter 为 / 同时设置 Prefix 为子目录(subfiledir), 可返回子目录的对象和次级目录。
*/
func ListObjectsExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"
		prefix     = "*** Provide your object key prefix ***"

		ctx = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 列举指定前缀下的所有对象
	truncated := true
	continuationToken := ""
	for truncated {

		output, err := client.ListObjectsType2(ctx, &tos.ListObjectsType2Input{
			Bucket:            bucketName,
			MaxKeys:           1000,
			ContinuationToken: continuationToken,
			Prefix:            prefix,
		})
		checkErr(err)
		for _, obj := range output.Contents {
			// 对象 Key
			fmt.Println("Object Key:", obj.Key)
			// 对象最后修改时间
			fmt.Println("Object LastModified:", obj.LastModified)
			// 对象 Etag
			fmt.Println("Object ETag:", obj.ETag)
			// 对象大小
			fmt.Println("Object Size:", obj.Size)
			// 对象 Owner
			fmt.Println("Object Owner:", obj.Owner)
			// 对象存储类型
			fmt.Println("Object StorageClass:", obj.StorageClass)
			// 对象 CRC64
			fmt.Println("Object HashCrc64ecma:", obj.HashCrc64ecma)
		}
		truncated = output.IsTruncated
		continuationToken = output.NextContinuationToken
	}
}

// 判断对象存在
func CheckObjectExistsExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 存储桶中的对象名
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	output, err := client.HeadObjectV2(ctx, &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectKey,
	})
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
			if serverErr.StatusCode == http.StatusNotFound {
				fmt.Println("Object not found.")
			} else {
				fmt.Println("Error:", serverErr.Error())
				fmt.Println("Request ID:", serverErr.RequestID)
				fmt.Println("Response Status Code:", serverErr.StatusCode)
				fmt.Println("Response Header:", serverErr.Header)
				fmt.Println("Response Err Code:", serverErr.Code)
				fmt.Println("Response Err Msg:", serverErr.Message)
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	fmt.Println("HeadObjectV2 Request ID:", output.RequestID)
	// 查看内容语言格式
	fmt.Println("HeadObjectV2 Response ContentLanguage:", output.ContentLanguage)
	// 查看下载时的名称
	fmt.Println("HeadObjectV2 Response ContentDisposition:", output.ContentDisposition)
	// 查看编码类型
	fmt.Println("HeadObjectV2 Response ContentEncoding:", output.ContentEncoding)
	// 查看缓存策略
	fmt.Println("HeadObjectV2 Response CacheControl:", output.CacheControl)
	// 查看对象类型
	fmt.Println("HeadObjectV2 Response ContentType:", output.ContentType)
	// 查看缓存过期时间
	fmt.Println("HeadObjectV2 Response Expires:", output.Expires)
}

// 获取元信息
func GetObjectMetaExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 存储桶中的对象名
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	output, err := client.HeadObjectV2(ctx, &tos.HeadObjectV2Input{
		Bucket: bucketName,
		Key:    objectKey,
	})
	if err != nil {
		if serverErr, ok := err.(*tos.TosServerError); ok {
			// 判断对象是否存在
			if serverErr.StatusCode == http.StatusNotFound {
				fmt.Println("Object not found.")
			} else {
				fmt.Println("Error:", serverErr.Error())
				fmt.Println("Request ID:", serverErr.RequestID)
				fmt.Println("Response Status Code:", serverErr.StatusCode)
				fmt.Println("Response Header:", serverErr.Header)
				fmt.Println("Response Err Code:", serverErr.Code)
				fmt.Println("Response Err Msg:", serverErr.Message)
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	// 查看对象元数据
	fmt.Println("HeadObjectV2 Response Metadata:", output.Meta)
}

// 删除对象
func DeleteObjectExample() {
	var (
		accessKey = os.Getenv("TOS_ACCESS_KEY")
		secretKey = os.Getenv("TOS_SECRET_KEY")
		// Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
		endpoint = "https://tos-cn-beijing.volces.com"
		region   = "cn-beijing"
		// 填写 BucketName
		bucketName = "*** Provide your bucket name ***"

		// 指定的 objectKey
		objectKey = "example_dir/example.txt"
		ctx       = context.Background()
	)
	// 初始化客户端
	client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
	checkErr(err)
	// 删除存储桶中指定对象
	output, err := client.DeleteObjectV2(ctx, &tos.DeleteObjectV2Input{
		Bucket: bucketName,
		Key:    objectKey,
	})
	checkErr(err)
	fmt.Println("DeleteObjectV2 Request ID:", output.RequestID)
}
