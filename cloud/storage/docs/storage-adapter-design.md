# 存储适配设计说明

## 1. 目标与范围
- 统一阿里云 OSS 与火山云 TOS 的对象存储操作接口，业务侧仅依赖 `cloud/storage` 抽象。
- 在同一套 API 下覆盖对象写入、追加、分片上传、断点续传、元信息、读取下载、列举、复制、条件创建、删除等能力。

## 2. 目录与职责
- `cloud/storage/interface.go`
  - 定义统一 `Client` 接口（对象存储全量能力）。
- `cloud/storage/consts.go`
  - 定义 provider 常量、ACL 常量、存储类型常量及 provider 判断函数。
- `cloud/storage/aliyun`
  - 阿里云 OSS 适配实现（`client.go`、`config.go`、`credentials.go`）。
- `cloud/storage/volc`
  - 火山云 TOS 适配实现（`client.go`、`config.go`）。

## 3. 统一接口分组
### 3.1 写入对象
- `PutObjectFromFile`
- `PutObjectFromContent`
- `PutObjectFromBytes`
- `PutObjectFromReader`

### 3.2 追加对象
- `AppendObjectFromFile`
- `AppendObjectFromContent`
- `AppendObjectFromBytes`
- `AppendObjectFromReader`

### 3.3 大文件上传
- `UploadFileMultipart`
- `UploadFileBreakpoint`

### 3.4 元信息与位置
- `ReadObjectSelfMetas`
- `UpdateObjectMetas`
- `SetObjectNxMeta`
- `ReadObjectPosition`

### 3.5 读取对象
- `GetObjectToFile`
- `GetObjectContent`
- `GetObjectBytes`
- `GetObjectBytesRange`
- `GetObjectToWriter`

### 3.6 对象检查与列举
- `CheckObjectExist`
- `ListObjects`
- `ListObjectFiles`
- `ListObjectDirectories`

### 3.7 对象复制与条件创建
- `CopyObject`
- `MkIfNxObject`
- `MkIfNxAppendableObject`

### 3.8 删除能力
- `DeleteObject`
- `DeleteObjects`
- `DeleteObjectsFromDirectory`
- `DeleteDirectory`

## 4. 关键实现思路
- 通过 `meta` 与 `extra` 传递可扩展参数：
  - `meta`：对象元信息。
  - `extra`：云厂商扩展参数（如 ACL、StorageClass、checkpoint 等）。
- 统一方法签名，屏蔽 SDK 调用差异：
  - OSS 与 TOS 对同一能力的请求结构不同，由适配层完成映射。
- 目录语义统一：
  - 目录通过对象 key 前缀与 `/` 分隔符模拟，删除目录与列举目录均基于 prefix 处理。

## 5. 阿里云与火山云差异对齐
- 客户端初始化：
  - 阿里云通过 `credentials.Provider` 初始化 OSS Client。
  - 火山云通过静态 AK/SK 初始化 TOS ClientV2。
- 对象读取：
  - 两端均封装为 `GetObject*` 系列，统一返回 `string`、`[]byte` 或写入文件/Writer。
- 分页列举：
  - 阿里云使用 `ListObjectsV2Paginator`。
  - 火山云使用 `ListObjectsType2` + `ContinuationToken` 循环。
- 条件创建与元信息更新：
  - 两端均通过请求头 `If-None-Match: *` 实现“仅当不存在才创建”。
  - 元信息更新均走复制对象并替换元信息策略。

## 6. 使用示例（业务侧）
1. 初始化 provider 对应 `Client`。
2. 调用统一方法完成上传/下载/列举/删除。
3. 通过 `meta` 处理对象元信息，通过 `extra` 传入云扩展参数。

## 7. 单元测试策略
- 方法集覆盖：
  - 测试中逐一校验 `storage.Client` 接口的全部方法在阿里云与火山云实现中都存在。
- 可离线行为覆盖：
  - 覆盖本地可确定分支（例如文件不存在、空输入 no-op、provider 判断、凭证映射）。
- 网络相关路径说明：
  - 真实云请求依赖外部网络与账号环境，不在离线单测中做成功性断言，避免环境耦合。
