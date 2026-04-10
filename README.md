# 多云服务适配框架

## 项目摘要

多云服务适配框架是一个统一的云服务访问层，旨在简化业务代码对不同云服务提供商的访问。该框架目前支持阿里云和火山云的日志服务和对象存储服务，提供统一的API接口，使业务代码无需关心底层云服务的差异，从而提高开发效率和代码可维护性。

## 项目结构

项目采用模块化设计，清晰分离不同云厂商和不同云产品的实现。核心代码位于`cloud`目录下，按云产品类型（日志服务、对象存储服务）和云厂商（阿里云、火山云）进行组织。

## 功能模块

### 1. 日志服务适配

日志服务适配模块提供了统一的日志写入、消费和查询接口，支持阿里云SLS和火山云TLS。

- **核心功能**：
  - 日志写入（Producer）：支持单条和批量日志发送
  - 日志消费（Consumer）：支持日志订阅和消费
  - 日志查询/分析（Client）：支持原始日志检索和SQL分析

- **实现细节**：
  - 统一的接口定义：`cloud/log/interface.go`
  - 通用逻辑实现：`cloud/log/base.go`
  - 阿里云实现：`cloud/log/aliyun/`
  - 火山云实现：`cloud/log/volc/`

### 2. 对象存储服务适配

对象存储服务适配模块提供了统一的对象存储操作接口，支持阿里云OSS和火山云TOS。

- **核心功能**：
  - 对象写入：支持从文件、内容、字节数组和Reader写入
  - 对象追加：支持向现有对象追加内容
  - 大文件上传：支持分片上传和断点续传
  - 元信息管理：支持读取、更新和设置对象元数据
  - 对象读取：支持下载到文件、内容、字节数组和Writer
  - 对象操作：支持检查、列举、复制、条件创建和删除对象

- **实现细节**：
  - 统一的接口定义：`cloud/storage/interface.go`
  - 常量定义：`cloud/storage/consts.go`
  - 阿里云实现：`cloud/storage/aliyun/`
  - 火山云实现：`cloud/storage/volc/`

## 文档链接

- [项目文件树结构](./Tree.md)
- [日志服务适配设计文档](./cloud/log/docs/log-adapter-design.md)
- [对象存储服务适配设计文档](./cloud/storage/docs/storage-adapter-design.md)

## 示例代码

项目提供了丰富的示例代码，展示如何使用适配框架访问不同云厂商的云服务：

- **日志服务示例**：
  - [阿里云日志服务示例](./example/cloud/log/aliyun/aliyun_log_example.go)
  - [火山云日志服务示例](./example/cloud/log/volc/volc_log_example.go)

- **对象存储服务示例**：
  - [阿里云对象存储服务示例](./example/cloud/storage/aliyun/aliyun_storage_example.go)
  - [火山云对象存储服务示例](./example/cloud/storage/volc/volc_storage_example.go)

- **实际业务中使用示例**：
  - [日志服务示例](./example/adapter/business/shop/infra/log/transaction/)
  - [存储服务示例](./example/adapter/business/shop/infra/storage/transaction/)

## 快速开始

1. **复制适配器代码到项目中pkg目录中使用**

2. **从githua下载依赖库使用**
```
go get github.com/eeeeeeeee-ccc/go-adapter
```

## 注意事项
### 通用
#### 云厂商名称自定义
- 阿里云厂商名称通过环境变量 X_CLOUD_PROVIDER_ALIYUN 自定义, 不设置默认值为 ALIYUN
- 火山云厂商名称通过环境变量 X_CLOUD_PROVIDER_VOLC 自定义, 不设置默认值为 VOLC
#### 日志服务
##### 客户端 Client
- GetLogs：查询原始日志,SelectLogs：查询分析日志, 一定要区分两者的区别

- GetLogs 使用的时候
1. limit 一定要设置成正数 > 0 , 否则会返回空结果
2. 不要将extra参数设置成nil 或者 直接在调用时候在参数里直接传 map[string]any{}
2.1. 因为GetLogs的extra参数要用于存储循环查询的分页标记(阿里云是 offset,火山云是 context)
2.2. 正确用法是
```
extra := map[string]any{}
result, err := client.GetLogs(sql, st, et, limit, reverse, extra)
```

- SelectLogs：查询分析日志 使用的时候
1. limit 一定要设置成正数 > 0 , 否则会返回空结果

### 阿里云
### 火山云