# 日志适配设计说明

## 1. 设计目标
- 统一阿里云 SLS 与火山云 TLS 的日志读写接口，业务侧只依赖 `cloud/log` 抽象。
- 对外稳定提供三类能力：日志写入（Producer）、日志消费（Consumer）、日志查询/分析（Client）。
- 将跨云差异收敛在适配层，避免业务中出现大量 provider 分支。

## 2. 包结构与角色
- `cloud/log/interface.go`
  - 定义 `Producer`、`Consumer`、`Client` 三类接口。
  - 定义 `Sql` 模版结构与 `LogsResult` 通用结果。
- `cloud/log/base.go`
  - 提供 `BaseClient`，集中实现 SQL 模版注册和分页 SQL 拼装的通用逻辑。
- `cloud/log/aliyun`
  - 阿里云日志实现：对接 `aliyun-log-go-sdk`。
- `cloud/log/volc`
  - 火山云日志实现：对接 `volc-sdk-golang/service/tls`。

## 3. 核心抽象
### 3.1 Producer
- `Start/Close`：生命周期管理。
- `SendLog/SendLogs`：发送单条或多条日志。

### 3.2 Consumer
- `Init(handler)`：注入消费回调。
- `Start/Close`：启动和关闭消费。

### 3.3 Client
- `RegistSql/FormatSql`：SQL 模版注册与格式化。
- `GetLogs`：原始日志查询。
- `SelectLogs`：分析型查询（SQL 分析）。

## 4. 为什么区分 `GetLogs` 与 `SelectLogs`
- 职责不同：
  - `GetLogs` 面向“原始日志检索”，关注时间范围、顺序、拉取条数。
  - `SelectLogs` 面向“分析查询”，关注聚合、分组、统计结果。
- 参数模型不同：
  - `GetLogs` 使用 `reverse` 控制时序；
  - `SelectLogs` 使用 `offset + limit` 做分析结果分页。
- 云厂商语义不同但可以统一：
  - 两家云都支持 SQL 查询，但“原始检索”和“分析语义”在请求参数、结果字段、完备性判断上不同，拆分后业务侧语义更清晰。

## 5. 为什么需要 `WithLimit` 与 `WithOffset`
- 分析查询通常存在“结果分页”诉求，需要稳定地按页获取统计结果。
- 各云 SDK 都以 SQL 文本作为查询载体，分页控制最终要拼接到 SQL。
- 将 `offset/limit` 拼装收敛在 `BaseClient`：
  - 避免每个 provider 重复拼接代码；
  - 保证业务传入分页参数后，跨云行为一致。

## 6. 为什么需要 `RegistSql` 与 `FormatSql`
- 业务 SQL 不是固定字符串，通常包含动态条件（时间窗、过滤条件、维度等）。
- 不同云 SQL 方言存在差异（函数名、字段函数、细节语法），需要按 provider 维护不同模板。
- 通过“注册 + 格式化”实现：
  - 模版在初始化阶段注册；
  - 查询阶段仅传格式参数；
  - 业务层只传 `sqlName`，不直接耦合具体 SQL 文本。

## 7. 阿里云与火山云查询实现差异（适配点）
- 阿里云：
  - 通过 `GetLogsV2` 请求日志；
  - 使用 `PowerSQL`、`Topic` 等可选参数；
  - `IsComplete` 标识结果是否完整。
- 火山云：
  - 通过 `SearchLogs` 请求日志；
  - 可用 `Context` 做连续查询；
  - `Status`（`complete/incomplete/error/time_out`）描述查询状态。

适配层将以上差异转换为统一的 `LogsResult{Complete, Count, Logs, More}` 与 error 语义。

## 8. 典型使用流程
1. 初始化 provider 对应 `Client`。
2. 使用 `RegistSql` 注册查询模板。
3. 调用：
   - `GetLogs` 拉取原始日志；
   - `SelectLogs` 拉取分析结果。
4. 写入场景使用 `Producer`；订阅场景使用 `Consumer`。

## 9. 单测策略
- 通用层：覆盖 `Sql`、`BaseClient`、provider 常量判定函数。
- Provider 层：覆盖可离线验证逻辑：
  - 构造函数参数校验；
  - 查询方法的边界分支（`limit<=0`、SQL 未注册）；
  - Producer 空日志短路分支；
  - Consumer `Handler` 的日志内容转换逻辑。
- 说明：依赖真实云网络/凭证的路径不放入单元测试，避免引入环境耦合。
