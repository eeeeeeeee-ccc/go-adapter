# 项目文件树结构
```
.
├── LICENSE
├── cloud
│   ├── consts.go
│   ├── log
│   │   ├── aliyun
│   │   │   ├── client.go
│   │   │   ├── config.go
│   │   │   ├── consumer.go
│   │   │   └── producer.go
│   │   ├── base.go
│   │   ├── docs
│   │   │   └── log-adapter-design.md
│   │   ├── interface.go
│   │   └── volc
│   │       ├── client.go
│   │       ├── config.go
│   │       ├── consumer.go
│   │       └── producer.go
│   └── storage
│       ├── aliyun
│       │   ├── client.go
│       │   ├── config.go
│       │   └── credentials.go
│       ├── consts.go
│       ├── docs
│       │   └── storage-adapter-design.md
│       ├── interface.go
│       └── volc
│           ├── client.go
│           └── config.go
├── example
│   ├── adapter
│   │   ├── log
│   │   └── storage
│   └── cloud
│       ├── log
│       │   └── aliyun
│       │       ├── aliyun_log_examplr.go
│       │       └── volc
│       │           └── volc_log_example.go
│       └── storage
│           ├── aliyun
│           │   └── aliyun_storage_example.go
│           └── volc
│               └── volc_storage_example.go
├── go.mod
├── go.sum
├── pkg
│   └── util
│       └── conversion.go
└── tests
    └── cloud
        ├── log
        │   ├── aliyun_test.go
        │   ├── base_test.go
        │   └── volc_test.go
        └── storage
            ├── aliyun_test.go
            ├── storage_adapter_test.go
            └── volc_test.go
```