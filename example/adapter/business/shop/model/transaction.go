package model

import (
	"fmt"
	"strconv"
	"time"
)

// Transaction 表示商店的一笔交易流水
type Transaction struct {
	// 产品信息
	Category    string `json:"category"`    // 产品分类，如 "食品", "日用品"
	Subcategory string `json:"subcategory"` // 产品子类，如 "饮料", "洗发水"
	ProductName string `json:"productName"` // 产品名称

	// 价格与数量
	UnitPrice    float64 `json:"unitPrice"`    // 产品单价（元）
	UnitCost     float64 `json:"unitCost"`     // 单位成本（元），用于利润计算
	Quantity     int     `json:"quantity"`     // 购买数量
	DiscountRate float64 `json:"discountRate"` // 折扣率（0.0 ~ 1.0），如 0.2 表示打 8 折

	// 金额计算结果（可选存储）
	ActualRevenue  float64 `json:"actualRevenue"`  // 实际收银 = UnitPrice * Quantity * (1 - DiscountRate)
	ExpectedProfit float64 `json:"expectedProfit"` // 预计利润 = (UnitPrice*(1-DiscountRate) - UnitCost) * Quantity

	// 交易元数据
	TransactionID    string    `json:"transactionId"`       // 交易唯一 ID（系统生成）
	BillSerialNumber string    `json:"billSerialNumber"`    // 账单流水号（来自支付平台或收银系统）
	Timestamp        time.Time `json:"timestamp"`           // 交易发生时间（记录时间）
	StoreID          string    `json:"storeId"`             // 门店 ID
	StoreName        string    `json:"storeName,omitempty"` // 门店名称（可选）

	// 交易上下文
	TransactionType string `json:"transactionType"` // 交易类型："online" 或 "offline"
	PaymentProvider string `json:"paymentProvider"` // 收银服务商，如 "wechat", "alipay", "unionpay", "cash"
}

func (t *Transaction) ToLogMap() map[string]string {
	return map[string]string{
		"ymd":              t.Timestamp.Format("20060102"),
		"hour":             t.Timestamp.Format("15"),
		"category":         t.Category,
		"subcategory":      t.Subcategory,
		"productName":      t.ProductName,
		"unitPrice":        fmt.Sprintf("%f", t.UnitPrice),
		"unitCost":         fmt.Sprintf("%f", t.UnitCost),
		"quantity":         fmt.Sprintf("%d", t.Quantity),
		"discountRate":     fmt.Sprintf("%f", t.DiscountRate),
		"actualRevenue":    fmt.Sprintf("%f", t.ActualRevenue),
		"expectedProfit":   fmt.Sprintf("%f", t.ExpectedProfit),
		"transactionId":    t.TransactionID,
		"billSerialNumber": t.BillSerialNumber,
		"timestamp":        t.Timestamp.Format("2006-01-02 15:04:05"),
		"storeId":          t.StoreID,
		"storeName":        t.StoreName,
		"transactionType":  t.TransactionType,
		"paymentProvider":  t.PaymentProvider,
	}
}

func (t *Transaction) ParseFromLogMap(log map[string]string) {
	t.Category = log["category"]
	t.Subcategory = log["subcategory"]
	t.ProductName = log["productName"]
	t.UnitPrice, _ = strconv.ParseFloat(log["unitPrice"], 64)
	t.UnitCost, _ = strconv.ParseFloat(log["unitCost"], 64)
	t.Quantity, _ = strconv.Atoi(log["quantity"])
	t.DiscountRate, _ = strconv.ParseFloat(log["discountRate"], 64)
	t.ActualRevenue, _ = strconv.ParseFloat(log["actualRevenue"], 64)
	t.ExpectedProfit, _ = strconv.ParseFloat(log["expectedProfit"], 64)
	t.TransactionID = log["transactionId"]
	t.BillSerialNumber = log["billSerialNumber"]
	t.Timestamp, _ = time.Parse("2006-01-02 15:04:05", log["timestamp"])
	t.StoreID = log["storeId"]
	t.StoreName = log["storeName"]
	t.TransactionType = log["transactionType"]
	t.PaymentProvider = log["paymentProvider"]
}
