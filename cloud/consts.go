package cloud

import (
	"os"
	"strings"
)

// Providers
const (
	_CloudProviderAliyun = "ALIYUN"
	_CloudProviderVolc   = "VOLC"
)

var (
	cloudProviderAliyun = os.Getenv("X_CLOUD_PROVIDER_ALIYUN")
	cloudProviderVolc   = os.Getenv("X_CLOUD_PROVIDER_VOLC")
)

func init() {
	if cloudProviderAliyun == "" {
		cloudProviderAliyun = _CloudProviderAliyun
	}
	if cloudProviderVolc == "" {
		cloudProviderVolc = _CloudProviderVolc
	}
}

func AliyunProvider() string {
	return cloudProviderAliyun
}

func VolcProvider() string {
	return cloudProviderVolc
}

func IsAliyunProvider(provider string) bool {
	return strings.EqualFold(provider, cloudProviderAliyun)
}

func IsVolcProvider(provider string) bool {
	return strings.EqualFold(provider, cloudProviderVolc)
}
