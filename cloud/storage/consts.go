package storage

// 访问权限类型
const (
	ACLPrivate                = "private"
	ACLPublicRead             = "public-read"
	ACLPublicReadWrite        = "public-read-write"
	ACLAuthRead               = "authenticated-read"
	ACLBucketOwnerRead        = "bucket-owner-read"
	ACLBucketOwnerFullControl = "bucket-owner-full-control"
	ACLLogDeliveryWrite       = "log-delivery-write"
	ACLBucketOwnerEntrusted   = "bucket-owner-entrusted"
	ACLDefault                = "default"
)

// 存储类型
const (
	StorageClassStandard           = "STANDARD"
	StorageClassIa                 = "IA"
	StorageClassArchiveFr          = "ARCHIVE_FR"
	StorageClassIntelligentTiering = "INTELLIGENT_TIERING"
	StorageClassColdArchive        = "COLD_ARCHIVE"
	StorageClassArchive            = "ARCHIVE"
	StorageClassDeepColdArchive    = "DEEP_COLD_ARCHIVE"

	// Deprecated: use StorageClassDeepColdArchive of ClientV2 instead
	StorageClassDeepClodArchive = "DEEP_COLD_ARCHIVE"
)
