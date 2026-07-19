package media

import "time"

const (
	AssetPurposeOutput     = "output"
	AssetPurposeVideoInput = "video_input"
)

// Asset 表示已归档到本地媒体存储的不可变资源。
type Asset struct {
	ID          string
	Kind        string
	Purpose     string
	StorageKey  string
	MIMEType    string
	SizeBytes   int64
	SHA256      string
	CreatedAt   time.Time
	StagedUntil *time.Time
}
