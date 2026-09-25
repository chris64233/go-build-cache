package buildcache

import (
	"encoding"
	"time"
)

// ChunkSpec 是创建会话时声明的单个分片：
// 分片号、在最终制品中的偏移、字节数以及该分片内容的预期摘要。
type ChunkSpec struct {
	Index  int    `json:"index"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	Digest Digest `json:"digest"`
}

// 会话状态。
const (
	// SessionOpen 会话进行中：可上传分片，其数据对读者不可见。
	SessionOpen = "open"
	// SessionCompleted 会话已完成并原子发布（或复用了已有发布结果）。
	SessionCompleted = "completed"
	// SessionCanceled 会话被显式取消。
	SessionCanceled = "canceled"
)

// ChunkReceipt 记录一个已到达并校验通过的分片。
type ChunkReceipt struct {
	Index      int       `json:"index"`
	Digest     Digest    `json:"digest"`
	Size       int64     `json:"size"`
	ReceivedAt time.Time `json:"received_at"`
}

// Session 是分片上传会话的持久化元数据。
type Session struct {
	ID             string      `json:"id"`
	Key            string      `json:"key"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	TotalSize      int64       `json:"total_size"`
	FinalDigest    Digest      `json:"final_digest"`
	Chunks         []ChunkSpec `json:"chunks"`
	CreatedAt      time.Time   `json:"created_at"`
	ExpiresAt      time.Time   `json:"expires_at"`
	Status         string      `json:"status"`
	// 完成后记录发布结果：复用已有条目时二者也会被填上。
	PublishedVersion uint64 `json:"published_version,omitempty"`
	// 已到达分片（分片号 -> 回执）。
	Received map[int]ChunkReceipt `json:"received"`
}

// ChunkRef 是已发布条目对单个内容块的引用。
type ChunkRef struct {
	Index  int    `json:"index"`
	Size   int64  `json:"size"`
	Digest Digest `json:"digest"`
}

// Entry 是一个已发布的、可被读者看到的缓存条目。
type Entry struct {
	Key         string     `json:"key"`
	Version     uint64     `json:"version"`
	Digest      Digest     `json:"digest"`
	TotalSize   int64      `json:"total_size"`
	Chunks      []ChunkRef `json:"chunks"`
	PublishedAt time.Time  `json:"published_at"`
}

// GCRecord 是垃圾回收/清理过程留下的可审计决策记录。
type GCRecord struct {
	Time       time.Time `json:"time"`
	Generation uint64    `json:"generation"`
	Action     string    `json:"action"`
	Blob       Digest    `json:"blob,omitempty"`
	Key        string    `json:"key,omitempty"`
	SessionID  string    `json:"session_id,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Detail     string    `json:"detail,omitempty"`
}

// GC 动作类型。
const (
	GCStart        = "gc_start"
	GCSessionSwept = "session_swept" // 过期/取消会话被回收
	GCMark         = "blob_retained" // mark 阶段判定块存活
	GCDelete       = "blob_deleted"  // sweep 阶段删除块
	GCSkipInUse    = "blob_skip_in_use"
	GCFinish       = "gc_finish"
)

// 让 Digest 以 "algo:hex" 字符串形式持久化。
func (d Digest) MarshalText() ([]byte, error) { return []byte(d.String()), nil }
func (d *Digest) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*d = Digest{}
		return nil
	}
	parsed, err := ParseDigest(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

var _ encoding.TextMarshaler = Digest{}
