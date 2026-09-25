package buildcache

import "time"

// ChunkSpec 声明一个分片：其序号、字节大小与内容摘要。
type ChunkSpec struct {
	Index int    `json:"index"`
	Size  int64  `json:"size"`
	Hash  Digest `json:"hash"`
}

// SessionState 是上传会话的生命周期状态。
type SessionState string

const (
	SessionActive      SessionState = "active"
	SessionCompleted   SessionState = "completed"
	SessionCancelled   SessionState = "cancelled"
	SessionExpiredMark SessionState = "expired" // 已被过期清理回收（未发布）
)

// Session 是上传会话的持久化元数据。
type Session struct {
	ID        string       `json:"id"`
	Key       string       `json:"key"`
	Digest    Digest       `json:"digest"` // 拼接后的预期最终摘要
	TotalSize int64        `json:"total_size"`
	Chunks    []ChunkSpec  `json:"chunks"` // 按 Index 排序、连续无空洞
	CreatedAt time.Time    `json:"created_at"`
	ExpiresAt time.Time    `json:"expires_at"`
	State     SessionState `json:"state"`

	// Uploaded[index] = 该分片已确认内容块的摘要（等于 Chunks[index].Hash）。
	// nil 摘要表示尚未上传。
	Uploaded []Digest `json:"uploaded,omitempty"`

	// 完成后填充。
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	PublishedKey bool      `json:"published_key,omitempty"` // 是否成功发布为新条目（false 可能是复用已有同摘要条目）
	ReusedEntry  bool      `json:"reused_entry,omitempty"`  // 发布时复用了键上已有的同摘要条目

	// 取消/过期时填充。
	EndedAt time.Time `json:"ended_at,omitempty"`
	Reason  string    `json:"reason,omitempty"`
}

// Entry 是一个已发布的缓存条目。条目在发布成功前对读者完全不可见。
type Entry struct {
	Key       string      `json:"key"`
	Digest    Digest      `json:"digest"` // 拼接内容的实际摘要，也是完整性校验依据
	Size      int64       `json:"size"`
	Chunks    []ChunkSpec `json:"chunks"`
	Version   uint64      `json:"version"` // 单调递增；异值覆盖因版本条件失败而被拒绝
	CreatedAt time.Time   `json:"created_at"`
}

// GCAction 是垃圾回收对单个内容块的处置决策。
type GCAction string

const (
	// GCKeepMarkedPublished 内容块被某个已发布条目引用。
	GCKeepMarkedPublished GCAction = "keep_published"
	// GCKeepMarkedSession 内容块被某个活跃（未过期）会话引用。
	GCKeepMarkedSession GCAction = "keep_active_session"
	// GCKeepInFlight 第一轮标记时还没有已发布条目引用它，屏障后重新标记发现
	// 并发发布刚刚建立了引用——屏障的存在使该块被保留。这正是
	// “标记到删除之间防住并发发布”的审计证据。
	GCKeepInFlight GCAction = "keep_inflight_publish"
	// GCDelete 未被任何已发布键或活跃会话引用，删除。
	GCDelete GCAction = "delete"
	// GCRetryRemoveFailed 决定删除但文件删除失败；保留至下一轮重试。
	GCRetryRemoveFailed GCAction = "retry_remove_failed"
)

// GCDecision 是一条可审计的 GC 决策记录。
type GCDecision struct {
	RunID      string    `json:"run_id"`
	Time       time.Time `json:"time"`
	Digest     Digest    `json:"digest"`
	Size       int64     `json:"size"`
	Action     GCAction  `json:"action"`
	Referenced bool      `json:"referenced"` // 屏障后重新标记时是否仍被引用
	Reason     string    `json:"reason,omitempty"`
}

// GCResult 是一轮垃圾回收的汇总结果。
type GCResult struct {
	RunID       string
	StartedAt   time.Time
	FinishedAt  time.Time
	Scanned     int
	Kept        int
	Deleted     int
	DeletedSize int64
	Decisions   []GCDecision
}
