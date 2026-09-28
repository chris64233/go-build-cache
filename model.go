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
	ID string `json:"id"`
	// Namespace 会话目标条目所属命名空间（发布时计入该命名空间配额）。
	Namespace      string      `json:"namespace,omitempty"`
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

// DefaultNamespace 是未显式指定命名空间的会话/条目所属的默认命名空间。
const DefaultNamespace = "default"

// Entry 是一个已发布的、可被读者看到的缓存条目。
type Entry struct {
	// Namespace 条目所属命名空间（配额与固定都以命名空间为界）。
	Namespace   string     `json:"namespace"`
	Key         string     `json:"key"`
	Version     uint64     `json:"version"`
	Digest      Digest     `json:"digest"`
	TotalSize   int64      `json:"total_size"`
	Chunks      []ChunkRef `json:"chunks"`
	PublishedAt time.Time  `json:"published_at"`
	// LastAccessedAt 最近一次访问时间：读取命中或同摘要重新发布都会刷新，
	// 是配额淘汰 LRU 排序与淘汰决定复核的依据。
	LastAccessedAt time.Time `json:"last_accessed_at"`
}

// Namespace 是配额单位：命名空间内全部已发布条目总字节数不得超过 MaxBytes
// （MaxBytes <= 0 表示不限制）。
type Namespace struct {
	Name      string    `json:"name"`
	MaxBytes  int64     `json:"max_bytes"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// 固定租约状态。
const (
	// PinActive 固定生效中：保护条目免于淘汰，并通过引用快照保护内容块。
	PinActive = "active"
	// PinReleased 被显式解除；终态。再次固定会在其之上分配更高版本。
	PinReleased = "released"
	// PinExpired 已被过期扫描判定失效；终态。再次固定会在其之上分配更高版本。
	PinExpired = "expired"
)

// Pin 是一个缓存键的固定租约。
//
// 版本（Version）在该 (namespace,key) 上严格递增：首次固定为 1，
// 此后每次续租 / 解除 / 过期失效 / 重新固定都会 +1。所有写操作都带版本条件，
// 因此并发交错时旧版本操作不可能缩短（续租）或复活（解除、过期扫描）新租约。
//
// Chunks 是固定瞬间的条目引用快照：即使条目随后被覆盖发布而消失，
// 只要租约仍 active 且未到期，这些块依旧受到 GC 保护。
type Pin struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	// Version 固定租约自身的严格递增版本号（与条目内容版本无关）。
	Version uint64 `json:"version"`
	// EntryVersion / Digest 标识被固定的条目版本与摘要；
	// 固定只作用于存在且摘要匹配的条目。
	EntryVersion uint64     `json:"entry_version"`
	Digest       Digest     `json:"digest"`
	Chunks       []ChunkRef `json:"chunks"`
	Deadline     time.Time  `json:"deadline"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// Active 报告固定在时刻 now 是否仍提供保护。
func (p Pin) Active(now time.Time) bool {
	return p.Status == PinActive && p.Deadline.After(now)
}

// PinRequest 记录成功的固定类请求（请求号幂等）：
// 相同请求号 + 相同请求指纹返回原结果；相同请求号 + 不同指纹按冲突拒绝。
type PinRequest struct {
	RequestID   string    `json:"request_id"`
	Namespace   string    `json:"namespace"`
	Key         string    `json:"key"`
	Action      string    `json:"action"` // pin / renew / unpin
	Fingerprint string    `json:"fingerprint"`
	Result      Pin       `json:"result"`
	CreatedAt   time.Time `json:"created_at"`
}

// 固定类请求动作。
const (
	PinActionPin   = "pin"
	PinActionRenew = "renew"
	PinActionUnpin = "unpin"
)

// 淘汰候选在提交（commit）阶段的复核结果。
const (
	// EvictPending 决策已建立，尚未提交。
	EvictPending = ""
	// EvictDeleted 复核通过，条目已删除。
	EvictDeleted = "deleted"
	// EvictSkippedAccessed 候选确认后发生过读取（访问时间前移）。
	EvictSkippedAccessed = "skipped_accessed"
	// EvictSkippedPinned 候选确认后被有效固定保护。
	EvictSkippedPinned = "skipped_pinned"
	// EvictSkippedRepublished 候选确认后被重新发布（版本或摘要变化）。
	EvictSkippedRepublished = "skipped_republished"
	// EvictSkippedMissing 提交时条目已不存在。
	EvictSkippedMissing = "skipped_missing"
	// EvictSkippedStale 依附的两阶段请求（晋级）在提交时判定过期，整个淘汰决定被放弃。
	EvictSkippedStale = "skipped_stale"
)

// EvictionCandidate 是淘汰决策中的单个候选及其提交结果。
type EvictionCandidate struct {
	Namespace      string    `json:"namespace"`
	Key            string    `json:"key"`
	Digest         Digest    `json:"digest"`
	Version        uint64    `json:"version"`
	Size           int64     `json:"size"`
	LastAccessedAt time.Time `json:"last_accessed_at"`
	// Outcome 提交复核结果，EvictPending 表示尚未提交。
	Outcome string `json:"outcome,omitempty"`
}

// EvictionDecision 是一次"选择淘汰候选 → 确认后删除"的两阶段决定。
//
// 候选在决策建立时快照（版本 / 摘要 / 最近访问时间）；提交时逐个复核，
// 快照后发生过访问、固定或重新发布的候选会被跳过（旧决定失效），
// 不会删除仍在使用的条目。
type EvictionDecision struct {
	ID         string `json:"id"`
	Namespace  string `json:"namespace"`
	Reason     string `json:"reason"`
	MaxBytes   int64  `json:"max_bytes"`
	UsedBefore int64  `json:"used_before"`
	// NeededBytes 决策时需要释放的字节数（含计划中的新发布体量）。
	NeededBytes int64 `json:"needed_bytes"`
	// FreedBytes 提交后实际释放的字节数。
	FreedBytes int64               `json:"freed_bytes"`
	Candidates []EvictionCandidate `json:"candidates"`
	// Status proposed / committed。
	Status      string    `json:"status"`
	DecidedAt   time.Time `json:"decided_at"`
	CommittedAt time.Time `json:"committed_at,omitempty"`
}

// 淘汰决策状态。
const (
	EvictionProposed  = "proposed"
	EvictionCommitted = "committed"
)

// BlobReference 描述一个内容块当前被谁引用（内容引用查询结果项）。
type BlobReference struct {
	// Kind 引用来源：entry（已发布条目）/ session（活跃上传）/ pin（有效固定）。
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Key       string `json:"key"`
	Version   uint64 `json:"version,omitempty"`
	// Active 引用是否有效（会话未过期 open；固定未到期 active）。已发布条目恒为 true。
	Active bool   `json:"active"`
	Detail string `json:"detail,omitempty"`
}

// 内容引用来源。
const (
	RefEntry   = "entry"
	RefSession = "session"
	RefPin     = "pin"
)

// 晋级目标模式。
const (
	// PromoteCopy 把来源条目复制为目标命名空间下的新键（目标键必须不存在）。
	PromoteCopy = "copy"
	// PromoteReplace 替换目标命名空间下的同名旧条目（旧条目不存在时退化为新建）。
	PromoteReplace = "replace"
)

// PromotionSource 是晋级请求建立时冻结的来源条目快照：
// 此后来源条目即使被覆盖发布或删除，本次晋级依据的摘要与内容引用也不变。
type PromotionSource struct {
	Namespace string     `json:"namespace"`
	Key       string     `json:"key"`
	Version   uint64     `json:"version"`
	Digest    Digest     `json:"digest"`
	TotalSize int64      `json:"total_size"`
	Chunks    []ChunkRef `json:"chunks"`
}

// Promotion 是一次跨命名空间晋级的持久化记录（请求号幂等）。
//
// 晋级在创建请求时冻结来源条目的摘要与内容引用；提交时原子写入目标条目、
// 提交配额淘汰结果并落定本记录。提交后两端条目共享同一组内容块：
// 来源条目随后被删除或覆盖，也不会回收仍由目标条目引用的块。
type Promotion struct {
	// RequestID 外部请求号，全局唯一，是晋级记录的身份。
	RequestID string `json:"request_id"`
	// Mode 目标模式：copy（新键）/ replace（同名覆盖）。
	Mode string `json:"mode"`
	// Source 请求建立时冻结的来源条目快照。
	Source PromotionSource `json:"source"`
	// TargetNamespace / TargetKey 目标命名空间与目标键。
	TargetNamespace string `json:"target_namespace"`
	TargetKey       string `json:"target_key"`
	// Fingerprint 请求指纹：来源/目标/模式/来源预期摘要。
	// 同请求号不同指纹一律冲突；来源摘要变化、配额版本变化等"状态漂移"
	// 不走指纹冲突，而在提交时按 Stale 明确失败。
	Fingerprint string `json:"fingerprint"`
	// TargetVersion 请求建立时目标同名条目的版本快照（不存在为 0），
	// replace 提交时必须仍与之相等，否则旧请求按 target_changed 失败。
	TargetVersion uint64 `json:"target_version"`
	// QuotaVersion 提交所依据的目标命名空间配额版本（UpdatedAt.UnixNano()）；
	// 重放时若目标配额版本已变化，旧请求明确失败。
	QuotaVersion int64 `json:"quota_version"`
	// MaxBytes / UsedBefore 请求建立时目标命名空间的配额上限与用量快照。
	MaxBytes   int64 `json:"max_bytes"`
	UsedBefore int64 `json:"used_before"`
	// UsedAfter 提交完成后的目标命名空间实际用量（含淘汰释放）。
	UsedAfter int64 `json:"used_after,omitempty"`
	// ResultEntry 提交后目标条目的身份与摘要（copy 恒为 v1；replace 为新版本）。
	ResultEntry PromotionResultEntry `json:"result_entry"`
	// ReplacedVersion 被替换旧条目的版本（copy 或旧条目不存在时为 0）。
	ReplacedVersion uint64 `json:"replaced_version,omitempty"`
	// SharedChunks 晋级后两端共享的内容块摘要数量（查询用）。
	SharedChunks int `json:"shared_chunks"`
	// Eviction 目标配额不足时在创建时算定的淘汰决策（未发生时省略）。
	// 决策快照在请求建立时形成，提交时逐候选复核：期间被访问/固定/重新发布
	// 的候选自动跳过；若因此无法满足配额，旧请求按 eviction_decision_stale 失败。
	Eviction *EvictionDecision `json:"eviction,omitempty"`
	// FailReason 提交判定为过期（stale）时的机器可读原因；空表示尚未失败。
	FailReason string `json:"fail_reason,omitempty"`
	// FailDetail 失败时的人类可读说明（来源/配额/目标如何漂移）。
	FailDetail  string    `json:"fail_detail,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	CommittedAt time.Time `json:"committed_at,omitempty"`
}

// PromotionResultEntry 是晋级提交后目标条目的身份摘要。
type PromotionResultEntry struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   uint64 `json:"version"`
	Digest    Digest `json:"digest"`
}

// 晋级记录状态。
const (
	// PromotionProposed 请求已建立：来源快照与淘汰候选已冻结，尚未提交。
	PromotionProposed = "proposed"
	// PromotionCommitted 晋级已原子提交：目标条目、引用计数（共享块）、淘汰结果落定。
	PromotionCommitted = "committed"
	// PromotionFailed 提交时判定旧请求已过期（来源/配额/目标/淘汰决定变化），
	// 终态；同号重放返回同一失败，绝不覆盖新状态。
	PromotionFailed = "failed"
)

// PromotionLink 描述一个条目与另一个命名空间条目之间由晋级建立的共享关系。
type PromotionLink struct {
	RequestID    string `json:"request_id"`
	Namespace    string `json:"namespace"`
	Key          string `json:"key"`
	Version      uint64 `json:"version"`
	Digest       Digest `json:"digest"`
	SharedChunks int    `json:"shared_chunks"`
	// Role 该条目在本次晋级中的角色：source / target。
	Role        string    `json:"role"`
	CommittedAt time.Time `json:"committed_at"`
}

// 晋级关系中的角色。
const (
	PromotionRoleSource = "source"
	PromotionRoleTarget = "target"
)

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
	GCPinSwept     = "pin_swept" // 过期固定租约被扫描失效
	GCEviction     = "eviction"  // 配额淘汰（详情记于 Detail）
	GCPromotion    = "promotion" // 跨命名空间晋级（详情记于 Detail）
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
