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
	// PromotedFrom 非 nil 时表示该条目由一次跨命名空间晋级建立（或最近一次覆盖来自晋级），
	// 记录来源条目身份与外部请求号，用于两端关系与内容来源查询。
	PromotedFrom *PromotionOrigin `json:"promoted_from,omitempty"`
}

// PromotionOrigin 是目标条目上的晋级来源标记（内容血缘）。
type PromotionOrigin struct {
	RequestID string `json:"request_id"`
	// Namespace / Key / Version / Digest 为来源条目在晋级提交时的冻结身份。
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   uint64 `json:"version"`
	Digest    Digest `json:"digest"`
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

// 晋级模式：目标键如何写入。
const (
	// PromotionModeCopy 在目标命名空间复制为新键；目标键必须不存在。
	PromotionModeCopy = "copy"
	// PromotionModeReplace 替换目标命名空间下同名旧条目；旧条目必须不存在或摘要不同。
	PromotionModeReplace = "replace"
)

// 晋级生命周期状态。
const (
	// PromotionPrepared 晋级已建立：来源快照与淘汰候选已冻结，等待提交。
	// prepared 晋级的来源块快照作为 GC 的第四类引用，保护本次晋级依赖的内容。
	PromotionPrepared = "prepared"
	// PromotionCommitted 晋级已原子提交：目标条目、淘汰结果、请求记录已落盘。
	PromotionCommitted = "committed"
	// PromotionFailed 晋级因来源变化 / 配额版本变化 / 旧淘汰失效 / 配额不足而明确失败；
	// 不覆盖任何新状态，可通过查询取回失败原因。
	PromotionFailed = "failed"
)

// 晋级失败原因（机器可读）。
const (
	// PromotionFailSourceChanged 提交时来源条目版本或摘要相对冻结快照变化（被覆盖发布/删除重建）。
	PromotionFailSourceChanged = "source_changed"
	// PromotionFailUploading 建立时来源键存在未完成的上传会话，来源内容尚未稳定发布。
	PromotionFailUploading = "source_uploading"
	// PromotionFailQuotaVersion 提交时目标命名空间配额（max_bytes）相对决策时变化。
	PromotionFailQuotaVersion = "quota_version_changed"
	// PromotionFailEvictionStale 提交时淘汰候选被访问/固定/重新发布/删除，旧淘汰决定失效。
	PromotionFailEvictionStale = "eviction_stale"
	// PromotionFailQuota 崩溃恢复重做时，按实际用量复核发现淘汰后空间仍不足。
	PromotionFailQuota = "quota_exceeded"
	// PromotionFailTargetConflict 提交时目标键版本/摘要相对冻结基线变化（被并发覆盖）。
	PromotionFailTargetConflict = "target_conflict"
)

// PromotionQuota 是晋级决策时目标配额的乐观版本快照：max_bytes + 命名空间 UpdatedAt。
// 提交时若目标配额的这两个值变化，旧晋级请求明确失败而非按新配额覆盖。
type PromotionQuota struct {
	MaxBytes  int64     `json:"max_bytes"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Promotion 是一次跨命名空间条目的晋级决策与结果（两阶段：prepare → commit）。
//
// 建立（prepare）时冻结：
//   - 来源条目的版本/摘要/内容块引用（Source*）；
//   - 目标配额版本（Quota）；
//   - 目标键当前状态与目标写入版本条件；
//   - 按目标配额算出的淘汰候选（Eviction）。
//
// 提交（commit）时在同一把服务锁内先复核再原子完成：提交配套淘汰 → 写入目标条目（CAS）
// → 记录结果。prepared 状态的来源块快照受 GC 保护，因此即使来源条目随后被删除，晋级
// 依赖的内容仍在（提交以冻结快照完成）；任一步持久化失败时通过重做日志恢复。
type Promotion struct {
	// ID 晋级决策 ID（"promo-<零填充序号>"）。
	ID string `json:"id"`
	// RequestID 外部请求号；相同请求号 + 相同内容返回首次结果，同号异内容冲突。
	RequestID string `json:"request_id,omitempty"`

	// SourceNamespace / SourceKey 来源条目身份。
	SourceNamespace string `json:"source_namespace"`
	SourceKey       string `json:"source_key"`
	// SourceVersion / SourceDigest / SourceSize / SourceChunks 为建立时冻结的来源快照。
	SourceVersion uint64     `json:"source_version"`
	SourceDigest  Digest     `json:"source_digest"`
	SourceSize    int64      `json:"source_size"`
	SourceChunks  []ChunkRef `json:"source_chunks"`

	// TargetNamespace / TargetKey 目标条目身份。
	TargetNamespace string `json:"target_namespace"`
	TargetKey       string `json:"target_key"`
	// Mode copy / replace。
	Mode string `json:"mode"`
	// TargetBaseVersion 提交时目标键必须处于的条目版本：copy 为 0（不存在），
	// replace 为建立时旧条目版本（旧条目不存在时为 0）。
	TargetBaseVersion uint64 `json:"target_base_version"`
	// TargetExistingDigest 建立时目标旧条目摘要（不存在为空）。
	TargetExistingDigest Digest `json:"target_existing_digest,omitempty"`
	// TargetExistingSize 建立时目标旧条目体量（不存在为 0），用于恢复时按实际用量复核配额。
	TargetExistingSize int64 `json:"target_existing_size,omitempty"`

	// Quota 建立时目标配额版本快照。
	Quota PromotionQuota `json:"quota"`
	// UsedBefore 建立时目标命名空间已用字节。
	UsedBefore int64 `json:"used_before"`
	// UsedAfter 提交后目标命名空间已用字节（仅 committed 后填充）。
	UsedAfter int64 `json:"used_after,omitempty"`

	// Eviction 提交前按目标配额计算的淘汰决策（不需要淘汰时省略）。
	Eviction *EvictionDecision `json:"eviction,omitempty"`

	// Status prepared / committed / failed。
	Status     string `json:"status"`
	FailReason string `json:"fail_reason,omitempty"`
	// TargetVersion 提交后目标条目版本（copy 为 1，replace 为旧版本+1）。
	TargetVersion uint64    `json:"target_version,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	CommittedAt   time.Time `json:"committed_at,omitempty"`
}

// PromotionRequest 记录晋级外部请求号（幂等）：相同请求号 + 相同请求指纹返回首次结果；
// 相同请求号 + 不同指纹按冲突拒绝。即使首次结果是 failed，也记录并原样回放。
type PromotionRequest struct {
	RequestID   string `json:"request_id"`
	Fingerprint string `json:"fingerprint"`
	// PromotionID 首次结果对应的晋级决策 ID。
	PromotionID string    `json:"promotion_id"`
	CreatedAt   time.Time `json:"created_at"`
}

// PromotionTxn 是晋级提交的重做日志（WAL）。仅当提交真正开始（通过全部校验、
// 即将产生首个变更）时才创建；因此"只建立未提交"的 prepared 晋级在重启后仍保持
// prepared，不会被自动提交；而存在该日志说明提交在进行中崩溃，恢复时重做。
//
// 每个阶段完成后推进 Phase 并 fsync；恢复时结合实际状态（淘汰是否已 committed、
// 目标条目是否已带晋级标记）跳过已完成步骤，最终 Promotion 进入 committed。
// 任一步持久化失败都沿同一序列重试，已提交端不会回滚、未提交端不留痕。
type PromotionTxn struct {
	PromotionID string `json:"promotion_id"`
	// Phase committing / eviction_committed / entry_written。
	Phase     string    `json:"phase"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PromotionTxn 阶段值。
const (
	// promoTxnCommitting 提交已开始，首个变更（淘汰）尚未确认完成。
	promoTxnCommitting = "committing"
	// promoTxnEviction 配套淘汰已提交。
	promoTxnEviction = "eviction_committed"
	// promoTxnEntry 目标条目已写入。
	promoTxnEntry = "entry_written"
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
	RefEntry     = "entry"
	RefSession   = "session"
	RefPin       = "pin"
	RefPromotion = "promotion" // prepared 晋级的冻结来源快照（跨命名空间内容保护）
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
	GCPromotion    = "promotion" // 跨命名空间晋级（准备/提交/失败，详情记于 Detail）
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
