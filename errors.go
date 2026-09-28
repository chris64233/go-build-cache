package buildcache

import "errors"

// 哨兵错误：可用 errors.Is 判定错误类别。
var (
	// ErrSessionNotFound 会话不存在（或已被彻底清理）。
	ErrSessionNotFound = errors.New("buildcache: upload session not found")
	// ErrEntryNotFound 缓存键尚未发布，读者看不到任何未完成的数据。
	ErrEntryNotFound = errors.New("buildcache: cache entry not found")
	// ErrSessionNotActive 会话已取消或已完成，不能再接收分片。
	ErrSessionNotActive = errors.New("buildcache: upload session is not open")
	// ErrLeaseExpired 会话租约到期，所有续期/上传/完成都会被拒绝，且不会复活。
	ErrLeaseExpired = errors.New("buildcache: upload session lease expired")
	// ErrNamespaceNotFound 命名空间尚未注册。
	ErrNamespaceNotFound = errors.New("buildcache: namespace not found")
	// ErrPinNotFound 指定键不存在任何固定租约。
	ErrPinNotFound = errors.New("buildcache: pin not found")
	// ErrPinNotActive 固定租约已解除或已过期：不能续租；如需保护请重新固定。
	ErrPinNotActive = errors.New("buildcache: pin is not active")
	// ErrPromotionNotFound 晋级请求号不存在。
	ErrPromotionNotFound = errors.New("buildcache: promotion request not found")
)

// DigestMismatchError 表示实际计算出的摘要与声明的预期摘要不一致。
// 既可能发生在单片上传（声明了单片摘要时），也可能发生在完成拼接校验时。
type DigestMismatchError struct {
	Operation string // "upload_chunk" / "complete"
	Key       string
	Want      Digest
	Got       Digest
}

func (e *DigestMismatchError) Error() string {
	return "buildcache: digest mismatch during " + e.Operation +
		": want " + e.Want.String() + ", got " + e.Got.String()
}

// SizeMismatchError 表示拼接后的总大小与创建会话时声明的总大小不一致。
type SizeMismatchError struct {
	Want int64
	Got  int64
}

func (e *SizeMismatchError) Error() string {
	return "buildcache: total size mismatch: want " +
		itoa(e.Want) + ", got " + itoa(e.Got)
}

// ChunkConflictError 表示分片冲突：
//   - ReasonContentMismatch：同一分片号已经上传过不同内容；
//   - ReasonSizeMismatch：上传分片大小与会话声明的分片大小不符；
//   - ReasonIncomplete：完成时分片尚未到齐（Missing 给出缺失分片号）。
//
// 相同内容重传不构成冲突，按幂等处理。
type ChunkConflictError struct {
	Index        int
	Missing      []int
	Reason       string
	ExistingSize int64
	GotSize      int64
	Existing     Digest
	Got          Digest
}

const (
	ReasonContentMismatch = "content_mismatch"
	ReasonSizeMismatch    = "size_mismatch"
	ReasonIncomplete      = "incomplete"
)

func (e *ChunkConflictError) Error() string {
	switch e.Reason {
	case ReasonSizeMismatch:
		return "buildcache: chunk " + itoa(int64(e.Index)) +
			" size mismatch: want " + itoa(e.ExistingSize) + ", got " + itoa(e.GotSize)
	case ReasonIncomplete:
		return "buildcache: session incomplete: missing chunks " + intsToString(e.Missing)
	default:
		return "buildcache: chunk " + itoa(int64(e.Index)) +
			" conflict: existing " + e.Existing.String() + ", got " + e.Got.String()
	}
}

// VersionConflictError 表示并发发布冲突：
// 同一缓存键已存在不同摘要的条目，且调用方没有给出匹配的版本条件（CAS）。
type VersionConflictError struct {
	Key             string
	CurrentVersion  uint64
	CurrentDigest   Digest
	ExpectedVersion *uint64 // 调用方提供的条件，nil 表示无条件发布
	AttemptDigest   Digest
}

func (e *VersionConflictError) Error() string {
	if e.ExpectedVersion == nil {
		return "buildcache: key " + e.Key + " already published with a different digest (" +
			e.CurrentDigest.String() + "); a matching version condition is required to overwrite"
	}
	return "buildcache: key " + e.Key + " version conflict: expected v" +
		itoa(int64(*e.ExpectedVersion)) + ", current v" + itoa(int64(e.CurrentVersion))
}

// IdempotencyConflictError 表示同一个幂等键被用于创建参数不同的会话。
type IdempotencyConflictError struct {
	IdempotencyKey string
	Existing       string // 已存在会话的 ID
}

func (e *IdempotencyConflictError) Error() string {
	return "buildcache: idempotency key " + e.IdempotencyKey +
		" was already used with different parameters (session " + e.Existing + ")"
}

// QuotaExceededError 表示命名空间配额不足，且按 LRU 淘汰（仅未固定条目）
// 也无法释放出足够空间。
type QuotaExceededError struct {
	Namespace  string
	MaxBytes   int64
	UsedBytes  int64
	NeedBytes  int64 // 本次新发布需要额外占用的字节数
	FreedBytes int64 // 本次淘汰实际释放的字节数
}

func (e *QuotaExceededError) Error() string {
	return "buildcache: namespace " + e.Namespace + " quota exceeded: max " +
		itoa(e.MaxBytes) + ", used " + itoa(e.UsedBytes) +
		", need additional " + itoa(e.NeedBytes) + ", freed " + itoa(e.FreedBytes)
}

// PinDigestConflictError 表示固定请求指定的摘要与该键当前已发布条目的摘要不一致：
// 固定不得作用于摘要不匹配的条目。
type PinDigestConflictError struct {
	Namespace      string
	Key            string
	ExpectedDigest Digest // 请求要求的摘要
	CurrentDigest  Digest // 当前条目摘要
}

func (e *PinDigestConflictError) Error() string {
	return "buildcache: pin digest mismatch for " + e.Namespace + "/" + e.Key +
		": want " + e.ExpectedDigest.String() + ", current " + e.CurrentDigest.String()
}

// PinConflictError 表示固定类请求与当前租约状态冲突，例如对不存在条目的键固定、
// 对已解除租约续租等。Reason 给出机器可读类别。
type PinConflictError struct {
	Namespace string
	Key       string
	Reason    string
	Detail    string
}

const (
	// PinReasonNoEntry 被固定的键当前没有已发布条目。
	PinReasonNoEntry = "no_entry"
	// PinReasonNotActive 租约不活跃（已解除/已过期），不能续租。
	PinReasonNotActive = "not_active"
	// PinReasonDeadlineInPast 续租/固定给出的截止时间不晚于当前时间。
	PinReasonDeadlineInPast = "deadline_in_past"
)

func (e *PinConflictError) Error() string {
	return "buildcache: pin conflict for " + e.Namespace + "/" + e.Key +
		": " + e.Reason + " " + e.Detail
}

// PinVersionConflictError 表示固定类请求携带的租约版本条件与当前版本不一致
// （或已过期），旧版本操作被拒绝：它既不能缩短也不能复活新租约。
type PinVersionConflictError struct {
	Namespace       string
	Key             string
	ExpectedVersion uint64 // 调用方条件；0 表示未提供
	CurrentVersion  uint64
	CurrentStatus   string
}

func (e *PinVersionConflictError) Error() string {
	return "buildcache: pin version conflict for " + e.Namespace + "/" + e.Key +
		": expected v" + itoa(int64(e.ExpectedVersion)) +
		", current v" + itoa(int64(e.CurrentVersion)) + " (" + e.CurrentStatus + ")"
}

// PinRequestConflictError 表示相同请求号被用于不同请求内容（指纹不同）。
type PinRequestConflictError struct {
	RequestID string
}

func (e *PinRequestConflictError) Error() string {
	return "buildcache: pin request id " + e.RequestID + " was already used with different parameters"
}

// PromotionConflictError 表示晋级请求与当前状态冲突，Reason 给出机器可读类别。
type PromotionConflictError struct {
	Reason    string
	RequestID string
	Detail    string
}

const (
	// PromotionReasonMissing 来源命名空间没有该已发布条目。
	PromotionReasonMissing = "source_missing"
	// PromotionReasonDigest 来源条目当前摘要与请求预期摘要不符。
	PromotionReasonDigest = "source_digest_mismatch"
	// PromotionReasonUploading 来源键在来源命名空间内有正在进行的上传会话，暂不能晋级。
	PromotionReasonUploading = "source_uploading"
	// PromotionReasonTargetExists copy 模式下目标键已存在。
	PromotionReasonTargetExists = "target_exists"
	// PromotionReasonSameEntry 来源与目标指向同一条目（同命名空间同键）。
	PromotionReasonSameEntry = "same_entry"
)

func (e *PromotionConflictError) Error() string {
	return "buildcache: promotion conflict (" + e.Reason + "): " + e.Detail
}

// PromotionRequestConflictError 表示相同晋级请求号被用于不同请求内容（指纹不同）。
type PromotionRequestConflictError struct {
	RequestID string
}

func (e *PromotionRequestConflictError) Error() string {
	return "buildcache: promotion request id " + e.RequestID + " was already used with different parameters"
}

// PromotionStaleError 表示旧晋级请求依据的状态已经变化，重放必须明确失败
// 而不能覆盖新状态：来源摘要变化、目标配额版本变化、目标条目已被推进到更新版本。
type PromotionStaleError struct {
	Reason    string
	RequestID string
	Detail    string
}

const (
	// PromotionStaleSource 来源条目摘要相对请求冻结快照已变化。
	PromotionStaleSource = "source_changed"
	// PromotionStaleQuota 目标命名空间配额版本相对请求建立时已变化。
	PromotionStaleQuota = "quota_version_changed"
	// PromotionStaleTarget replace 模式下目标条目版本相对请求建立时已变化。
	PromotionStaleTarget = "target_changed"
	// PromotionStaleEviction 请求内冻结的旧淘汰决定在提交时已失效且无法满足配额。
	PromotionStaleEviction = "eviction_decision_stale"
)

func (e *PromotionStaleError) Error() string {
	return "buildcache: promotion request " + e.RequestID + " is stale (" + e.Reason + "): " + e.Detail
}
