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
