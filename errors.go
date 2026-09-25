package buildcache

import (
	"errors"
	"fmt"
)

// 哨兵错误。调用方可用 errors.Is 判定错误类别。
var (
	// ErrSessionNotFound 会话不存在（或从未创建）。
	ErrSessionNotFound = errors.New("buildcache: upload session not found")
	// ErrEntryNotFound 缓存键没有已发布的条目。
	ErrEntryNotFound = errors.New("buildcache: cache entry not found")
	// ErrLeaseExpired 会话租约已过期，任何续期/上传/完成都会被拒绝，且不会“复活”。
	ErrLeaseExpired = errors.New("buildcache: upload session lease expired")
	// ErrSessionCompleted 会话已完成发布，不能再取消或上传。
	ErrSessionCompleted = errors.New("buildcache: upload session already completed")
	// ErrSessionCancelled 会话已被取消。
	ErrSessionCancelled = errors.New("buildcache: upload session cancelled")
	// ErrChunkIndex 上传了会话声明之外的分片号。
	ErrChunkIndex = errors.New("buildcache: chunk index out of declared range")
	// ErrGCBusy 已有一轮垃圾回收在运行。
	ErrGCBusy = errors.New("buildcache: garbage collection already in progress")
	// ErrInvalidArgument 请求参数不合法（如分片声明有空洞、摘要格式错误）。
	ErrInvalidArgument = errors.New("buildcache: invalid argument")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("buildcache: store closed")
)

// DigestMismatchError 表示摘要校验失败：
// 既可能是单个分片内容与声明不符，也可能是拼接后的最终摘要/总大小与预期不符。
type DigestMismatchError struct {
	// Scope 为 "chunk" 时表示分片摘要/大小不符；为 "entry" 时表示最终拼接结果不符。
	Scope string
	// Index 仅在 Scope=="chunk" 时有意义。
	Index int
	// Expected 声明的摘要。
	Expected Digest
	// Got 实际计算出的摘要。
	Got Digest
	// ExpectedSize/GotSize 在大小不符时填充（可能为 0 表示未涉及）。
	ExpectedSize int64
	GotSize      int64
}

func (e *DigestMismatchError) Error() string {
	if e.Scope == "chunk" {
		return fmt.Sprintf("buildcache: chunk %d digest/size mismatch: expected %s (%d bytes), got %s (%d bytes)",
			e.Index, e.Expected, e.ExpectedSize, e.Got, e.GotSize)
	}
	return fmt.Sprintf("buildcache: entry digest/size mismatch: expected %s (%d bytes), got %s (%d bytes)",
		e.Expected, e.ExpectedSize, e.Got, e.GotSize)
}

// ChunkConflictError 是幂等冲突：同一个分片号此前已上传过内容 A，
// 现在又上传了不同的内容 B。相同内容重传不会触发该错误。
type ChunkConflictError struct {
	SessionID      string
	Index          int
	ExistingDigest Digest
	IncomingDigest Digest
}

func (e *ChunkConflictError) Error() string {
	return fmt.Sprintf("buildcache: idempotency conflict on session %s chunk %d: already stored %s, got %s",
		e.SessionID, e.Index, e.ExistingDigest, e.IncomingDigest)
}

// ChunkMissingError 表示完成时仍有分片未上传（或其内容块已不可读）。
type ChunkMissingError struct {
	SessionID string
	Missing   []int
}

func (e *ChunkMissingError) Error() string {
	return fmt.Sprintf("buildcache: session %s cannot complete, missing chunks: %v", e.SessionID, e.Missing)
}

// VersionConflictError 表示版本条件失败：缓存键已有条目，
// 且已发布摘要与本次会话声明的摘要不同。内容寻址缓存中的键不可被异值覆盖。
type VersionConflictError struct {
	Key             string
	ExistingVersion uint64
	ExistingDigest  Digest
	RequestedDigest Digest
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf("buildcache: version conflict on key %q: published version %d has digest %s, refusing overwrite with %s",
		e.Key, e.ExistingVersion, e.ExistingDigest, e.RequestedDigest)
}
