package buildcache

import (
	"errors"
	"io"
)

// ErrBlobNotFound 表示内容寻址存储中不存在指定块。
var ErrBlobNotFound = errors.New("buildcache: blob not found")

// ErrCASFailed 表示条目的版本条件写入（CAS）失败。
var ErrCASFailed = errors.New("buildcache: entry version condition failed")

// BlobInfo 描述内容寻址存储中的一个块。
type BlobInfo struct {
	Digest Digest
	Size   int64
}

// Store 是缓存服务的持久化抽象。
//
// 约定：
//   - 块按内容摘要寻址，PutBlob 必须幂等（同摘要重复写入等同 no-op）；
//   - SaveSession / PutEntry / DeleteEntry 必须是原子的，崩溃不得产生半写文件；
//   - 并发安全由 Cache 层在更上层串行化保证，实现本身仍应支持并发访问
//     （文件实现依赖原子 rename 与 O_APPEND）。
type Store interface {
	// ---- 内容寻址块 ----
	PutBlob(d Digest, data []byte) error
	GetBlob(d Digest) (io.ReadCloser, error)
	BlobSize(d Digest) (int64, error)
	DeleteBlob(d Digest) error
	ListBlobs() ([]BlobInfo, error)

	// ---- 会话元数据 ----
	SaveSession(s Session) error
	GetSession(id string) (Session, error)
	DeleteSession(id string) error
	ListSessions() ([]Session, error)

	// ---- 已发布条目（按命名空间隔离）----
	// PutEntry 按版本条件原子写入：
	// wantVersion < 0 表示仅允许新建（命名空间内该键必须不存在）；
	// wantVersion >=0 表示当前版本必须恰好等于 wantVersion。
	PutEntry(e Entry, wantVersion int64) error
	GetEntry(namespace, key string) (Entry, error)
	DeleteEntry(namespace, key string) error
	ListEntries() ([]Entry, error)
	ListNamespaceEntries(namespace string) ([]Entry, error)

	// ---- 命名空间配额 ----
	// SaveNamespace 按名字 upsert 命名空间配额元数据。
	SaveNamespace(ns Namespace) error
	GetNamespace(name string) (Namespace, error)
	ListNamespaces() ([]Namespace, error)

	// ---- 固定租约 ----
	SavePin(p PinLease) error
	GetPin(namespace, key string) (PinLease, error)
	DeletePin(namespace, key string) error
	ListPins() ([]PinLease, error)

	// ---- 审计 ----
	AppendAudit(rec GCRecord) error
	ListAudit() ([]GCRecord, error)

	// Close 释放底层资源。
	Close() error
}
