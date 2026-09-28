package buildcache

import (
	"errors"
	"io"
)

// ErrBlobNotFound 表示内容寻址存储中不存在指定块。
var ErrBlobNotFound = errors.New("buildcache: blob not found")

// ErrCASFailed 表示条目的版本条件写入（CAS）失败。
var ErrCASFailed = errors.New("buildcache: entry version condition failed")

// ErrNotFound 通用的"对象不存在"，用于命名空间 / 固定 / 决策等辅助元数据。
var ErrNotFound = errors.New("buildcache: object not found")

// BlobInfo 描述内容寻址存储中的一个块。
type BlobInfo struct {
	Digest Digest
	Size   int64
}

// Store 是缓存服务的持久化抽象。
//
// 约定：
//   - 块按内容摘要寻址，PutBlob 必须幂等（同摘要重复写入等同 no-op）；
//   - SaveSession / PutEntry / 命名空间 / 固定等元数据写必须是原子的，崩溃不得半写；
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

	// ---- 命名空间（配额）----
	SaveNamespace(ns Namespace) error
	GetNamespace(name string) (Namespace, error)
	ListNamespaces() ([]Namespace, error)
	DeleteNamespace(name string) error

	// ---- 已发布条目（身份为 命名空间 + 键）----
	// PutEntry 按版本条件原子写入：
	// wantVersion < 0 表示仅允许新建（该命名空间下键必须不存在）；
	// wantVersion >=0 表示当前版本必须恰好等于 wantVersion。
	PutEntry(e Entry, wantVersion int64) error
	GetEntry(namespace, key string) (Entry, error)
	DeleteEntry(namespace, key string) error
	ListEntries() ([]Entry, error)

	// ---- 固定租约（身份为 命名空间 + 键）----
	SavePin(p Pin) error
	GetPin(namespace, key string) (Pin, error)
	DeletePin(namespace, key string) error
	ListPins() ([]Pin, error)

	// ---- 固定类请求号（幂等记录）----
	SavePinRequest(rec PinRequest) error
	GetPinRequest(requestID string) (PinRequest, error)
	DeletePinRequest(requestID string) error
	ListPinRequests() ([]PinRequest, error)

	// ---- 淘汰决策（两阶段、可审计）----
	SaveEvictionDecision(d EvictionDecision) error
	GetEvictionDecision(id string) (EvictionDecision, error)
	ListEvictionDecisions() ([]EvictionDecision, error)
	DeleteEvictionDecision(id string) error

	// ---- 跨命名空间晋级（两阶段、可审计）----
	SavePromotion(p Promotion) error
	GetPromotion(id string) (Promotion, error)
	ListPromotions() ([]Promotion, error)
	DeletePromotion(id string) error

	// ---- 晋级外部请求号（幂等记录）----
	SavePromotionRequest(rec PromotionRequest) error
	GetPromotionRequest(requestID string) (PromotionRequest, error)
	ListPromotionRequests() ([]PromotionRequest, error)

	// ---- 晋级提交重做日志（崩溃恢复）----
	SavePromotionTxn(txn PromotionTxn) error
	GetPromotionTxn(promotionID string) (PromotionTxn, error)
	ListPromotionTxns() ([]PromotionTxn, error)
	DeletePromotionTxn(promotionID string) error

	// ---- 审计 ----
	AppendAudit(rec GCRecord) error
	ListAudit() ([]GCRecord, error)

	// Close 释放底层资源。
	Close() error
}
