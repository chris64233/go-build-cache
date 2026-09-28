package buildcache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// DefaultSessionTTL 是创建会话时未显式指定租约时长时使用的默认值。
const DefaultSessionTTL = 15 * time.Minute

// DefaultPinTTL 是固定请求未显式指定截止时间/时长时使用的默认租约长度。
const DefaultPinTTL = time.Hour

// CreateSessionOptions 是创建上传会话的参数。
type CreateSessionOptions struct {
	// Namespace 目标命名空间；空表示 DefaultNamespace。命名空间必须已注册。
	Namespace string
	// Key 缓存键；FinalDigest 最终制品的预期摘要；TotalSize 制品总字节数。
	Key         string
	FinalDigest Digest
	TotalSize   int64
	// Chunks 分片集合：分片号、偏移、大小、单片摘要。偏移必须连续覆盖整个制品。
	Chunks []ChunkSpec
	// IdempotencyKey 可选的幂等键：相同幂等键 + 相同参数返回同一会话，
	// 相同幂等键 + 不同参数返回 IdempotencyConflictError。
	IdempotencyKey string
	// LeaseDuration 租约时长；<=0 时使用 DefaultSessionTTL。
	LeaseDuration time.Duration
}

// CompleteOptions 控制完成发布时的版本条件。
type CompleteOptions struct {
	// ExpectedVersion 为 nil 表示"仅接受新建或同摘要复用"；
	// 非 nil 时，若该键已存在不同摘要的条目，当前版本必须等于 *ExpectedVersion 才允许覆盖。
	ExpectedVersion *uint64
}

// PublishResult 是完成操作的结果。
type PublishResult struct {
	Entry  Entry `json:"entry"`
	Reused bool  `json:"reused"` // true 表示已有同摘要条目，本次直接复用，没有新建版本
	// Eviction 是发布引发配额不足时内部执行的淘汰决策（未发生淘汰时省略）。
	Eviction *EvictionDecision `json:"eviction,omitempty"`
}

// GCReport 汇总一次垃圾回收的决策，与审计日志内容对应。
type GCReport struct {
	Generation    uint64
	StartedAt     time.Time
	FinishedAt    time.Time
	LiveBlobCount int
	DeletedBlobs  []DeletedBlob
	SweptSessions []SweptSession
	SweptPins     []Pin
}

// DeletedBlob 记录一个被删除的内容块及其判定原因。
type DeletedBlob struct {
	Digest Digest
	Size   int64
	Reason string
}

// SweptSession 记录一个被清理的会话。
type SweptSession struct {
	ID     string
	Key    string
	Status string
	Reason string
}

// Cache 是内容寻址构建缓存服务。
//
// 所有改变状态的操作（上传、完成、取消、清理、GC、固定、淘汰）都在同一把互斥锁下
// 串行，因此"建立引用"（complete 发布条目 / pin 固定）与"判定/删除块/淘汰条目"
// 不可能交错：任何两阶段决策（GC mark→sweep、淘汰 plan→commit）在提交阶段都会基于
// 最新状态逐项复核，快照之后发生的访问 / 固定 / 重新发布都会让旧决定失效。
type Cache struct {
	store Store
	clock Clock
	ttl   time.Duration

	mu sync.Mutex
	// idem 幂等键 -> 会话 ID；键为 "namespace\x00idempotencyKey" 复合形式。
	idem map[string]string
	// pinReqs 请求号 -> 固定类请求记录（重启后从存储恢复）。
	pinReqs map[string]PinRequest
	gcGen   uint64 // GC 代次，重启后由审计日志恢复
	// evictSeq 淘汰决策序号，重启后由已持久化决策恢复。
	evictSeq uint64
}

// New 创建缓存服务。clock 为 nil 时使用 SystemClock；store 由调用方提供。
// 启动时会自动确保默认命名空间（不限配额）存在。
func New(store Store, clock Clock, ttl time.Duration) (*Cache, error) {
	if store == nil {
		return nil, errors.New("buildcache: store is required")
	}
	if clock == nil {
		clock = SystemClock{}
	}
	if ttl <= 0 {
		ttl = DefaultSessionTTL
	}
	c := &Cache{
		store: store, clock: clock, ttl: ttl,
		idem:    make(map[string]string),
		pinReqs: make(map[string]PinRequest),
	}
	if err := c.restoreIndex(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cache) restoreIndex() error {
	sessions, err := c.store.ListSessions()
	if err != nil {
		return fmt.Errorf("buildcache: restore sessions: %w", err)
	}
	for _, s := range sessions {
		if s.IdempotencyKey != "" && s.Status == SessionOpen {
			c.idem[idemComposite(s.Namespace, s.IdempotencyKey)] = s.ID
		}
	}
	records, err := c.store.ListAudit()
	if err != nil {
		return fmt.Errorf("buildcache: restore audit: %w", err)
	}
	for _, r := range records {
		if r.Generation > c.gcGen {
			c.gcGen = r.Generation
		}
	}
	reqs, err := c.store.ListPinRequests()
	if err != nil {
		return fmt.Errorf("buildcache: restore pin requests: %w", err)
	}
	for _, r := range reqs {
		c.pinReqs[r.RequestID] = r
	}
	decisions, err := c.store.ListEvictionDecisions()
	if err != nil {
		return fmt.Errorf("buildcache: restore evictions: %w", err)
	}
	for _, d := range decisions {
		if n := evictionSeqOf(d.ID); n > c.evictSeq {
			c.evictSeq = n
		}
	}
	// 默认命名空间：不存在则注册为不限配额。
	if _, err := c.store.GetNamespace(DefaultNamespace); errors.Is(err, ErrNotFound) {
		now := c.clock.Now()
		if err := c.store.SaveNamespace(Namespace{
			Name: DefaultNamespace, MaxBytes: 0, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			return fmt.Errorf("buildcache: ensure default namespace: %w", err)
		}
	} else if err != nil {
		return err
	}
	return nil
}

func idemComposite(namespace, key string) string { return namespace + "\x00" + key }

// evictionSeqOf 从决策 ID（"evict-<零填充序号>"）解析序号；无法识别时返回 0。
func evictionSeqOf(id string) uint64 {
	const prefix = "evict-"
	if len(id) <= len(prefix) || id[:len(prefix)] != prefix {
		return 0
	}
	var n uint64
	for _, ch := range id[len(prefix):] {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + uint64(ch-'0')
	}
	return n
}

// Clock 暴露统一时间来源（测试/租约判定共用）。
func (c *Cache) Clock() Clock { return c.clock }

func normNamespace(ns string) string {
	if ns == "" {
		return DefaultNamespace
	}
	return ns
}

// requireNamespaceLocked 返回命名空间元数据，不存在则 ErrNamespaceNotFound。
func (c *Cache) requireNamespaceLocked(name string) (Namespace, error) {
	ns, err := c.store.GetNamespace(name)
	if errors.Is(err, ErrNotFound) {
		return Namespace{}, fmt.Errorf("%w: %s", ErrNamespaceNotFound, name)
	}
	return ns, err
}

// ---- 命名空间与配额 ----

// SetNamespaceQuota 创建命名空间或更新其最大可用字节数（maxBytes<=0 表示不限）。
func (c *Cache) SetNamespaceQuota(name string, maxBytes int64) (*Namespace, error) {
	if name == "" {
		return nil, errors.New("buildcache: namespace name is required")
	}
	if maxBytes < 0 {
		return nil, errors.New("buildcache: max bytes must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	ns, err := c.store.GetNamespace(name)
	switch {
	case errors.Is(err, ErrNotFound):
		ns = Namespace{Name: name, MaxBytes: maxBytes, CreatedAt: now, UpdatedAt: now}
	case err != nil:
		return nil, err
	default:
		ns.MaxBytes = maxBytes
		ns.UpdatedAt = now
	}
	if err := c.store.SaveNamespace(ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

// GetNamespace 返回命名空间配额元数据。
func (c *Cache) GetNamespace(name string) (Namespace, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requireNamespaceLocked(normNamespace(name))
}

// DeleteNamespace 删除空命名空间；仍有条目或活跃固定时拒绝。
func (c *Cache) DeleteNamespace(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.requireNamespaceLocked(name); err != nil {
		return err
	}
	entries, err := c.store.ListEntries()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Namespace == name {
			return fmt.Errorf("buildcache: namespace %s not empty: entry %q still exists", name, e.Key)
		}
	}
	pins, err := c.store.ListPins()
	if err != nil {
		return err
	}
	now := c.clock.Now()
	for _, p := range pins {
		if p.Namespace != name {
			continue
		}
		if p.Active(now) {
			return fmt.Errorf("buildcache: namespace %s not empty: key %q is pinned", name, p.Key)
		}
		// 终态（released/expired）固定记录随命名空间一并清理。
		if err := c.store.DeletePin(name, p.Key); err != nil {
			return err
		}
	}
	return c.store.DeleteNamespace(name)
}

// NamespaceStatus 是配额查询结果。
type NamespaceStatus struct {
	Namespace   string    `json:"namespace"`
	MaxBytes    int64     `json:"max_bytes"`
	UsedBytes   int64     `json:"used_bytes"`
	FreeBytes   int64     `json:"free_bytes"` // -1 表示不限配额
	EntryCount  int       `json:"entry_count"`
	PinnedKeys  int       `json:"pinned_keys"`
	PinnedBytes int64     `json:"pinned_bytes"` // 活跃固定快照引用的去重块体量（按条目体量近似）
	UpdatedAt   time.Time `json:"updated_at"`
}

// NamespaceStatus 返回配额占用：上限、已用、固定保护体量。
func (c *Cache) NamespaceStatus(name string) (*NamespaceStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	name = normNamespace(name)
	ns, err := c.requireNamespaceLocked(name)
	if err != nil {
		return nil, err
	}
	return c.namespaceStatusLocked(ns)
}

// ListNamespaceStatus 列出全部命名空间的配额状态。
func (c *Cache) ListNamespaceStatus() ([]NamespaceStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nss, err := c.store.ListNamespaces()
	if err != nil {
		return nil, err
	}
	out := make([]NamespaceStatus, 0, len(nss))
	for _, ns := range nss {
		st, err := c.namespaceStatusLocked(ns)
		if err != nil {
			return nil, err
		}
		out = append(out, *st)
	}
	return out, nil
}

func (c *Cache) namespaceStatusLocked(ns Namespace) (*NamespaceStatus, error) {
	now := c.clock.Now()
	st := &NamespaceStatus{
		Namespace: ns.Name, MaxBytes: ns.MaxBytes,
		FreeBytes: -1, UpdatedAt: ns.UpdatedAt,
	}
	entries, err := c.store.ListEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Namespace != ns.Name {
			continue
		}
		st.UsedBytes += e.TotalSize
		st.EntryCount++
	}
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	for _, p := range pins {
		if p.Namespace != ns.Name || !p.Active(now) {
			continue
		}
		st.PinnedKeys++
		var n int64
		for _, ch := range p.Chunks {
			n += ch.Size
		}
		st.PinnedBytes += n
	}
	if ns.MaxBytes > 0 {
		st.FreeBytes = ns.MaxBytes - st.UsedBytes
	}
	return st, nil
}

// CreateSession 创建分片上传会话。
func (c *Cache) CreateSession(opts CreateSessionOptions) (*Session, error) {
	if opts.Key == "" {
		return nil, errors.New("buildcache: cache key is required")
	}
	if !opts.FinalDigest.Valid() {
		return nil, errors.New("buildcache: valid final digest is required")
	}
	if opts.TotalSize <= 0 {
		return nil, errors.New("buildcache: total size must be positive")
	}
	chunks, err := normalizeChunks(opts.Chunks, opts.TotalSize, opts.FinalDigest.Algo)
	if err != nil {
		return nil, err
	}
	nsName := normNamespace(opts.Namespace)

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.requireNamespaceLocked(nsName); err != nil {
		return nil, err
	}
	now := c.clock.Now()

	if opts.IdempotencyKey != "" {
		comp := idemComposite(nsName, opts.IdempotencyKey)
		if existingID, ok := c.idem[comp]; ok {
			existing, gerr := c.store.GetSession(existingID)
			if gerr == nil && existing.Status == SessionOpen && !c.expired(existing, now) {
				if sameParams(existing, opts) {
					return &existing, nil // 幂等命中：原样返回
				}
				return nil, &IdempotencyConflictError{
					IdempotencyKey: opts.IdempotencyKey,
					Existing:       existing.ID,
				}
			}
			// 指向的会话已失效：删除陈旧映射，允许重新创建。
			delete(c.idem, comp)
		}
	}

	id, err := newSessionID()
	if err != nil {
		return nil, err
	}
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = c.ttl
	}
	sess := Session{
		ID:             id,
		Namespace:      nsName,
		Key:            opts.Key,
		IdempotencyKey: opts.IdempotencyKey,
		TotalSize:      opts.TotalSize,
		FinalDigest:    opts.FinalDigest,
		Chunks:         chunks,
		CreatedAt:      now,
		ExpiresAt:      now.Add(lease),
		Status:         SessionOpen,
		Received:       make(map[int]ChunkReceipt),
	}
	if err := c.store.SaveSession(sess); err != nil {
		return nil, err
	}
	if opts.IdempotencyKey != "" {
		c.idem[idemComposite(nsName, opts.IdempotencyKey)] = id
	}
	return &sess, nil
}

// GetSession 返回会话当前状态（只读副本）。
func (c *Cache) GetSession(id string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.GetSession(id)
}

// UploadChunk 上传一个分片。
//
// 分片可以乱序到达；相同内容重传按幂等成功处理；
// 同一分片号上传不同内容返回 ChunkConflictError（ReasonContentMismatch）。
// 数据只写入内容寻址块存储并登记在会话上，在 complete 成功前读者无法通过缓存键看到它。
func (c *Cache) UploadChunk(sessionID string, index int, data []byte) (ChunkReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sess, err := c.store.GetSession(sessionID)
	if err != nil {
		return ChunkReceipt{}, err
	}
	now := c.clock.Now()
	if err := c.ensureOpen(&sess, now); err != nil {
		return ChunkReceipt{}, err
	}

	spec, ok := chunkSpecByIndex(sess.Chunks, index)
	if !ok {
		return ChunkReceipt{}, fmt.Errorf("%w: chunk %d is not part of session %s",
			ErrChunkNotDeclared, index, sessionID)
	}
	if int64(len(data)) != spec.Size {
		return ChunkReceipt{}, &ChunkConflictError{
			Index:        index,
			Reason:       ReasonSizeMismatch,
			ExistingSize: spec.Size,
			GotSize:      int64(len(data)),
		}
	}
	got, err := hashBytes(spec.Digest.Algo, data)
	if err != nil {
		return ChunkReceipt{}, err
	}

	if rec, ok := sess.Received[index]; ok {
		// 同一分片号已上传过：内容相同 -> 幂等成功；内容不同 -> 冲突。
		if rec.Digest != got {
			return ChunkReceipt{}, &ChunkConflictError{
				Index: index, Reason: ReasonContentMismatch,
				Existing: rec.Digest, Got: got,
			}
		}
		return rec, nil
	}

	if got != spec.Digest {
		return ChunkReceipt{}, &DigestMismatchError{
			Operation: "upload_chunk", Key: sess.Key, Want: spec.Digest, Got: got,
		}
	}

	if err := c.store.PutBlob(got, data); err != nil {
		return ChunkReceipt{}, err
	}
	rec := ChunkReceipt{Index: index, Digest: got, Size: int64(len(data)), ReceivedAt: now}
	sess.Received[index] = rec
	if err := c.store.SaveSession(sess); err != nil {
		return ChunkReceipt{}, err
	}
	return rec, nil
}

// RenewSession 续期会话租约。只有"当前未过期"的 open 会话可以续期，
// 过期会话不会因任何操作复活。
func (c *Cache) RenewSession(sessionID string, ext time.Duration) (time.Time, error) {
	if ext <= 0 {
		ext = c.ttl
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	sess, err := c.store.GetSession(sessionID)
	if err != nil {
		return time.Time{}, err
	}
	now := c.clock.Now()
	if err := c.ensureOpen(&sess, now); err != nil {
		return time.Time{}, err
	}
	sess.ExpiresAt = now.Add(ext)
	if err := c.store.SaveSession(sess); err != nil {
		return time.Time{}, err
	}
	return sess.ExpiresAt, nil
}

// Complete 校验并原子发布：分片齐全、总大小一致、最终摘要一致后才发布条目。
func (c *Cache) Complete(sessionID string, opts CompleteOptions) (*PublishResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sess, err := c.store.GetSession(sessionID)
	if err != nil {
		return nil, err
	}
	now := c.clock.Now()
	if err := c.ensureOpen(&sess, now); err != nil {
		return nil, err
	}

	// 1) 分片齐全。
	var missing []int
	for _, spec := range sess.Chunks {
		if _, ok := sess.Received[spec.Index]; !ok {
			missing = append(missing, spec.Index)
		}
	}
	if len(missing) > 0 {
		return nil, &ChunkConflictError{Reason: ReasonIncomplete, Missing: missing}
	}

	// 2) 拼接后总大小（声明的偏移在创建时已校验为连续覆盖 TotalSize，
	//    此处再对实际到达分片求和，防止元数据被篡改）。
	var gotSize int64
	refs := make([]ChunkRef, 0, len(sess.Chunks))
	for _, spec := range sess.Chunks {
		rec := sess.Received[spec.Index]
		if rec.Size != spec.Size {
			return nil, &ChunkConflictError{
				Index: spec.Index, Reason: ReasonSizeMismatch,
				ExistingSize: spec.Size, GotSize: rec.Size,
			}
		}
		gotSize += rec.Size
		refs = append(refs, ChunkRef{Index: spec.Index, Size: rec.Size, Digest: rec.Digest})
	}
	if gotSize != sess.TotalSize {
		return nil, &SizeMismatchError{Want: sess.TotalSize, Got: gotSize}
	}

	// 3) 最终摘要：按分片号顺序流式拼接实际块内容计算。
	final, err := c.computeFinalDigest(sess)
	if err != nil {
		return nil, err
	}
	if final != sess.FinalDigest {
		return nil, &DigestMismatchError{
			Operation: "complete", Key: sess.Key, Want: sess.FinalDigest, Got: final,
		}
	}

	// 4) 带版本条件的原子发布（含配额检查与 LRU 淘汰）。
	result, err := c.publish(&sess, refs, now, opts)
	if err != nil {
		return nil, err
	}

	// 5) 发布成功后会话元数据即完成使命：条目是唯一的持久事实，删除会话记录，
	//    避免已完成会话无限堆积。若在发布与删除之间崩溃，恢复后会留下一个 open
	//    会话，但其内容已被条目引用；对它再次 complete 会命中"同摘要复用"路径并
	//    正常清理，不影响正确性。
	if err := c.store.DeleteSession(sess.ID); err != nil {
		return nil, err
	}
	if sess.IdempotencyKey != "" {
		delete(c.idem, idemComposite(sess.Namespace, sess.IdempotencyKey))
	}
	return result, nil
}

func (c *Cache) publish(sess *Session, refs []ChunkRef, now time.Time, opts CompleteOptions) (*PublishResult, error) {
	nsName := normNamespace(sess.Namespace)
	ns, err := c.requireNamespaceLocked(nsName)
	if err != nil {
		return nil, err
	}
	existing, err := c.store.GetEntry(nsName, sess.Key)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		if opts.ExpectedVersion != nil && *opts.ExpectedVersion != 0 {
			return nil, &VersionConflictError{
				Key: sess.Key, CurrentVersion: 0,
				ExpectedVersion: opts.ExpectedVersion, AttemptDigest: sess.FinalDigest,
			}
		}
		// 配额：投影用量 = 当前已用 + 新条目体量；不足则先淘汰未固定 LRU。
		if decision, err := c.enforceQuotaLocked(
			ns, map[string]bool{sess.Key: true}, 0, sess.TotalSize, now); err != nil {
			return nil, err
		} else {
			entry := Entry{
				Namespace: nsName, Key: sess.Key, Version: 1, Digest: sess.FinalDigest,
				TotalSize: sess.TotalSize, Chunks: refs,
				PublishedAt: now, LastAccessedAt: now,
			}
			if err := c.store.PutEntry(entry, -1); err != nil {
				return nil, err
			}
			return &PublishResult{Entry: entry, Reused: false, Eviction: decision}, nil
		}

	case err != nil:
		return nil, err

	case existing.Digest == sess.FinalDigest:
		// 摘要相同：直接复用现有结果，版本条件不再适用；重新发布视同一次访问。
		if !existing.LastAccessedAt.Equal(now) {
			existing.LastAccessedAt = now
			if err := c.store.PutEntry(existing, int64(existing.Version)); err != nil {
				return nil, err
			}
		}
		return &PublishResult{Entry: existing, Reused: true}, nil

	default:
		// 摘要不同：必须借助版本条件才能覆盖。
		if opts.ExpectedVersion == nil {
			return nil, &VersionConflictError{
				Key: sess.Key, CurrentVersion: existing.Version,
				CurrentDigest:   existing.Digest,
				ExpectedVersion: nil, AttemptDigest: sess.FinalDigest,
			}
		}
		if *opts.ExpectedVersion != existing.Version {
			return nil, &VersionConflictError{
				Key: sess.Key, CurrentVersion: existing.Version,
				CurrentDigest:   existing.Digest,
				ExpectedVersion: opts.ExpectedVersion, AttemptDigest: sess.FinalDigest,
			}
		}
		// 配额：投影用量 = 已用 - 旧版本体量 + 新版本体量。
		decision, err := c.enforceQuotaLocked(
			ns, map[string]bool{sess.Key: true}, existing.TotalSize, sess.TotalSize, now)
		if err != nil {
			return nil, err
		}
		entry := Entry{
			Namespace: nsName, Key: sess.Key, Version: existing.Version + 1, Digest: sess.FinalDigest,
			TotalSize: sess.TotalSize, Chunks: refs,
			PublishedAt: now, LastAccessedAt: now,
		}
		// CAS：即使绕过本进程锁（例如共享存储），旧版本也无法覆盖新版本。
		if err := c.store.PutEntry(entry, int64(existing.Version)); err != nil {
			if errors.Is(err, ErrCASFailed) {
				fresh, gerr := c.store.GetEntry(nsName, sess.Key)
				if gerr == nil {
					return nil, &VersionConflictError{
						Key: sess.Key, CurrentVersion: fresh.Version,
						CurrentDigest:   fresh.Digest,
						ExpectedVersion: opts.ExpectedVersion,
						AttemptDigest:   sess.FinalDigest,
					}
				}
			}
			return nil, err
		}
		return &PublishResult{Entry: entry, Reused: false, Eviction: decision}, nil
	}
}

// enforceQuotaLocked 在发布前确保投影用量不超过配额，必要时执行未固定 LRU 淘汰。
// exclude 中的键不参与淘汰（至少排除本次写入的键）；oldSize 是被覆盖旧版本体量
// （新建为 0），newSize 是即将发布的体量。调用方持有 c.mu。
func (c *Cache) enforceQuotaLocked(ns Namespace, exclude map[string]bool, oldSize, newSize int64,
	now time.Time) (*EvictionDecision, error) {
	if ns.MaxBytes <= 0 {
		return nil, nil
	}
	used, err := c.namespaceUsedLocked(ns.Name)
	if err != nil {
		return nil, err
	}
	projected := used - oldSize + newSize
	if projected <= ns.MaxBytes {
		return nil, nil
	}
	need := projected - ns.MaxBytes
	decision, err := c.planEvictionLocked(ns, need, "quota_on_publish", exclude, now)
	if err != nil {
		return nil, err
	}
	// 即使没有任何候选也提交一次：决策进入 committed 终态（空操作），
	// 审计与决策查询都能看到"因配额不足尝试淘汰但无未固定候选"这一事实。
	if err := c.commitEvictionLocked(decision, now); err != nil {
		return nil, err
	}
	if decision.FreedBytes < need {
		return nil, c.quotaErrorLocked(ns, used, need, decision.FreedBytes)
	}
	return decision, nil
}

func (c *Cache) quotaErrorLocked(ns Namespace, used, need, freed int64) error {
	return &QuotaExceededError{
		Namespace: ns.Name, MaxBytes: ns.MaxBytes,
		UsedBytes: used, NeedBytes: need, FreedBytes: freed,
	}
}

func (c *Cache) namespaceUsedLocked(namespace string) (int64, error) {
	entries, err := c.store.ListEntries()
	if err != nil {
		return 0, err
	}
	var used int64
	for _, e := range entries {
		if e.Namespace == namespace {
			used += e.TotalSize
		}
	}
	return used, nil
}

// Read 按命名空间与缓存键读取已发布条目，并刷新该条目的最近访问时间。
// 未完成/未发布的内容对读者永远不可见。
func (c *Cache) Read(namespace, key string) (*EntryReader, error) {
	namespace = normNamespace(namespace)
	c.mu.Lock()
	entry, err := c.store.GetEntry(namespace, key)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	now := c.clock.Now()
	// 访问会前移 LRU 时间戳：持有锁原子更新，使任何进行中的淘汰决定随后失效。
	if entry.LastAccessedAt.Before(now) {
		entry.LastAccessedAt = now
		if err := c.store.PutEntry(entry, int64(entry.Version)); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	c.mu.Unlock()
	return newEntryReader(c.store, entry), nil
}

// Cancel 取消会话。已取消/重复取消按幂等处理；已完成或已过期的会话不能取消。
func (c *Cache) Cancel(sessionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sess, err := c.store.GetSession(sessionID)
	if err != nil {
		return err
	}
	switch {
	case sess.Status == SessionCompleted:
		return fmt.Errorf("%w: session %s already completed", ErrSessionNotActive, sessionID)
	case sess.Status == SessionCanceled:
		return nil
	case c.expired(sess, c.clock.Now()):
		// 过期会话已"死亡"：取消既不能让它复活，也不应假装成功。
		return fmt.Errorf("%w: session %s already expired", ErrLeaseExpired, sessionID)
	}
	sess.Status = SessionCanceled
	if err := c.store.SaveSession(sess); err != nil {
		return err
	}
	if sess.IdempotencyKey != "" {
		delete(c.idem, idemComposite(sess.Namespace, sess.IdempotencyKey))
	}
	return nil
}

// SweepExpired 清理所有过期（open）与已取消的会话元数据。
// 只删会话记录，不直接删块——块的生死由 CollectGarbage 统一按引用判定。
func (c *Cache) SweepExpired() ([]SweptSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	sessions, err := c.store.ListSessions()
	if err != nil {
		return nil, err
	}
	var swept []SweptSession
	for i := range sessions {
		s := sessions[i]
		switch {
		case s.Status == SessionCanceled:
			if err := c.sweepSession(&s, "canceled"); err != nil {
				return nil, err
			}
			swept = append(swept, SweptSession{ID: s.ID, Key: s.Key, Status: s.Status, Reason: "canceled"})
		case s.Status == SessionOpen && c.expired(s, now):
			if err := c.sweepSession(&s, "lease_expired"); err != nil {
				return nil, err
			}
			swept = append(swept, SweptSession{ID: s.ID, Key: s.Key, Status: s.Status, Reason: "lease_expired"})
		}
	}
	return swept, nil
}

// CollectGarbage 执行一次标记-清除垃圾回收，并顺带把到期固定租约扫描失效。
//
// 标记与删除在同一临界区内完成（与发布、固定互斥），且删除每个块之前都会基于
// 最新状态再次复核引用（已发布条目 / 活跃上传 / 有效固定三类），因此不存在 mark 与
// delete 之间被并发发布或固定"抢先引用"而误删的窗口。每个决策都写入审计日志。
func (c *Cache) CollectGarbage() (*GCReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := c.clock.Now()
	c.gcGen++
	gen := c.gcGen
	report := &GCReport{Generation: gen, StartedAt: start}

	if err := c.store.AppendAudit(GCRecord{
		Time: start, Generation: gen, Action: GCStart,
		Detail: "garbage collection started",
	}); err != nil {
		return nil, err
	}

	// 先清理失效会话（它们不再是"活跃会话"，不得再保护任何块）。
	sessions, err := c.store.ListSessions()
	if err != nil {
		return nil, err
	}
	for i := range sessions {
		s := sessions[i]
		switch {
		case s.Status == SessionCanceled:
			if err := c.sweepSessionLocked(&s, "canceled", gen); err != nil {
				return nil, err
			}
			report.SweptSessions = append(report.SweptSessions,
				SweptSession{ID: s.ID, Key: s.Key, Status: s.Status, Reason: "canceled"})
		case s.Status == SessionOpen && c.expired(s, start):
			if err := c.sweepSessionLocked(&s, "lease_expired", gen); err != nil {
				return nil, err
			}
			report.SweptSessions = append(report.SweptSessions,
				SweptSession{ID: s.ID, Key: s.Key, Status: s.Status, Reason: "lease_expired"})
		}
	}

	// 到期固定租约扫描失效（状态翻转为 expired 并 +1 版本；旧版本解除无法复活它，
	// 但块的生死只看"有效固定"，翻转后其快照不再保护块）。
	sweptPins, err := c.sweepExpiredPinsLocked(start, gen)
	if err != nil {
		return nil, err
	}
	report.SweptPins = sweptPins

	// ---- Mark：快照全部存活引用 ----
	live, err := c.buildLiveSet(start)
	if err != nil {
		return nil, err
	}
	report.LiveBlobCount = len(live)

	// ---- Sweep：逐块在删除前基于最新状态二次复核 ----
	blobs, err := c.store.ListBlobs()
	if err != nil {
		return nil, err
	}
	for _, b := range blobs {
		if _, liveNow := live[b.Digest.String()]; liveNow {
			continue
		}
		// 二次复核：临界区内没有任何发布/固定能在标记之后建立引用，
		// 但仍以"删除瞬间"的引用集合为准做最后确认并记录原因。
		reason := c.deathReason(b.Digest, start)
		if reason == "" {
			// 标记后新出现了引用（防御性分支）：保留并审计。
			if err := c.store.AppendAudit(GCRecord{
				Time: c.clock.Now(), Generation: gen, Action: GCSkipInUse,
				Blob: b.Digest, Detail: "reference appeared between mark and delete",
			}); err != nil {
				return nil, err
			}
			continue
		}
		if err := c.store.DeleteBlob(b.Digest); err != nil {
			return nil, err
		}
		report.DeletedBlobs = append(report.DeletedBlobs,
			DeletedBlob{Digest: b.Digest, Size: b.Size, Reason: reason})
		if err := c.store.AppendAudit(GCRecord{
			Time: c.clock.Now(), Generation: gen, Action: GCDelete,
			Blob: b.Digest, Detail: reason,
		}); err != nil {
			return nil, err
		}
	}

	finish := c.clock.Now()
	report.FinishedAt = finish
	if err := c.store.AppendAudit(GCRecord{
		Time: finish, Generation: gen, Action: GCFinish,
		Detail: fmt.Sprintf("live=%d deleted=%d swept_sessions=%d swept_pins=%d",
			report.LiveBlobCount, len(report.DeletedBlobs),
			len(report.SweptSessions), len(report.SweptPins)),
	}); err != nil {
		return nil, err
	}
	return report, nil
}

// AuditLog 返回全部审计记录（含历史 GC 代次）。
func (c *Cache) AuditLog() ([]GCRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListAudit()
}

// ---- 固定租约 ----

// PinOptions 是固定请求参数。
type PinOptions struct {
	Namespace string
	Key       string
	// ExpectedDigest 非空时，仅当条目摘要与之相等才允许固定（摘要不匹配拒绝）。
	ExpectedDigest Digest
	// Deadline 绝对截止时间；零值时使用 TTL。与 TTL 二选一，Deadline 优先。
	Deadline time.Time
	// TTL 相对时长；<=0 且 Deadline 为零值时使用 DefaultPinTTL。
	TTL time.Duration
	// ExpectedVersion 可选的租约版本条件（重固定场景）。
	ExpectedVersion *uint64
	// RequestID 请求号：相同请求号 + 相同参数返回原结果，不同参数冲突。
	RequestID string
}

func pinDeadline(opts PinOptions, now time.Time) (time.Time, error) {
	var d time.Time
	if !opts.Deadline.IsZero() {
		d = opts.Deadline
	} else {
		ttl := opts.TTL
		if ttl <= 0 {
			ttl = DefaultPinTTL
		}
		d = now.Add(ttl)
	}
	if !d.After(now) {
		return time.Time{}, &PinConflictError{
			Namespace: normNamespace(opts.Namespace), Key: opts.Key,
			Reason: PinReasonDeadlineInPast, Detail: d.Format(time.RFC3339Nano),
		}
	}
	return d, nil
}

// Pin 把指定键的已发布条目固定到某个截止时间。
//
// 固定不得作用于不存在的条目；若给出 ExpectedDigest 则还必须与当前摘要匹配。
// 每次成功固定都在该键上产生严格递增的新版本租约（无论此前是否有租约、租约是否
// 已终态），并快照当前条目引用用于块保护。
func (c *Cache) Pin(opts PinOptions) (*Pin, error) {
	opts.Namespace = normNamespace(opts.Namespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()

	deadline, err := pinDeadline(opts, now)
	if err != nil {
		return nil, err
	}
	// 指纹基于原始请求参数（绝对 deadline 或相对 TTL 原值），
	// 因此相同 TTL 的请求号重放不会因时钟推进而被误判为冲突。
	fp := pinFingerprint(PinActionPin, opts.Namespace, opts.Key,
		"deadline="+optDeadlineStr(opts.Deadline)+"|ttl="+itoa(int64(opts.TTL))+
			"|digest="+opts.ExpectedDigest.String()+"|ver="+optVerStr(opts.ExpectedVersion))
	if p, conflict, err := c.lookupPinRequest(opts.RequestID, PinActionPin, opts.Namespace, opts.Key, fp); err != nil {
		return nil, err
	} else if conflict != nil {
		return nil, conflict
	} else if p != nil {
		return p, nil
	}

	entry, err := c.store.GetEntry(opts.Namespace, opts.Key)
	if errors.Is(err, ErrEntryNotFound) {
		return nil, &PinConflictError{
			Namespace: opts.Namespace, Key: opts.Key, Reason: PinReasonNoEntry,
		}
	}
	if err != nil {
		return nil, err
	}
	if opts.ExpectedDigest.Valid() && opts.ExpectedDigest != entry.Digest {
		return nil, &PinDigestConflictError{
			Namespace: opts.Namespace, Key: opts.Key,
			ExpectedDigest: opts.ExpectedDigest, CurrentDigest: entry.Digest,
		}
	}
	if opts.ExpectedVersion != nil {
		if cur, err := c.store.GetPin(opts.Namespace, opts.Key); err == nil {
			if *opts.ExpectedVersion != cur.Version {
				return nil, &PinVersionConflictError{
					Namespace: opts.Namespace, Key: opts.Key,
					ExpectedVersion: *opts.ExpectedVersion,
					CurrentVersion:  cur.Version, CurrentStatus: cur.Status,
				}
			}
		} else if *opts.ExpectedVersion != 0 {
			return nil, &PinVersionConflictError{
				Namespace: opts.Namespace, Key: opts.Key,
				ExpectedVersion: *opts.ExpectedVersion, CurrentVersion: 0,
			}
		}
	}

	var version uint64 = 1
	if prev, err := c.store.GetPin(opts.Namespace, opts.Key); err == nil {
		version = prev.Version + 1 // 严格递增：旧租约（含终态）之上建立新一代
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	pin := Pin{
		Namespace: opts.Namespace, Key: opts.Key, Version: version,
		EntryVersion: entry.Version, Digest: entry.Digest,
		Chunks:   append([]ChunkRef(nil), entry.Chunks...),
		Deadline: deadline, Status: PinActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := c.store.SavePin(pin); err != nil {
		return nil, err
	}
	if err := c.recordPinRequest(opts.RequestID, PinActionPin, opts.Namespace, opts.Key, fp, pin, now); err != nil {
		return nil, err
	}
	return &pin, nil
}

// RenewPinOptions 是续租参数。
type RenewPinOptions struct {
	Namespace       string
	Key             string
	Extend          time.Duration // 从当前时刻延长多久；<=0 使用 DefaultPinTTL
	ExpectedVersion *uint64       // 可选：必须与当前租约版本一致，否则拒绝旧操作
	RequestID       string
}

// RenewPin 续租一个活跃固定。新截止时间取 max(现有截止时间, now+Extend)，
// 因此即使锁外发生交错，续租也永远不会缩短更新版本的租约；
// 携带过期版本号的续租直接返回 PinVersionConflictError。
func (c *Cache) RenewPin(opts RenewPinOptions) (*Pin, error) {
	opts.Namespace = normNamespace(opts.Namespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()

	ext := opts.Extend
	if ext <= 0 {
		ext = DefaultPinTTL
	}
	fp := pinFingerprint(PinActionRenew, opts.Namespace, opts.Key,
		"extend="+itoa(int64(ext))+"|ver="+optVerStr(opts.ExpectedVersion))
	if p, conflict, err := c.lookupPinRequest(opts.RequestID, PinActionRenew, opts.Namespace, opts.Key, fp); err != nil {
		return nil, err
	} else if conflict != nil {
		return nil, conflict
	} else if p != nil {
		return p, nil
	}

	pin, err := c.store.GetPin(opts.Namespace, opts.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: %s/%s", ErrPinNotFound, opts.Namespace, opts.Key)
	}
	if err != nil {
		return nil, err
	}
	if !pin.Active(now) {
		return nil, fmt.Errorf("%w: %s/%s is %s", ErrPinNotActive, opts.Namespace, opts.Key, pin.Status)
	}
	if opts.ExpectedVersion != nil && *opts.ExpectedVersion != pin.Version {
		return nil, &PinVersionConflictError{
			Namespace: opts.Namespace, Key: opts.Key,
			ExpectedVersion: *opts.ExpectedVersion,
			CurrentVersion:  pin.Version, CurrentStatus: pin.Status,
		}
	}
	newDeadline := now.Add(ext)
	if pin.Deadline.After(newDeadline) {
		newDeadline = pin.Deadline // 单调延长：不得缩短
	}
	pin.Version++
	pin.Deadline = newDeadline
	pin.Status = PinActive
	pin.UpdatedAt = now
	if err := c.store.SavePin(pin); err != nil {
		return nil, err
	}
	if err := c.recordPinRequest(opts.RequestID, PinActionRenew, opts.Namespace, opts.Key, fp, pin, now); err != nil {
		return nil, err
	}
	return &pin, nil
}

// UnpinOptions 是解除固定参数。
type UnpinOptions struct {
	Namespace       string
	Key             string
	ExpectedVersion *uint64 // 可选：版本不匹配时拒绝（旧版本解除不得触碰新租约）
	RequestID       string
}

// Unpin 显式解除一个活跃固定：版本 +1 并进入 released 终态。
// 已解除/已过期的租约不能被复活或重复操作（ErrPinNotActive）。
func (c *Cache) Unpin(opts UnpinOptions) (*Pin, error) {
	opts.Namespace = normNamespace(opts.Namespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()

	fp := pinFingerprint(PinActionUnpin, opts.Namespace, opts.Key,
		"ver="+optVerStr(opts.ExpectedVersion))
	if p, conflict, err := c.lookupPinRequest(opts.RequestID, PinActionUnpin, opts.Namespace, opts.Key, fp); err != nil {
		return nil, err
	} else if conflict != nil {
		return nil, conflict
	} else if p != nil {
		return p, nil
	}

	pin, err := c.store.GetPin(opts.Namespace, opts.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: %s/%s", ErrPinNotFound, opts.Namespace, opts.Key)
	}
	if err != nil {
		return nil, err
	}
	if !pin.Active(now) {
		return nil, fmt.Errorf("%w: %s/%s is %s", ErrPinNotActive, opts.Namespace, opts.Key, pin.Status)
	}
	if opts.ExpectedVersion != nil && *opts.ExpectedVersion != pin.Version {
		return nil, &PinVersionConflictError{
			Namespace: opts.Namespace, Key: opts.Key,
			ExpectedVersion: *opts.ExpectedVersion,
			CurrentVersion:  pin.Version, CurrentStatus: pin.Status,
		}
	}
	pin.Version++
	pin.Status = PinReleased
	pin.UpdatedAt = now
	if err := c.store.SavePin(pin); err != nil {
		return nil, err
	}
	if err := c.recordPinRequest(opts.RequestID, PinActionUnpin, opts.Namespace, opts.Key, fp, pin, now); err != nil {
		return nil, err
	}
	return &pin, nil
}

// SweepExpiredPins 扫描并失效所有已到期但仍标记 active 的固定租约。
func (c *Cache) SweepExpiredPins() ([]Pin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepExpiredPinsLocked(c.clock.Now(), 0)
}

func (c *Cache) sweepExpiredPinsLocked(now time.Time, gen uint64) ([]Pin, error) {
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	var swept []Pin
	for i := range pins {
		p := pins[i]
		if p.Status != PinActive || p.Deadline.After(now) {
			continue
		}
		p.Version++
		p.Status = PinExpired
		p.UpdatedAt = now
		if err := c.store.SavePin(p); err != nil {
			return nil, err
		}
		swept = append(swept, p)
		if err := c.store.AppendAudit(GCRecord{
			Time: now, Generation: gen, Action: GCPinSwept,
			Key: p.Key, Detail: fmt.Sprintf("namespace=%s version=%d", p.Namespace, p.Version),
		}); err != nil {
			return nil, err
		}
	}
	return swept, nil
}

// GetPin 查询指定键的固定租约（含已解除/已过期的终态记录）。
func (c *Cache) GetPin(namespace, key string) (Pin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	namespace = normNamespace(namespace)
	p, err := c.store.GetPin(namespace, key)
	if errors.Is(err, ErrNotFound) {
		return Pin{}, fmt.Errorf("%w: %s/%s", ErrPinNotFound, namespace, key)
	}
	return p, err
}

// ListPins 列出固定租约；namespace 为空表示全部命名空间，activeOnly 只返回有效租约。
func (c *Cache) ListPins(namespace string, activeOnly bool) ([]Pin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	out := make([]Pin, 0, len(pins))
	for _, p := range pins {
		if namespace != "" && p.Namespace != namespace {
			continue
		}
		if activeOnly && !p.Active(now) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// lookupPinRequest 处理固定类请求号：返回 (已有结果, 冲突错误, 无记录)。
// 调用方持有 c.mu。
func (c *Cache) lookupPinRequest(requestID, action, namespace, key, fingerprint string,
) (*Pin, error, error) {
	if requestID == "" {
		return nil, nil, nil
	}
	rec, ok := c.pinReqs[requestID]
	if !ok {
		return nil, nil, nil
	}
	if rec.Action == action && rec.Namespace == namespace &&
		rec.Key == key && rec.Fingerprint == fingerprint {
		p := rec.Result
		return &p, nil, nil
	}
	return nil, nil, &PinRequestConflictError{RequestID: requestID}
}

func (c *Cache) recordPinRequest(requestID, action, namespace, key, fingerprint string,
	result Pin, now time.Time) error {
	if requestID == "" {
		return nil
	}
	rec := PinRequest{
		RequestID: requestID, Namespace: namespace, Key: key, Action: action,
		Fingerprint: fingerprint, Result: result, CreatedAt: now,
	}
	if err := c.store.SavePinRequest(rec); err != nil {
		return err
	}
	c.pinReqs[requestID] = rec
	return nil
}

func pinFingerprint(action, namespace, key, params string) string {
	return action + "|" + namespace + "|" + key + "|" + params
}

func optDeadlineStr(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return itoa(t.UnixNano())
}
func optVerStr(v *uint64) string {
	if v == nil {
		return "-"
	}
	return itoa(int64(*v))
}

// ---- 配额淘汰（两阶段：plan → commit）----

// PlanEviction 在命名空间内建立一个"未固定 LRU"淘汰决策但不立即删除，
// 需要释放 needBytes 字节（候选体量按 LRU、键名升序累计到满足为止）。
// 发布/晋级内部流程通过持锁内核的 exclude 集合排除自身依赖的键。
func (c *Cache) PlanEviction(namespace string, needBytes int64) (*EvictionDecision, error) {
	namespace = normNamespace(namespace)
	if needBytes <= 0 {
		return nil, errors.New("buildcache: need bytes must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, err := c.requireNamespaceLocked(namespace)
	if err != nil {
		return nil, err
	}
	return c.planEvictionLocked(ns, needBytes, "manual", nil, c.clock.Now())
}

// planEvictionLocked 建立淘汰决策。exclude 中的键永不成为候选
// （发布时排除自身；晋级时排除目标键与同命名空间内的来源键——
// 它们是本次操作依赖的内容，不得淘汰）。调用方持有 c.mu。
func (c *Cache) planEvictionLocked(ns Namespace, needBytes int64, reason string,
	exclude map[string]bool, now time.Time) (*EvictionDecision, error) {
	entries, err := c.store.ListEntries()
	if err != nil {
		return nil, err
	}
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	pinned := make(map[string]Pin)
	for _, p := range pins {
		if p.Namespace == ns.Name && p.Active(now) {
			pinned[p.Key] = p
		}
	}
	var cands []Entry
	for _, e := range entries {
		if e.Namespace != ns.Name || exclude[e.Key] {
			continue
		}
		if _, isPinned := pinned[e.Key]; isPinned {
			continue // 固定条目永不参与配额淘汰
		}
		cands = append(cands, e)
	}
	// 未固定、最近最少使用（LastAccessedAt 升序）、键名稳定次序。
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].LastAccessedAt.Equal(cands[j].LastAccessedAt) {
			return cands[i].LastAccessedAt.Before(cands[j].LastAccessedAt)
		}
		return cands[i].Key < cands[j].Key
	})
	used, err := c.namespaceUsedLocked(ns.Name)
	if err != nil {
		return nil, err
	}
	decision := EvictionDecision{
		ID: c.newEvictionID(), Namespace: ns.Name, Reason: reason,
		MaxBytes: ns.MaxBytes, UsedBefore: used, NeededBytes: needBytes,
		Status: EvictionProposed, DecidedAt: now,
	}
	var accumulated int64
	for _, e := range cands {
		if accumulated >= needBytes {
			break
		}
		decision.Candidates = append(decision.Candidates, EvictionCandidate{
			Namespace: ns.Name, Key: e.Key, Digest: e.Digest,
			Version: e.Version, Size: e.TotalSize,
			LastAccessedAt: e.LastAccessedAt,
		})
		accumulated += e.TotalSize
	}
	if err := c.store.SaveEvictionDecision(decision); err != nil {
		return nil, err
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCEviction, Key: ns.Name,
		Detail: fmt.Sprintf("decision=%s phase=proposed reason=%s candidates=%d need=%d",
			decision.ID, reason, len(decision.Candidates), needBytes),
	}); err != nil {
		return nil, err
	}
	return &decision, nil
}

func (c *Cache) newEvictionID() string {
	c.evictSeq++
	return fmt.Sprintf("evict-%012d", c.evictSeq)
}

// CommitEviction 提交淘汰决策：逐个候选在删除前基于最新状态复核。
// 决策建立之后候选若被访问、固定或重新发布（版本/摘要变化、消失），
// 旧决定对该候选失效并记录跳过原因；其余候选被删除。
func (c *Cache) CommitEviction(decisionID string) (*EvictionDecision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	decision, err := c.store.GetEvictionDecision(decisionID)
	if err != nil {
		return nil, err
	}
	if decision.Status == EvictionCommitted {
		return &decision, nil // 提交幂等
	}
	now := c.clock.Now()
	if err := c.commitEvictionLocked(&decision, now); err != nil {
		return nil, err
	}
	return &decision, nil
}

func (c *Cache) commitEvictionLocked(decision *EvictionDecision, now time.Time) error {
	victims, err := c.evaluateEvictionLocked(decision, now)
	if err != nil {
		return err
	}
	for _, e := range victims {
		if err := c.store.DeleteEntry(decision.Namespace, e.Key); err != nil {
			return err
		}
	}
	decision.Status = EvictionCommitted
	decision.CommittedAt = now
	if err := c.store.SaveEvictionDecision(*decision); err != nil {
		return err
	}
	var deleted, skipped int
	for _, cand := range decision.Candidates {
		if cand.Outcome == EvictDeleted {
			deleted++
		} else {
			skipped++
		}
	}
	return c.store.AppendAudit(GCRecord{
		Time: now, Action: GCEviction, Key: decision.Namespace,
		Detail: fmt.Sprintf("decision=%s phase=committed deleted=%d skipped=%d freed=%d",
			decision.ID, deleted, skipped, decision.FreedBytes),
	})
}

// evaluateEvictionLocked 用最新状态逐个复核淘汰候选并填写 Outcome，
// 返回判定为"可删除"的候选的完整当前条目（调用方负责真正删除）。
// 不修改任何条目状态。调用方持有 c.mu。
func (c *Cache) evaluateEvictionLocked(decision *EvictionDecision, now time.Time) ([]Entry, error) {
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	activePin := make(map[string]Pin)
	for _, p := range pins {
		if p.Namespace == decision.Namespace && p.Active(now) {
			activePin[p.Key] = p
		}
	}
	decision.FreedBytes = 0
	var victims []Entry
	for i := range decision.Candidates {
		cand := &decision.Candidates[i]
		cand.Outcome = EvictPending
		entry, gerr := c.store.GetEntry(decision.Namespace, cand.Key)
		switch {
		case errors.Is(gerr, ErrEntryNotFound):
			cand.Outcome = EvictSkippedMissing
			continue
		case gerr != nil:
			return nil, gerr
		}
		// 重新发布：版本或摘要与决策快照不一致。
		if entry.Version != cand.Version || entry.Digest != cand.Digest {
			cand.Outcome = EvictSkippedRepublished
			continue
		}
		// 访问：最近访问时间相对决策快照前移。
		if entry.LastAccessedAt.After(cand.LastAccessedAt) {
			cand.Outcome = EvictSkippedAccessed
			continue
		}
		// 固定：决策之后获得了有效固定。
		if _, ok := activePin[cand.Key]; ok {
			cand.Outcome = EvictSkippedPinned
			continue
		}
		cand.Outcome = EvictDeleted
		decision.FreedBytes += cand.Size
		victims = append(victims, entry)
	}
	return victims, nil
}

// GetEvictionDecision 查询一次淘汰决策（含每个候选的提交结果）。
func (c *Cache) GetEvictionDecision(id string) (EvictionDecision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, err := c.store.GetEvictionDecision(id)
	if errors.Is(err, ErrNotFound) {
		return EvictionDecision{}, fmt.Errorf("%w: eviction %s", ErrNotFound, id)
	}
	return d, err
}

// ListEvictionDecisions 列出淘汰决策；namespace 为空表示全部。
func (c *Cache) ListEvictionDecisions(namespace string) ([]EvictionDecision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ds, err := c.store.ListEvictionDecisions()
	if err != nil {
		return nil, err
	}
	out := make([]EvictionDecision, 0, len(ds))
	for _, d := range ds {
		if namespace != "" && d.Namespace != namespace {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

// ---- 内容引用查询 ----

// BlobReferences 查询一个内容块当前被哪些已发布条目、活跃上传或有效固定引用。
// 会话类引用以 Active 标识该会话是否仍 open 且未过期；已发布条目引用恒为 active。
func (c *Cache) BlobReferences(digest Digest) ([]BlobReference, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()

	var refs []BlobReference
	entries, err := c.store.ListEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		for _, ref := range e.Chunks {
			if ref.Digest == digest {
				refs = append(refs, BlobReference{
					Kind: RefEntry, Namespace: e.Namespace, Key: e.Key,
					Version: e.Version, Active: true,
					Detail: "published entry",
				})
				break
			}
		}
	}
	sessions, err := c.store.ListSessions()
	if err != nil {
		return nil, err
	}
	for _, s := range sessions {
		found := false
		for _, rec := range s.Received {
			if rec.Digest == digest {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		active := s.Status == SessionOpen && !c.expired(s, now)
		refs = append(refs, BlobReference{
			Kind: RefSession, Namespace: normNamespace(s.Namespace), Key: s.Key,
			Active: active,
			Detail: fmt.Sprintf("session=%s status=%s", s.ID, s.Status),
		})
	}
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	for _, p := range pins {
		found := false
		for _, ref := range p.Chunks {
			if ref.Digest == digest {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		refs = append(refs, BlobReference{
			Kind: RefPin, Namespace: p.Namespace, Key: p.Key,
			Version: p.Version, Active: p.Active(now),
			Detail: fmt.Sprintf("pin v%d status=%s deadline=%s", p.Version, p.Status, p.Deadline.Format(time.RFC3339Nano)),
		})
	}
	return refs, nil
}

// ---- 内部辅助 ----

// ensureOpen 校验会话仍可写入。过期会话不会被复活，也不会在这里被删除——
// 记录的清理由 SweepExpired / GC 统一完成，保证"失效"与"清理"两个语义分离。
// 调用方必须持有 c.mu。
func (c *Cache) ensureOpen(sess *Session, now time.Time) error {
	switch sess.Status {
	case SessionCompleted, SessionCanceled:
		return fmt.Errorf("%w: session %s is %s", ErrSessionNotActive, sess.ID, sess.Status)
	}
	if c.expired(*sess, now) {
		return fmt.Errorf("%w: session %s expired at %s", ErrLeaseExpired, sess.ID, sess.ExpiresAt.Format(time.RFC3339Nano))
	}
	return nil
}

// sweepSession 删除会话元数据并写审计；调用方持有 c.mu（GC 代次为 0 表示非 GC 上下文）。
func (c *Cache) sweepSession(sess *Session, reason string) error {
	return c.sweepSessionLocked(sess, reason, 0)
}

func (c *Cache) sweepSessionLocked(sess *Session, reason string, gen uint64) error {
	if err := c.store.DeleteSession(sess.ID); err != nil {
		return err
	}
	if sess.IdempotencyKey != "" {
		comp := idemComposite(sess.Namespace, sess.IdempotencyKey)
		if c.idem[comp] == sess.ID {
			delete(c.idem, comp)
		}
	}
	return c.store.AppendAudit(GCRecord{
		Time: c.clock.Now(), Generation: gen, Action: GCSessionSwept,
		SessionID: sess.ID, Key: sess.Key, Reason: reason,
	})
}

func (c *Cache) expired(s Session, now time.Time) bool {
	return !s.ExpiresAt.After(now)
}

// buildLiveSet 汇总当前所有存活引用：
// 已发布键的全部块 + 未过且 open 的活跃会话已上传块 + 未到期 active 固定的快照块。
// 调用方持有 c.mu。
func (c *Cache) buildLiveSet(now time.Time) (map[string]struct{}, error) {
	live := make(map[string]struct{})
	entries, err := c.store.ListEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		for _, ref := range e.Chunks {
			live[ref.Digest.String()] = struct{}{}
		}
	}
	sessions, err := c.store.ListSessions()
	if err != nil {
		return nil, err
	}
	for _, s := range sessions {
		if s.Status != SessionOpen || c.expired(s, now) {
			continue
		}
		for _, rec := range s.Received {
			live[rec.Digest.String()] = struct{}{}
		}
	}
	pins, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	for _, p := range pins {
		if !p.Active(now) {
			continue
		}
		for _, ref := range p.Chunks {
			live[ref.Digest.String()] = struct{}{}
		}
	}
	return live, nil
}

// deathReason 返回块当前不被任何引用保护的人类可读原因；
// 若仍被引用则返回空串。调用方持有 c.mu。
func (c *Cache) deathReason(d Digest, now time.Time) string {
	entries, err := c.store.ListEntries()
	if err == nil {
		for _, e := range entries {
			for _, ref := range e.Chunks {
				if ref.Digest == d {
					return ""
				}
			}
		}
	}
	sessions, err := c.store.ListSessions()
	if err == nil {
		for _, s := range sessions {
			if s.Status != SessionOpen || c.expired(s, now) {
				continue
			}
			for _, rec := range s.Received {
				if rec.Digest == d {
					return ""
				}
			}
		}
	}
	pins, err := c.store.ListPins()
	if err == nil {
		for _, p := range pins {
			if !p.Active(now) {
				continue
			}
			for _, ref := range p.Chunks {
				if ref.Digest == d {
					return ""
				}
			}
		}
	}
	return "not referenced by any published key, active session, or active pin"
}

func (c *Cache) computeFinalDigest(sess Session) (Digest, error) {
	h, err := newHasher(sess.FinalDigest.Algo)
	if err != nil {
		return Digest{}, err
	}
	order := make([]ChunkSpec, len(sess.Chunks))
	copy(order, sess.Chunks)
	sort.Slice(order, func(i, j int) bool { return order[i].Index < order[j].Index })
	for _, spec := range order {
		rc, err := c.store.GetBlob(sess.Received[spec.Index].Digest)
		if err != nil {
			return Digest{}, err
		}
		_, err = io.Copy(h, rc)
		rc.Close()
		if err != nil {
			return Digest{}, err
		}
	}
	return Digest{Algo: sess.FinalDigest.Algo, Hex: hex.EncodeToString(h.Sum(nil))}, nil
}

func normalizeChunks(chunks []ChunkSpec, total int64, algo string) ([]ChunkSpec, error) {
	if len(chunks) == 0 {
		return nil, errors.New("buildcache: at least one chunk is required")
	}
	out := append([]ChunkSpec(nil), chunks...)
	seen := make(map[int]bool, len(out))
	for _, ch := range out {
		if ch.Index < 0 {
			return nil, fmt.Errorf("buildcache: chunk index must be non-negative, got %d", ch.Index)
		}
		if seen[ch.Index] {
			return nil, fmt.Errorf("buildcache: duplicate chunk index %d", ch.Index)
		}
		seen[ch.Index] = true
		if ch.Size <= 0 {
			return nil, fmt.Errorf("buildcache: chunk %d size must be positive", ch.Index)
		}
		if ch.Offset < 0 {
			return nil, fmt.Errorf("buildcache: chunk %d offset must be non-negative", ch.Index)
		}
		if !ch.Digest.Valid() {
			return nil, fmt.Errorf("buildcache: chunk %d has invalid digest", ch.Index)
		}
		if ch.Digest.Algo != algo {
			return nil, fmt.Errorf("buildcache: chunk %d digest algo %q differs from final %q",
				ch.Index, ch.Digest.Algo, algo)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	// 若分片号从 0 连续，则同时校验偏移连续且恰好覆盖 TotalSize。
	if out[0].Index == 0 {
		var expect int64
		for _, ch := range out {
			if ch.Offset != expect {
				return nil, fmt.Errorf("buildcache: chunks are not contiguous: index %d offset %d, expected %d",
					ch.Index, ch.Offset, expect)
			}
			expect += ch.Size
		}
		if expect != total {
			return nil, fmt.Errorf("buildcache: chunk sizes sum to %d but total size declared as %d",
				expect, total)
		}
	}
	return out, nil
}

func chunkSpecByIndex(chunks []ChunkSpec, index int) (ChunkSpec, bool) {
	// chunks 已按 index 排序，直接线性即可（分片数通常不多）。
	for _, ch := range chunks {
		if ch.Index == index {
			return ch, true
		}
	}
	return ChunkSpec{}, false
}

func sameParams(s Session, opts CreateSessionOptions) bool {
	if normNamespace(s.Namespace) != normNamespace(opts.Namespace) ||
		s.Key != opts.Key || s.TotalSize != opts.TotalSize || s.FinalDigest != opts.FinalDigest {
		return false
	}
	if len(s.Chunks) != len(opts.Chunks) {
		return false
	}
	// s.Chunks 已排序；opts.Chunks 排序后逐项比较。
	want := append([]ChunkSpec(nil), opts.Chunks...)
	sort.Slice(want, func(i, j int) bool { return want[i].Index < want[j].Index })
	for i := range want {
		if want[i] != s.Chunks[i] {
			return false
		}
	}
	return true
}

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "sess_" + hex.EncodeToString(b[:]), nil
}

// ErrChunkNotDeclared 表示上传的分片号不在会话声明的分片集合内。
var ErrChunkNotDeclared = errors.New("buildcache: chunk not declared in session")
