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

// CreateSessionOptions 是创建上传会话的参数。
type CreateSessionOptions struct {
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
	Entry  Entry
	Reused bool // true 表示已有同摘要条目，本次直接复用，没有新建版本
}

// GCReport 汇总一次垃圾回收的决策，与审计日志内容对应。
type GCReport struct {
	Generation    uint64
	StartedAt     time.Time
	FinishedAt    time.Time
	LiveBlobCount int
	DeletedBlobs  []DeletedBlob
	SweptSessions []SweptSession
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
// 所有改变状态的操作（上传、完成、取消、清理、GC）都在同一把互斥锁下串行，
// 因此"建立引用"（complete 发布条目）与"判定/删除块"（GC mark→sweep）
// 不可能交错：一个发布要么完整地发生在 GC 快照之前（块被保留），
// 要么只能发生在 GC 结束之后（届时该会话在统一时钟下必然已过期，发布会被拒绝）。
type Cache struct {
	store Store
	clock Clock
	ttl   time.Duration

	mu    sync.Mutex
	idem  map[string]string // 幂等键 -> 会话 ID
	gcGen uint64            // GC 代次，重启后由审计日志恢复
}

// New 创建缓存服务。clock 为 nil 时使用 SystemClock；store 由调用方提供。
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
	c := &Cache{store: store, clock: clock, ttl: ttl, idem: make(map[string]string)}
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
			c.idem[s.IdempotencyKey] = s.ID
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
	return nil
}

// Clock 暴露统一时间来源（测试/租约判定共用）。
func (c *Cache) Clock() Clock { return c.clock }

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

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.clock.Now()

	if opts.IdempotencyKey != "" {
		if existingID, ok := c.idem[opts.IdempotencyKey]; ok {
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
			delete(c.idem, opts.IdempotencyKey)
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
		c.idem[opts.IdempotencyKey] = id
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

	// 4) 带版本条件的原子发布。
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
		delete(c.idem, sess.IdempotencyKey)
	}
	return result, nil
}

func (c *Cache) publish(sess *Session, refs []ChunkRef, now time.Time, opts CompleteOptions) (*PublishResult, error) {
	existing, err := c.store.GetEntry(sess.Key)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		if opts.ExpectedVersion != nil && *opts.ExpectedVersion != 0 {
			return nil, &VersionConflictError{
				Key: sess.Key, CurrentVersion: 0,
				ExpectedVersion: opts.ExpectedVersion, AttemptDigest: sess.FinalDigest,
			}
		}
		entry := Entry{
			Key: sess.Key, Version: 1, Digest: sess.FinalDigest,
			TotalSize: sess.TotalSize, Chunks: refs, PublishedAt: now,
		}
		if err := c.store.PutEntry(entry, -1); err != nil {
			return nil, err
		}
		return &PublishResult{Entry: entry, Reused: false}, nil

	case err != nil:
		return nil, err

	case existing.Digest == sess.FinalDigest:
		// 摘要相同：直接复用现有结果，版本条件不再适用。
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
		entry := Entry{
			Key: sess.Key, Version: existing.Version + 1, Digest: sess.FinalDigest,
			TotalSize: sess.TotalSize, Chunks: refs, PublishedAt: now,
		}
		// CAS：即使绕过本进程锁（例如共享存储），旧版本也无法覆盖新版本。
		if err := c.store.PutEntry(entry, int64(existing.Version)); err != nil {
			if errors.Is(err, ErrCASFailed) {
				fresh, gerr := c.store.GetEntry(sess.Key)
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
		return &PublishResult{Entry: entry, Reused: false}, nil
	}
}

// Read 按缓存键读取已发布条目。未完成/未发布的内容对读者永远不可见。
func (c *Cache) Read(key string) (*EntryReader, error) {
	c.mu.Lock()
	entry, err := c.store.GetEntry(key)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
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
		delete(c.idem, sess.IdempotencyKey)
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

// CollectGarbage 执行一次标记-清除垃圾回收。
//
// 标记与删除在同一临界区内完成（与发布互斥），且删除每个块之前都会基于
// 最新状态再次复核引用，因此不存在 mark 与 delete 之间被并发发布"抢先引用"
// 而误删的窗口。每个决策都写入审计日志。
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
		// 二次复核：临界区内没有任何发布能在标记之后建立引用，
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
		Detail: fmt.Sprintf("live=%d deleted=%d swept_sessions=%d",
			report.LiveBlobCount, len(report.DeletedBlobs), len(report.SweptSessions)),
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
	if sess.IdempotencyKey != "" && c.idem[sess.IdempotencyKey] == sess.ID {
		delete(c.idem, sess.IdempotencyKey)
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
// 已发布键的全部块 + 未过且 open 的活跃会话已上传块。
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
	return "not referenced by any published key or active session"
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
	if s.Key != opts.Key || s.TotalSize != opts.TotalSize || s.FinalDigest != opts.FinalDigest {
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
