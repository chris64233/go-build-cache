package buildcache

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options 控制 Store 的行为。零值即可用于生产（使用 RealClock 与默认租约时长）。
type Options struct {
	// Clock 是统一的当前时间来源。nil 时使用 RealClock。
	Clock Clock
	// LeaseTTL 是会话租约时长：创建/续期后 ExpiresAt = now + LeaseTTL。
	// <=0 时使用默认值（15 分钟）。
	LeaseTTL time.Duration
}

// Store 是内容寻址构建缓存的核心，提供会话创建、分片上传、完成（原子发布）、
// 读取、取消、租约续期、过期清理与垃圾回收。
//
// 所有元数据变更都通过同一把互斥锁串行化并原子落盘；内容块存放在 CAS 中，
// 不可变且可被多个会话/条目共享。
type Store struct {
	root     string
	blobs    *blobStore
	clock    Clock
	leaseTTL time.Duration

	mu sync.Mutex // 保护以下全部字段以及元数据文件的写入
	// sessions 同时包含活跃会话和已终结（完成/取消/过期）会话。
	sessions map[string]*Session
	entries  map[string]*Entry

	// publishWait 在在途发布期间非空：GC 屏障会在其 WaitGroup 归零后才进入清扫。
	inflight  sync.WaitGroup
	gcRunning bool
	closed    bool

	// testHooks 仅用于测试在 GC 标记与屏障之间插入并发动作。
	testHooks struct {
		afterFirstMark func()
	}

	auditMu sync.Mutex // 仅保护审计日志的写入
}

const (
	defaultLeaseTTL = 15 * time.Minute
	sessionsDir     = "sessions"
	entriesDir      = "entries"
	auditFile       = "gc-audit.log"
)

// Open 打开（必要时初始化）root 下的缓存存储，并恢复磁盘上的元数据。
func Open(root string, opts Options) (*Store, error) {
	clock := opts.Clock
	if clock == nil {
		clock = RealClock{}
	}
	ttl := opts.LeaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	for _, d := range []string{root, filepath.Join(root, sessionsDir), filepath.Join(root, entriesDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	s := &Store{
		root:     root,
		blobs:    newBlobStore(root),
		clock:    clock,
		leaseTTL: ttl,
		sessions: make(map[string]*Session),
		entries:  make(map[string]*Entry),
	}
	if err := s.blobs.ensure(); err != nil {
		return nil, err
	}
	if err := s.blobs.cleanTemp(); err != nil {
		return nil, err
	}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close 关闭存储。当前实现下元数据均即时落盘，Close 仅标记状态。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// ---------------- 会话创建 ----------------

// CreateSessionInput 是创建上传会话的参数。
type CreateSessionInput struct {
	// Key 是缓存键。
	Key string
	// Digest 是拼接所有分片后内容的预期 SHA-256 摘要。
	Digest Digest
	// TotalSize 是拼接后的预期总字节数。
	TotalSize int64
	// Chunks 按上传序号（从 0 起）声明每个分片的大小与摘要；集合必须连续无空洞。
	Chunks []ChunkSpec
}

// CreateSession 创建一个上传会话：声明缓存键、预期摘要、总大小以及分片集合。
// 会话初始不拥有任何分片；在 Complete 成功发布前，读者通过 Key 看不到任何相关数据。
func (s *Store) CreateSession(in CreateSessionInput) (*Session, error) {
	if err := validateCreateInput(in); err != nil {
		return nil, err
	}
	chunks := make([]ChunkSpec, len(in.Chunks))
	copy(chunks, in.Chunks)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Index < chunks[j].Index })
	// validateCreateInput 已按调用方给出的顺序校验过空洞，排序后再确认一次连续性。
	for i := range chunks {
		if chunks[i].Index != i {
			return nil, fmt.Errorf("%w: chunk indices must be contiguous starting at 0", ErrInvalidArgument)
		}
	}

	id := newSessionID()
	now := s.clock.Now()
	sess := &Session{
		ID:        id,
		Key:       in.Key,
		Digest:    in.Digest,
		TotalSize: in.TotalSize,
		Chunks:    chunks,
		CreatedAt: now,
		ExpiresAt: now.Add(s.leaseTTL),
		State:     SessionActive,
		Uploaded:  make([]Digest, len(chunks)),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkClosedLocked(); err != nil {
		return nil, err
	}
	s.sessions[id] = sess
	if err := s.persistSessionLocked(sess); err != nil {
		delete(s.sessions, id)
		return nil, err
	}
	return cloneSession(sess), nil
}

func validateCreateInput(in CreateSessionInput) error {
	if in.Key == "" {
		return fmt.Errorf("%w: empty cache key", ErrInvalidArgument)
	}
	if !in.Digest.Valid() {
		return fmt.Errorf("%w: invalid expected digest", ErrInvalidArgument)
	}
	if in.TotalSize < 0 {
		return fmt.Errorf("%w: negative total size", ErrInvalidArgument)
	}
	if len(in.Chunks) == 0 {
		return fmt.Errorf("%w: at least one chunk is required", ErrInvalidArgument)
	}
	seen := make(map[int]ChunkSpec, len(in.Chunks))
	var sum int64
	for _, c := range in.Chunks {
		if c.Index < 0 {
			return fmt.Errorf("%w: negative chunk index %d", ErrInvalidArgument, c.Index)
		}
		if _, dup := seen[c.Index]; dup {
			return fmt.Errorf("%w: duplicate chunk index %d", ErrInvalidArgument, c.Index)
		}
		if !c.Hash.Valid() {
			return fmt.Errorf("%w: invalid digest on chunk %d", ErrInvalidArgument, c.Index)
		}
		if c.Size < 0 {
			return fmt.Errorf("%w: negative size on chunk %d", ErrInvalidArgument, c.Index)
		}
		seen[c.Index] = c
		sum += c.Size
	}
	for i := 0; i < len(in.Chunks); i++ {
		if _, ok := seen[i]; !ok {
			return fmt.Errorf("%w: chunk set has a gap at index %d", ErrInvalidArgument, i)
		}
	}
	if sum != in.TotalSize {
		return fmt.Errorf("%w: sum of chunk sizes (%d) != total size (%d)", ErrInvalidArgument, sum, in.TotalSize)
	}
	return nil
}

// ---------------- 分片上传 ----------------

// PutChunk 上传（或重传）会话的一个分片。
//
// 分片可以乱序到达；相同内容的重传是幂等的。错误分类：
//   - 该片号此前已确认过一份内容，而本次字节流与之不同：*ChunkConflictError（幂等冲突）；
//   - 该片号首次上传，但字节流的 SHA-256/大小与创建会话时的声明不符：*DigestMismatchError；
//   - 会话已过期/取消/完成：ErrLeaseExpired / ErrSessionCancelled / ErrSessionCompleted。
//
// 任何未被接受的内容都不会进入 CAS、也不会记录到会话中；会话在上传期间过期的，
// 提交元数据时会重新确认活跃状态，失效会话不会复活。
func (s *Store) PutChunk(sessionID string, index int, r io.Reader) error {
	s.mu.Lock()
	sess, err := s.liveSessionLocked(sessionID)
	if err == nil {
		if err = s.checkClosedLocked(); err == nil && (index < 0 || index >= len(sess.Chunks)) {
			err = fmt.Errorf("%w: chunk index %d not in declared set [0,%d)", ErrChunkIndex, index, len(sess.Chunks))
		}
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	spec := sess.Chunks[index]

	// 内容先写入暂存文件并算出实际摘要；被接受前它在 CAS 中不可见。
	got, n, accepted, err := s.blobs.PutChecked(r, spec.Hash, spec.Size)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkClosedLocked(); err != nil {
		return err
	}
	// 重新取会话：上传期间它可能已被取消，或恰好被过期清理终结——
	// 过期的会话不允许“复活”。
	sess, err = s.liveSessionLocked(sessionID)
	if err != nil {
		return err
	}

	if !accepted {
		existing := sess.Uploaded[index]
		if existing != "" && existing != got {
			// 同片号、不同内容：幂等冲突优先于摘要不符上报。
			return &ChunkConflictError{SessionID: sessionID, Index: index, ExistingDigest: existing, IncomingDigest: got}
		}
		return &DigestMismatchError{
			Scope:        "chunk",
			Index:        index,
			Expected:     spec.Hash,
			Got:          got,
			ExpectedSize: spec.Size,
			GotSize:      n,
		}
	}

	if existing := sess.Uploaded[index]; existing != "" {
		// accepted 时 got == spec.Hash；已记录的内容必然也是 spec.Hash，故此处恒为幂等重传。
		if existing != got {
			return &ChunkConflictError{SessionID: sessionID, Index: index, ExistingDigest: existing, IncomingDigest: got}
		}
		return nil
	}
	sess.Uploaded[index] = got
	if err := s.persistSessionLocked(sess); err != nil {
		sess.Uploaded[index] = ""
		return err
	}
	return nil
}

// PutChunkBytes 是 PutChunk 的字节数组便捷形式。
func (s *Store) PutChunkBytes(sessionID string, index int, data []byte) error {
	return s.PutChunk(sessionID, index, bytes.NewReader(data))
}

// ---------------- 完成与原子发布 ----------------

// Complete 校验会话并原子发布缓存条目。
//
// 校验内容：所有分片均已上传、拼接总大小等于声明值、拼接内容的 SHA-256 等于
// 声明的最终摘要。全部通过才发布；任一项失败都不会产生读者可见的条目。
//
// 同键并发发布时：
//   - 已发布条目的摘要与本会话相同：复用现有条目（幂等成功），会话标记为完成；
//   - 摘要不同：返回 *VersionConflictError，借助条目的版本条件拒绝覆盖，会话不发布。
func (s *Store) Complete(sessionID string) (*Entry, error) {
	// 在途发布计数必须在获取锁之前登记：GC 屏障据此保证“标记到删除之间”
	// 即将建立的引用不会被误删。
	s.inflight.Add(1)
	defer s.inflight.Done()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkClosedLocked(); err != nil {
		return nil, err
	}
	sess, err := s.liveSessionLocked(sessionID)
	if err != nil {
		return nil, err
	}

	// 1) 分片齐全性。
	var missing []int
	for i, d := range sess.Uploaded {
		if d == "" {
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 {
		return nil, &ChunkMissingError{SessionID: sessionID, Missing: missing}
	}

	// 2) 拼接并校验总大小与最终摘要。直接从 CAS 顺序读取各分片，
	//    避免在内存中持有完整制品。
	h := sha256.New()
	var total int64
	for _, spec := range sess.Chunks {
		rc, size, err := s.blobs.Open(sess.Uploaded[spec.Index])
		if err != nil {
			return nil, &ChunkMissingError{SessionID: sessionID, Missing: []int{spec.Index}}
		}
		if size != spec.Size {
			rc.Close()
			return nil, &DigestMismatchError{Scope: "chunk", Index: spec.Index, Expected: spec.Hash, ExpectedSize: spec.Size, GotSize: size}
		}
		n, err := io.Copy(h, rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		total += n
	}
	gotDigest := Digest(hex.EncodeToString(h.Sum(nil)))
	if total != sess.TotalSize || gotDigest != sess.Digest {
		return nil, &DigestMismatchError{
			Scope:        "entry",
			Expected:     sess.Digest,
			Got:          gotDigest,
			ExpectedSize: sess.TotalSize,
			GotSize:      total,
		}
	}

	now := s.clock.Now()

	// 3) 版本条件发布。
	if existing, ok := s.entries[sess.Key]; ok {
		if existing.Digest == sess.Digest {
			// 同摘要：内容寻址下值相同，复用现有结果（幂等）。
			sess.State = SessionCompleted
			sess.CompletedAt = now
			sess.ReusedEntry = true
			if err := s.persistSessionLocked(sess); err != nil {
				sess.State = SessionActive
				sess.CompletedAt = time.Time{}
				sess.ReusedEntry = false
				return nil, err
			}
			return cloneEntry(existing), nil
		}
		// 异值：版本条件失败，拒绝覆盖。
		return nil, &VersionConflictError{
			Key:             sess.Key,
			ExistingVersion: existing.Version,
			ExistingDigest:  existing.Digest,
			RequestedDigest: sess.Digest,
		}
	}

	chunks := make([]ChunkSpec, len(sess.Chunks))
	copy(chunks, sess.Chunks)
	entry := &Entry{
		Key:       sess.Key,
		Digest:    sess.Digest,
		Size:      total,
		Chunks:    chunks,
		Version:   1, // 首次发布版本为 1；异值发布永不允许，故不会出现同键多版本
		CreatedAt: now,
	}
	// 先原子写条目文件，再放入内存 map —— 两者在同一把锁内，读者只会看到已落盘的条目。
	if err := s.persistEntryLocked(entry); err != nil {
		return nil, err
	}
	s.entries[sess.Key] = entry

	sess.State = SessionCompleted
	sess.CompletedAt = now
	sess.PublishedKey = true
	if err := s.persistSessionLocked(sess); err != nil {
		// 条目已落盘且在内存生效；回滚会话标记，让调用方可以重试 Complete，
		// 重试会走“复用现有同摘要条目”的幂等路径。
		sess.State = SessionActive
		sess.CompletedAt = time.Time{}
		sess.PublishedKey = false
		return nil, err
	}
	return cloneEntry(entry), nil
}

// ---------------- 读取 ----------------

// Get 读取一个已发布的缓存条目。未发布（或不存在）时返回 ErrEntryNotFound：
// 未完成会话上传的任何分片都不可能通过该接口被观察到。
func (s *Store) Get(key string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return nil, fmt.Errorf("key %q: %w", key, ErrEntryNotFound)
	}
	return cloneEntry(e), nil
}

// OpenEntry 打开已发布条目的拼接内容流（按分片顺序串联）。
// 调用方必须关闭返回的 ReadCloser。
func (s *Store) OpenEntry(key string) (io.ReadCloser, *Entry, error) {
	e, err := s.Get(key)
	if err != nil {
		return nil, nil, err
	}
	readers := make([]io.Reader, 0, len(e.Chunks))
	closers := make([]io.Closer, 0, len(e.Chunks))
	for _, c := range e.Chunks {
		rc, _, err := s.blobs.Open(c.Hash)
		if err != nil {
			for _, cl := range closers {
				cl.Close()
			}
			return nil, nil, err
		}
		readers = append(readers, rc)
		closers = append(closers, rc)
	}
	return &entryReader{Reader: io.MultiReader(readers...), closers: closers}, e, nil
}

type entryReader struct {
	io.Reader
	closers []io.Closer
	closed  bool
}

func (r *entryReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var firstErr error
	for _, c := range r.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ---------------- 取消 ----------------

// Cancel 取消一个上传会话。已完成的会话不能取消；取消会终结会话，
// 其独占的内容块随后可被垃圾回收（共享块在仍有其他引用时保留）。
func (s *Store) Cancel(sessionID string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkClosedLocked(); err != nil {
		return err
	}
	sess, ok := s.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %s: %w", sessionID, ErrSessionNotFound)
	}
	switch sess.State {
	case SessionCompleted:
		return fmt.Errorf("session %s: %w", sessionID, ErrSessionCompleted)
	case SessionCancelled:
		return nil // 幂等
	case SessionExpiredMark:
		return fmt.Errorf("session %s: %w", sessionID, ErrLeaseExpired)
	}
	now := s.clock.Now()
	sess.State = SessionCancelled
	sess.EndedAt = now
	sess.Reason = reason
	if err := s.persistSessionLocked(sess); err != nil {
		sess.State = SessionActive
		sess.EndedAt = time.Time{}
		sess.Reason = ""
		return err
	}
	return nil
}

// ---------------- 租约与过期 ----------------

// RenewLease 将未过期会话的租约延长到 now + TTL。已过期的会话不能续期，
// 即租约失效后无法通过任何操作“复活”。
func (s *Store) RenewLease(sessionID string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkClosedLocked(); err != nil {
		return nil, err
	}
	sess, err := s.liveSessionLocked(sessionID)
	if err != nil {
		return nil, err
	}
	sess.ExpiresAt = s.clock.Now().Add(s.leaseTTL)
	if err := s.persistSessionLocked(sess); err != nil {
		return nil, err
	}
	return cloneSession(sess), nil
}

// Session 返回会话当前状态的快照。不存在返回 ErrSessionNotFound。
func (s *Store) Session(id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session %s: %w", id, ErrSessionNotFound)
	}
	return cloneSession(sess), nil
}

// ExpireSessions 终结所有租约已过期的活跃会话。过期判断统一使用注入的 Clock。
// 与上传/完成并发执行时：
//   - 正在进行的 PutChunk/Complete 持有“活跃”检查后的执行窗口，但其元数据提交
//     会在同一把锁下重新确认活跃状态，过期后提交一律失败，失效会话不会复活；
//   - 已完成发布的会话不受影响，发布产生的条目也不会被当作过期内容回收。
//
// 返回被终结的会话 ID 数量。
func (s *Store) ExpireSessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0
	}
	now := s.clock.Now()
	n := 0
	for _, sess := range s.sessions {
		if sess.State != SessionActive || now.Before(sess.ExpiresAt) {
			continue
		}
		sess.State = SessionExpiredMark
		sess.EndedAt = now
		sess.Reason = "lease expired"
		if err := s.persistSessionLocked(sess); err != nil {
			// 落盘失败：恢复内存状态，留待下一轮清理，避免“内存已过期/磁盘仍活跃”的不一致。
			sess.State = SessionActive
			sess.EndedAt = time.Time{}
			sess.Reason = ""
			continue
		}
		n++
	}
	return n
}

// ---------------- 垃圾回收 ----------------

// CollectGarbage 执行一轮垃圾回收。
//
// 只删除“未被任何已发布键或活跃会话引用”的内容块。为防住标记与删除之间
// 并发发布即将建立的引用，采用两阶段屏障：
//  1. 持锁标记（快照）所有当前引用；
//  2. 释放锁，等待所有在途发布（Complete）排空 —— 在途发布登记于 inflight，
//     屏障期间新开始的发布会被下一轮回收；
//  3. 重新持锁再次标记：只有两轮都未被引用的内容块才会删除。
//
// 每个内容块的处置（保留原因/删除/缺失）都会写入审计日志（gc-audit.log）。
// 同一时刻只允许一轮 GC，并发调用返回 ErrGCBusy。
func (s *Store) CollectGarbage() (*GCResult, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if s.gcRunning {
		s.mu.Unlock()
		return nil, ErrGCBusy
	}
	s.gcRunning = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.gcRunning = false
		s.mu.Unlock()
	}()

	runID := newRunID()
	started := s.clock.Now()
	result := &GCResult{RunID: runID, StartedAt: started}

	// 第一阶段：标记。
	marked1, sizes, reasons1, err := s.markReferences()
	if err != nil {
		return nil, err
	}
	if s.testHooks.afterFirstMark != nil {
		s.testHooks.afterFirstMark()
	}

	// 屏障：等待在途发布排空。Complete 在获取锁之前就已登记 inflight，
	// 因此 Wait 返回后，不存在“已决定发布但引用尚未计入”的发布。
	s.inflight.Wait()

	// 第二阶段：持锁重新标记，并对两轮都不可达的内容块做出处置。
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	now := s.clock.Now()
	marked2, reasons, err := s.markReferencesLocked()
	if err != nil {
		return nil, err
	}

	all, err := s.blobs.listAll()
	if err != nil {
		return nil, err
	}
	result.FinishedAt = now

	deleteSet := make(map[Digest]bool)
	for _, d := range all {
		result.Scanned++
		decision := GCDecision{RunID: runID, Time: now, Digest: d, Size: sizes[d]}
		switch {
		case marked2[d]:
			reason2 := reasons[d]
			if reason2 == "" {
				reason2 = "reference"
			}
			// 第一轮仅被会话引用（或不可达），第二轮被已发布条目引用，
			// 而第一轮尚不存在该条目引用：引用由屏障窗口内的并发发布建立。
			if !strings.HasPrefix(reasons1[d], "entry:") && strings.HasPrefix(reason2, "entry:") {
				decision.Action = GCKeepInFlight
				decision.Reason = "concurrent publish established reference during barrier (" + reason2 + ")"
			} else {
				decision.Action = actionForReference(reason2)
				decision.Reason = reason2
			}
			decision.Referenced = true
			result.Kept++
		case marked1[d]:
			// 第一轮被引用、屏障后不再引用：引用来自刚好终结的会话。
			// 引用它的发布若在屏障窗口内完成，则必然已进入 marked2；
			// 走到这里说明没有任何已发布条目引用它，可以安全删除。
			decision.Action = GCDelete
			decision.Referenced = false
			decision.Reason = "reference vanished before barrier; no published entry references it"
			deleteSet[d] = true
			result.Deleted++
		default:
			decision.Action = GCDelete
			decision.Referenced = false
			deleteSet[d] = true
			result.Deleted++
		}
		result.Decisions = append(result.Decisions, decision)
	}

	// 落盘审计记录先于实际删除，保证“决定删什么”可追溯（即使删除中途崩溃）。
	if err := s.appendAuditLocked(result.Decisions); err != nil {
		return nil, err
	}

	var retries []GCDecision
	for d := range deleteSet {
		if err := s.blobs.remove(d); err != nil {
			// 删除失败：记录一条保留决策并继续，下一轮 GC 会重试。
			decision := GCDecision{
				RunID: runID, Time: s.clock.Now(), Digest: d, Size: sizes[d],
				Action: GCRetryRemoveFailed, Referenced: false, Reason: err.Error(),
			}
			retries = append(retries, decision)
			result.Decisions = append(result.Decisions, decision)
			result.Deleted--
			result.Kept++
			continue
		}
		result.DeletedSize += sizes[d]
	}
	// 删除失败的决策同样必须可审计。
	if len(retries) > 0 {
		if err := s.appendAuditLocked(retries); err != nil {
			return result, err
		}
	}
	return result, nil
}

// markReferences 返回当前所有被引用内容块的集合、已知块大小映射，
// 以及每个块的保留原因（entry:<key> 或 session:<id>）。
// 不持有锁调用（GC 屏障前使用）；内部加锁。
func (s *Store) markReferences() (map[Digest]bool, map[Digest]int64, map[Digest]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	marked, reasons, err := s.markReferencesLocked()
	if err != nil {
		return nil, nil, nil, err
	}
	sizes := make(map[Digest]int64)
	for d := range marked {
		if size, ok, err := s.blobs.stat(d); err == nil && ok {
			sizes[d] = size
		}
	}
	// 也补上未标记块的大小，供审计使用。
	all, err := s.blobs.listAll()
	if err != nil {
		return nil, nil, nil, err
	}
	for _, d := range all {
		if _, ok := sizes[d]; !ok {
			if size, ok, err := s.blobs.stat(d); err == nil && ok {
				sizes[d] = size
			}
		}
	}
	return marked, sizes, reasons, nil
}

// actionForReference 把内部保留原因映射为对外的 GCAction。
func actionForReference(reason string) GCAction {
	if strings.HasPrefix(reason, "entry:") {
		return GCKeepMarkedPublished
	}
	return GCKeepMarkedSession
}

// markReferencesLocked 计算引用闭包：
//   - 所有已发布条目声明的分片；
//   - 所有未过期会话（active 且租约有效，或已完成会话）声明的分片与已上传分片。
//
// reasons[d] 给出一个保留原因（entry:<key> 或 session:<id>）。
func (s *Store) markReferencesLocked() (map[Digest]bool, map[Digest]string, error) {
	marked := make(map[Digest]bool)
	reasons := make(map[Digest]string)
	now := s.clock.Now()

	for key, e := range s.entries {
		for _, c := range e.Chunks {
			marked[c.Hash] = true
			if _, ok := reasons[c.Hash]; !ok {
				reasons[c.Hash] = "entry:" + key
			}
		}
	}
	for _, sess := range s.sessions {
		keep := false
		switch sess.State {
		case SessionActive:
			keep = now.Before(sess.ExpiresAt)
		case SessionCompleted:
			keep = true
		}
		if !keep {
			continue
		}
		// 活跃会话：声明集合中的块即受保护——上传可能随时发生，
		// 且客户端重传依赖这些块可被重新创建/已存在。
		for _, c := range sess.Chunks {
			marked[c.Hash] = true
			if _, ok := reasons[c.Hash]; !ok {
				reasons[c.Hash] = "session:" + sess.ID
			}
		}
	}
	return marked, reasons, nil
}

// appendAuditLocked 将一轮 GC 的全部决策以 JSON Lines 追加到审计日志。
func (s *Store) appendAuditLocked(decisions []GCDecision) error {
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	f, err := os.OpenFile(filepath.Join(s.root, auditFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for i := range decisions {
		if err := enc.Encode(&decisions[i]); err != nil {
			return err
		}
	}
	return f.Sync()
}

// AuditLog 读取审计日志中的全部 GC 决策（便于检查/导出）。
func (s *Store) AuditLog() ([]GCDecision, error) {
	s.auditMu.Lock()
	f, err := os.Open(filepath.Join(s.root, auditFile))
	s.auditMu.Unlock()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []GCDecision
	dec := json.NewDecoder(f)
	for {
		var d GCDecision
		if err := dec.Decode(&d); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// ---------------- 内部辅助 ----------------

// liveSessionLocked 取得可操作的会话，统一处理不存在/过期/已终结的判定。
// 调用方需持有 s.mu。
func (s *Store) liveSessionLocked(id string) (*Session, error) {
	sess, ok := s.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session %s: %w", id, ErrSessionNotFound)
	}
	now := s.clock.Now()
	switch sess.State {
	case SessionActive:
		if !now.Before(sess.ExpiresAt) {
			// 惰性标记：把过期状态落盘，过期操作不依赖显式清理轮次。
			sess.State = SessionExpiredMark
			sess.EndedAt = now
			sess.Reason = "lease expired"
			_ = s.persistSessionLocked(sess)
			return nil, fmt.Errorf("session %s: %w", id, ErrLeaseExpired)
		}
		return sess, nil
	case SessionCompleted:
		return nil, fmt.Errorf("session %s: %w", id, ErrSessionCompleted)
	case SessionCancelled:
		return nil, fmt.Errorf("session %s: %w", id, ErrSessionCancelled)
	default:
		return nil, fmt.Errorf("session %s: %w", id, ErrLeaseExpired)
	}
}

func (s *Store) checkClosedLocked() error {
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) persistSessionLocked(sess *Session) error {
	return atomicWriteJSON(filepath.Join(s.root, sessionsDir, sess.ID+".json"), sess)
}

func (s *Store) persistEntryLocked(e *Entry) error {
	return atomicWriteJSON(filepath.Join(s.root, entriesDir, entryFileName(e.Key)), e)
}

// entryFileName 对键做十六进制转义，保证任意字符串键都对应安全的文件名。
func entryFileName(key string) string {
	return hex.EncodeToString([]byte(key)) + ".json"
}

// loadLocked 从磁盘恢复会话与条目。
func (s *Store) loadLocked() error {
	sessFiles, err := os.ReadDir(filepath.Join(s.root, sessionsDir))
	if err != nil {
		return err
	}
	for _, f := range sessFiles {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var sess Session
		if err := readJSONFile(filepath.Join(s.root, sessionsDir, f.Name()), &sess); err != nil {
			return fmt.Errorf("recover session %s: %w", f.Name(), err)
		}
		s.sessions[sess.ID] = &sess
	}

	entryFiles, err := os.ReadDir(filepath.Join(s.root, entriesDir))
	if err != nil {
		return err
	}
	for _, f := range entryFiles {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var e Entry
		if err := readJSONFile(filepath.Join(s.root, entriesDir, f.Name()), &e); err != nil {
			return fmt.Errorf("recover entry %s: %w", f.Name(), err)
		}
		// 条目发布时先落盘后置入 map；若崩溃发生在两者之间，磁盘上会有一个
		// 孤儿条目文件。恢复时同样以“文件存在即已发布”为准（它本身完整且原子）。
		s.entries[e.Key] = &e
	}
	return nil
}

func cloneSession(s *Session) *Session {
	cp := *s
	cp.Chunks = make([]ChunkSpec, len(s.Chunks))
	copy(cp.Chunks, s.Chunks)
	cp.Uploaded = make([]Digest, len(s.Uploaded))
	copy(cp.Uploaded, s.Uploaded)
	return &cp
}

func cloneEntry(e *Entry) *Entry {
	cp := *e
	cp.Chunks = make([]ChunkSpec, len(e.Chunks))
	copy(cp.Chunks, e.Chunks)
	return &cp
}

func newSessionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("s_%x", b[:])
}

func newRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("gc_%x", b[:])
}

// ---------------- 原子 JSON 文件 ----------------

func atomicWriteJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".meta-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
