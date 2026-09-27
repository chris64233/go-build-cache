package buildcache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// 本文件实现四件事：
//
//  1. 命名空间配额注册与查询；
//  2. 两阶段 LRU 淘汰：先"选定候选"（决策），真正删除前再逐个确认，
//     候选在决策之后被访问 / 固定 / 重新发布都会使旧决定失效；
//  3. 固定租约：带严格递增版本的 pin / renew / release，过期扫描，
//     以及"请求号相同且内容相同返回原结果，内容不同即冲突"的幂等；
//  4. 引用查询：配额、租约、淘汰候选、内容块引用来源。
//
// 所有方法都在 c.mu 下串行，与发布 / 读取 / GC 互斥，因此决策与确认之间
// 的状态变化只能来自确认阶段本身读取到的最新状态。

// ---- 命名空间配额 ----

// RegisterNamespace 注册（或幂等获取）一个命名空间。maxBytes<=0 表示不限配额。
// 对已存在的同名命名空间，本方法只返回当前状态，不修改配额；修改配额用 SetNamespaceQuota。
func (c *Cache) RegisterNamespace(name string, maxBytes int64) (*Namespace, error) {
	if name == "" {
		return nil, errors.New("buildcache: namespace name is required")
	}
	if maxBytes < 0 {
		return nil, errors.New("buildcache: namespace max bytes must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, err := c.store.GetNamespace(name); err == nil {
		return &existing, nil
	} else if !errors.Is(err, ErrNamespaceNotFound) {
		return nil, err
	}
	ns := Namespace{Name: name, MaxBytes: maxBytes, UpdatedAt: c.clock.Now()}
	if err := c.store.SaveNamespace(ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

// SetNamespaceQuota 更新命名空间最大字节数（<=0 表示不限）。
// 收缩配额不会立即淘汰，超限部分在下次发布时按 LRU 回收。
func (c *Cache) SetNamespaceQuota(name string, maxBytes int64) (*Namespace, error) {
	if maxBytes < 0 {
		return nil, errors.New("buildcache: namespace max bytes must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, err := c.store.GetNamespace(name)
	if err != nil {
		return nil, err
	}
	ns.MaxBytes = maxBytes
	ns.UpdatedAt = c.clock.Now()
	if err := c.store.SaveNamespace(ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

// GetNamespace 返回命名空间配额元数据。
func (c *Cache) GetNamespace(name string) (Namespace, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.GetNamespace(name)
}

// ListNamespaces 返回全部命名空间。
func (c *Cache) ListNamespaces() ([]Namespace, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.ListNamespaces()
}

// QuotaInfo 是配额查询结果：配额、用量以及按固定状态拆分的字节数。
type QuotaInfo struct {
	Namespace
	EntryCount     int   // 当前条目数
	PinnedKeys     int   // 持有有效固定的键数
	PinnedBytes    int64 // 有效固定条目占用字节
	EvictableBytes int64 // 未固定、可被 LRU 淘汰的字节
}

// QuotaUsage 查询命名空间配额与用量明细。
func (c *Cache) QuotaUsage(namespace string) (*QuotaInfo, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, err := c.store.GetNamespace(namespace)
	if err != nil {
		return nil, err
	}
	entries, err := c.store.ListNamespaceEntries(namespace)
	if err != nil {
		return nil, err
	}
	info := &QuotaInfo{Namespace: ns, EntryCount: len(entries)}
	for _, e := range entries {
		if p, err := c.store.GetPin(namespace, e.Key); err == nil && p.Active(c.clock.Now()) {
			info.PinnedKeys++
			info.PinnedBytes += e.TotalSize
		} else {
			info.EvictableBytes += e.TotalSize
		}
	}
	return info, nil
}

// ---- 两阶段淘汰 ----

// 淘汰候选在"决策时刻"的状态快照。确认阶段以版本 / 摘要 / 访问序号三重判据
// 判断候选是否已经失效。
type EvictionCandidate struct {
	Namespace      string    `json:"namespace"`
	Key            string    `json:"key"`
	Version        uint64    `json:"version"`
	Digest         Digest    `json:"digest"`
	Size           int64     `json:"size"`
	LastAccessedAt time.Time `json:"last_accessed_at"`
	// AccessSeq 决策时条目的访问序号；确认时不一致说明决策之后发生过访问。
	AccessSeq uint64 `json:"access_seq"`
}

// EvictionDecision 是一次"选定淘汰候选"的决策。它是自包含的不可变对象：
// 调用方持它在任意时刻调用 CommitEviction，确认逻辑只对照最新状态。
type EvictionDecision struct {
	ID         string              `json:"id"`
	Namespace  string              `json:"namespace"`
	CreatedAt  time.Time           `json:"created_at"`
	NeedBytes  int64               `json:"need_bytes"`
	Candidates []EvictionCandidate `json:"candidates"`
}

// StaleCandidate 记录一个在确认阶段被判定失效的候选及原因。
type StaleCandidate struct {
	Candidate EvictionCandidate `json:"candidate"`
	Reason    string            `json:"reason"`
}

// 淘汰候选失效原因。
const (
	EvictStaleAccessed    = "accessed"    // 决策之后被读取
	EvictStaleRepublished = "republished" // 决策之后被重新发布（版本/摘要变化）
	EvictStalePinned      = "pinned"      // 决策之后被有效固定
	EvictStaleMissing     = "entry_gone"  // 条目已不存在
)

// EvictionReport 是确认（commit）阶段的执行结果。
type EvictionReport struct {
	DecisionID     string              `json:"decision_id"`
	Deleted        []EvictionCandidate `json:"deleted"`
	Stale          []StaleCandidate    `json:"stale"`
	ReclaimedBytes int64               `json:"reclaimed_bytes"`
}

// PlanEviction 进入淘汰第一阶段：在命名空间内按"未固定 → 最近最少使用 →
// 键名升序"选出累计至少 needBytes 的候选，写审计但不删除任何条目。
// 决策对象可在之后任意时刻交给 CommitEviction；期间被访问 / 固定 / 重新发布
// 的候选会在确认时失效。
func (c *Cache) PlanEviction(namespace string, needBytes int64) (*EvictionDecision, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	if needBytes <= 0 {
		return nil, errors.New("buildcache: need bytes must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, err := c.store.GetNamespace(namespace)
	if err != nil {
		return nil, err
	}
	return c.planEvictionLocked(&ns, needBytes, c.clock.Now())
}

// CommitEviction 进入淘汰第二阶段：逐个候选对照最新状态确认后才真正删除。
// 决策中每个候选的去留与失效原因都写入审计日志。
func (c *Cache) CommitEviction(d *EvictionDecision) (*EvictionReport, error) {
	if d == nil {
		return nil, errors.New("buildcache: eviction decision is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ns, err := c.store.GetNamespace(d.Namespace)
	if err != nil {
		return nil, err
	}
	return c.commitEvictionLocked(&ns, d, c.clock.Now())
}

// EvictionOrder 是淘汰决定查询：返回此刻命名空间内按 LRU + 键名排序的全部
// 可淘汰候选（不产生决策、不写审计、不删除）。
func (c *Cache) EvictionOrder(namespace string) ([]EvictionCandidate, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.store.GetNamespace(namespace); err != nil {
		return nil, err
	}
	entries, err := c.store.ListNamespaceEntries(namespace)
	if err != nil {
		return nil, err
	}
	now := c.clock.Now()
	candidates := c.selectEvictionCandidatesLocked(namespace, entries, now, -1)
	return candidates, nil
}

// enforceQuotaForPublish 在新条目真正写入前调用：若发布会使命名空间超过配额，
// 立即完成一次 plan→confirm 的 LRU 回收（同一临界区内没有并发，候选必然仍有效）。
// old 为被覆盖的同键旧条目（新增时为 nil），其字节将被替换而非叠加。
// 调用方持有 c.mu。
func (c *Cache) enforceQuotaForPublish(ns *Namespace, old *Entry, newSize int64, now time.Time) error {
	if ns.MaxBytes <= 0 {
		return nil
	}
	var oldSize int64
	if old != nil {
		oldSize = old.TotalSize
	}
	projected := ns.UsedBytes - oldSize + newSize
	if projected <= ns.MaxBytes {
		return nil
	}
	need := projected - ns.MaxBytes

	entries, err := c.store.ListNamespaceEntries(ns.Name)
	if err != nil {
		return err
	}
	// 正在被覆盖的同键旧条目将被替换（而非叠加占用），不能把它选作淘汰候选——
	// 否则删除后带版本条件的覆盖写入会因键不存在而失败。
	if old != nil {
		filtered := entries[:0]
		for _, e := range entries {
			if e.Key != old.Key {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	candidates := c.selectEvictionCandidatesLocked(ns.Name, entries, now, need)
	var reclaimable int64
	for _, cand := range candidates {
		reclaimable += cand.Size
	}
	if reclaimable < need {
		// 未固定条目全部腾出仍不够（典型：一个超大的新条目，或空间都被固定占用）。
		return &QuotaExceededError{
			Namespace: ns.Name, MaxBytes: ns.MaxBytes,
			UsedBytes: ns.UsedBytes - reclaimable, NeedBytes: newSize,
		}
	}
	d := &EvictionDecision{
		ID: newDecisionID(), Namespace: ns.Name, CreatedAt: now,
		NeedBytes: need, Candidates: candidates,
	}
	if err := c.auditEvictionSelected(d, now); err != nil {
		return err
	}
	if _, err := c.commitEvictionLocked(ns, d, now); err != nil {
		return err
	}
	return nil
}

// selectEvictionCandidatesLocked 按 未固定 → LastAccessedAt 升序 → Key 升序 排序，
// 返回累计字节达到 need 即止的候选；need<0 时返回全部可淘汰候选（查询用）。
// 调用方持有 c.mu。
func (c *Cache) selectEvictionCandidatesLocked(namespace string, entries []Entry, now time.Time, need int64) []EvictionCandidate {
	type ranked struct{ e Entry }
	var eligible []ranked
	for _, e := range entries {
		if p, err := c.store.GetPin(namespace, e.Key); err == nil && p.Active(now) {
			continue // 有效固定：永不淘汰
		}
		eligible = append(eligible, ranked{e})
	}
	sort.Slice(eligible, func(i, j int) bool {
		a, b := eligible[i].e, eligible[j].e
		if !a.LastAccessedAt.Equal(b.LastAccessedAt) {
			return a.LastAccessedAt.Before(b.LastAccessedAt)
		}
		return a.Key < b.Key // 键名稳定排序：时间相同时结果确定
	})
	var out []EvictionCandidate
	var total int64
	for _, r := range eligible {
		if need >= 0 && total >= need {
			break
		}
		e := r.e
		out = append(out, EvictionCandidate{
			Namespace: namespace, Key: e.Key, Version: e.Version, Digest: e.Digest,
			Size: e.TotalSize, LastAccessedAt: e.LastAccessedAt, AccessSeq: e.AccessSeq,
		})
		total += e.TotalSize
	}
	return out
}

func (c *Cache) planEvictionLocked(ns *Namespace, need int64, now time.Time) (*EvictionDecision, error) {
	entries, err := c.store.ListNamespaceEntries(ns.Name)
	if err != nil {
		return nil, err
	}
	candidates := c.selectEvictionCandidatesLocked(ns.Name, entries, now, need)
	var reclaimable int64
	for _, cand := range candidates {
		reclaimable += cand.Size
	}
	if reclaimable < need {
		return nil, &QuotaExceededError{
			Namespace: ns.Name, MaxBytes: ns.MaxBytes,
			UsedBytes: ns.UsedBytes - reclaimable, NeedBytes: need,
		}
	}
	d := &EvictionDecision{
		ID: newDecisionID(), Namespace: ns.Name, CreatedAt: now,
		NeedBytes: need, Candidates: candidates,
	}
	if err := c.auditEvictionSelected(d, now); err != nil {
		return nil, err
	}
	return d, nil
}

// auditEvictionSelected 记录一个淘汰决策中全部候选的"选定"事件。
// 调用方持有 c.mu。
func (c *Cache) auditEvictionSelected(d *EvictionDecision, now time.Time) error {
	for _, cand := range d.Candidates {
		if err := c.store.AppendAudit(GCRecord{
			Time: now, Action: GCEvictSelected, Namespace: d.Namespace,
			Key: cand.Key, Version: cand.Version,
			Detail: fmt.Sprintf("decision=%s size=%d access_seq=%d", d.ID, cand.Size, cand.AccessSeq),
		}); err != nil {
			return err
		}
	}
	return nil
}

// commitEvictionLocked 对每个候选做删除前最终确认。调用方持有 c.mu。
func (c *Cache) commitEvictionLocked(ns *Namespace, d *EvictionDecision, now time.Time) (*EvictionReport, error) {
	report := &EvictionReport{DecisionID: d.ID}
	for _, cand := range d.Candidates {
		entry, err := c.store.GetEntry(d.Namespace, cand.Key)
		switch {
		case errors.Is(err, ErrEntryNotFound):
			report.Stale = append(report.Stale, StaleCandidate{Candidate: cand, Reason: EvictStaleMissing})
			if err := c.auditEvictStale(d.ID, cand, EvictStaleMissing, now); err != nil {
				return nil, err
			}
			continue
		case err != nil:
			return nil, err
		}

		// 重新发布（版本或摘要变化）→ 旧决定失效。
		if entry.Version != cand.Version || entry.Digest != cand.Digest {
			report.Stale = append(report.Stale, StaleCandidate{Candidate: cand, Reason: EvictStaleRepublished})
			if err := c.auditEvictStale(d.ID, cand, EvictStaleRepublished, now); err != nil {
				return nil, err
			}
			continue
		}
		// 决策之后发生过读取（同摘要复用发布也会推进访问序号）→ 旧决定失效。
		if entry.AccessSeq != cand.AccessSeq {
			report.Stale = append(report.Stale, StaleCandidate{Candidate: cand, Reason: EvictStaleAccessed})
			if err := c.auditEvictStale(d.ID, cand, EvictStaleAccessed, now); err != nil {
				return nil, err
			}
			continue
		}
		// 决策之后被有效固定 → 旧决定失效。
		if p, err := c.store.GetPin(d.Namespace, cand.Key); err == nil && p.Active(now) {
			report.Stale = append(report.Stale, StaleCandidate{Candidate: cand, Reason: EvictStalePinned})
			if err := c.auditEvictStale(d.ID, cand, EvictStalePinned, now); err != nil {
				return nil, err
			}
			continue
		}

		// 三重判据全部通过：真正删除。
		if err := c.store.DeleteEntry(d.Namespace, cand.Key); err != nil {
			return nil, err
		}
		ns.UsedBytes -= cand.Size
		if ns.UsedBytes < 0 {
			ns.UsedBytes = 0
		}
		ns.UpdatedAt = now
		if err := c.store.SaveNamespace(*ns); err != nil {
			return nil, err
		}
		report.Deleted = append(report.Deleted, cand)
		report.ReclaimedBytes += cand.Size
		if err := c.store.AppendAudit(GCRecord{
			Time: now, Action: GCEvictDeleted, Namespace: d.Namespace,
			Key: cand.Key, Version: cand.Version,
			Detail: fmt.Sprintf("decision=%s reclaimed=%d", d.ID, cand.Size),
		}); err != nil {
			return nil, err
		}
	}
	return report, nil
}

func (c *Cache) auditEvictStale(decisionID string, cand EvictionCandidate, reason string, now time.Time) error {
	return c.store.AppendAudit(GCRecord{
		Time: now, Action: GCEvictStale, Namespace: cand.Namespace,
		Key: cand.Key, Version: cand.Version, Reason: reason,
		Detail: "decision=" + decisionID,
	})
}

func newDecisionID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand 失败在实践中不会发生；退回时间戳仍能保证进程内唯一性足够。
		return "dec_" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return "dec_" + hex.EncodeToString(b[:])
}

// ---- 固定租约 ----

// 固定类操作的请求号去重命名空间（与会话幂等键的进程内语义一致）。
const (
	pinOpAcquire = "pin"
	pinOpRenew   = "renew"
	pinOpRelease = "release"
)

// PinOptions 是固定 / 续租 / 解除的请求参数。
type PinOptions struct {
	Namespace string
	Key       string
	// ExpectedDigest 仅固定（acquire）时必填：目标条目不存在或当前摘要不匹配都拒绝。
	ExpectedDigest Digest
	// ExpiresAt 与 TTL 二选一给出截止时间；都为空时固定必须显式给出。
	ExpiresAt time.Time
	TTL       time.Duration
	// ExpectedVersion 续租 / 解除时必填：只能操作指定版本的租约。
	ExpectedVersion *uint64
	// RequestID 请求号：相同请求号 + 相同参数返回首次结果，参数不同即冲突。
	RequestID string
}

// PinResult 是固定类操作的结果。
type PinResult struct {
	Lease    PinLease `json:"lease"`
	Replayed bool     `json:"replayed"` // true 表示命中请求号重放，未执行新操作
}

func (o PinOptions) ns() string {
	if o.Namespace == "" {
		return DefaultNamespace
	}
	return o.Namespace
}

func (o PinOptions) deadline(now time.Time) time.Time {
	if !o.ExpiresAt.IsZero() {
		return o.ExpiresAt
	}
	return now.Add(o.TTL)
}

// Pin 把指定缓存键固定到某个截止时间。
// 固定只作用于存在且摘要匹配的已发布条目；租约带严格递增版本，
// 对已存在（含已过期 / 已解除）的租约重新固定会得到 version+1 的新租约。
func (c *Cache) Pin(opts PinOptions) (*PinResult, error) {
	if opts.Key == "" {
		return nil, errors.New("buildcache: cache key is required")
	}
	if !opts.ExpectedDigest.Valid() {
		return nil, errors.New("buildcache: expected digest is required to pin")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	namespace := opts.ns()
	if _, err := c.store.GetNamespace(namespace); err != nil {
		return nil, err
	}
	now := c.clock.Now()
	expires := opts.deadline(now)
	if !expires.After(now) {
		return nil, fmt.Errorf("%w: pin expiry must be in the future", ErrLeaseExpired)
	}

	if opts.RequestID != "" {
		if rec, ok, err := c.lookupPinRequest(opts.RequestID, pinOpAcquire, opts, now); err != nil {
			return nil, err
		} else if ok {
			return &PinResult{Lease: rec.result.Lease, Replayed: true}, nil
		}
	}

	entry, err := c.store.GetEntry(namespace, opts.Key)
	if errors.Is(err, ErrEntryNotFound) {
		return nil, &PinConflictError{Namespace: namespace, Key: opts.Key, Reason: PinMissingEntry}
	}
	if err != nil {
		return nil, err
	}
	if entry.Digest != opts.ExpectedDigest {
		return nil, &PinConflictError{
			Namespace: namespace, Key: opts.Key, Reason: PinDigestMismatch,
			Want: opts.ExpectedDigest, Got: entry.Digest,
		}
	}

	var version uint64 = 1
	if prior, err := c.store.GetPin(namespace, opts.Key); err == nil {
		version = prior.Version + 1
	} else if !errors.Is(err, ErrPinNotFound) {
		return nil, err
	}
	lease := PinLease{
		Namespace: namespace, Key: opts.Key, Version: version,
		Digest: entry.Digest, Chunks: cloneEntry(entry).Chunks,
		CreatedAt: now, ExpiresAt: expires,
	}
	if err := c.store.SavePin(lease); err != nil {
		return nil, err
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPinAcquired, Namespace: namespace, Key: opts.Key,
		Version: version, Detail: "expires_at=" + expires.UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return nil, err
	}
	res := &PinResult{Lease: lease}
	c.rememberPinRequest(opts.RequestID, pinOpAcquire, opts, now, *res)
	return res, nil
}

// RenewPin 续租固定租约。只能延长、不能缩短：新截止时间必须晚于当前截止时间；
// 携带的 ExpectedVersion 必须等于当前租约版本，过期 / 已解除的租约不能被续租复活。
func (c *Cache) RenewPin(opts PinOptions) (*PinResult, error) {
	if opts.ExpectedVersion == nil {
		return nil, errors.New("buildcache: expected lease version is required to renew")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	namespace := opts.ns()
	now := c.clock.Now()
	if opts.RequestID != "" {
		if rec, ok, err := c.lookupPinRequest(opts.RequestID, pinOpRenew, opts, now); err != nil {
			return nil, err
		} else if ok {
			return &PinResult{Lease: rec.result.Lease, Replayed: true}, nil
		}
	}

	lease, err := c.store.GetPin(namespace, opts.Key)
	if err != nil {
		return nil, err
	}
	if lease.Version != *opts.ExpectedVersion {
		return nil, &PinVersionConflictError{
			Namespace: namespace, Key: opts.Key,
			CurrentVersion: lease.Version, ExpectedVersion: *opts.ExpectedVersion,
		}
	}
	if !lease.Active(now) {
		// 已过期或已解除：旧版本（乃至任何版本）的续租都不能让它复活；
		// 想继续固定请对当前条目重新 Pin。
		return nil, fmt.Errorf("%w: pin on %s/%s", ErrPinExpired, namespace, opts.Key)
	}
	expires := opts.deadline(now)
	if !expires.After(lease.ExpiresAt) {
		return nil, fmt.Errorf("buildcache: renew must extend beyond current expiry %s",
			lease.ExpiresAt.Format(time.RFC3339Nano))
	}
	lease.Version++
	lease.ExpiresAt = expires
	if err := c.store.SavePin(lease); err != nil {
		return nil, err
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPinRenewed, Namespace: namespace, Key: opts.Key,
		Version: lease.Version, Detail: "expires_at=" + expires.UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return nil, err
	}
	res := &PinResult{Lease: lease}
	c.rememberPinRequest(opts.RequestID, pinOpRenew, opts, now, *res)
	return res, nil
}

// ReleasePin 解除固定租约。ExpectedVersion 必须等于当前租约版本：
// 旧版本的解除请求不能解除更新的租约；重复解除同一版本按幂等成功返回。
func (c *Cache) ReleasePin(opts PinOptions) (*PinResult, error) {
	if opts.ExpectedVersion == nil {
		return nil, errors.New("buildcache: expected lease version is required to release")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	namespace := opts.ns()
	now := c.clock.Now()
	if opts.RequestID != "" {
		if rec, ok, err := c.lookupPinRequest(opts.RequestID, pinOpRelease, opts, now); err != nil {
			return nil, err
		} else if ok {
			return &PinResult{Lease: rec.result.Lease, Replayed: true}, nil
		}
	}

	lease, err := c.store.GetPin(namespace, opts.Key)
	if err != nil {
		return nil, err
	}
	if lease.Version != *opts.ExpectedVersion {
		return nil, &PinVersionConflictError{
			Namespace: namespace, Key: opts.Key,
			CurrentVersion: lease.Version, ExpectedVersion: *opts.ExpectedVersion,
		}
	}
	if !lease.Released {
		lease.Version++
		lease.Released = true
		if err := c.store.SavePin(lease); err != nil {
			return nil, err
		}
		if err := c.store.AppendAudit(GCRecord{
			Time: now, Action: GCPinReleased, Namespace: namespace, Key: opts.Key,
			Version: lease.Version,
		}); err != nil {
			return nil, err
		}
	}
	res := &PinResult{Lease: lease}
	c.rememberPinRequest(opts.RequestID, pinOpRelease, opts, now, *res)
	return res, nil
}

// GetPin 查询单个固定租约（含已解除但尚未被扫描清除的记录）。
func (c *Cache) GetPin(namespace, key string) (PinLease, error) {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.store.GetPin(namespace, key)
}

// ListPins 查询固定租约。namespace 为空表示全部命名空间；
// includeInactive 为 false 时只返回当前有效的租约。
func (c *Cache) ListPins(namespace string, includeInactive bool) ([]PinLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	all, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	now := c.clock.Now()
	var out []PinLease
	for _, p := range all {
		if namespace != "" && p.Namespace != namespace {
			continue
		}
		if !includeInactive && !p.Active(now) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// SweptPin 是一条被过期扫描清除的租约记录。
type SweptPin struct {
	Namespace string
	Key       string
	Version   uint64
	Reason    string // "expired" / "released"
}

// SweepExpiredPins 物理清除已到期（未解除）与已解除的固定租约记录。
// 清除后其快照块不再受固定保护，是否回收由下一次 GC 按统一引用集判定。
func (c *Cache) SweepExpiredPins() ([]SweptPin, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	all, err := c.store.ListPins()
	if err != nil {
		return nil, err
	}
	var swept []SweptPin
	for _, p := range all {
		switch {
		case p.Released:
			if err := c.store.DeletePin(p.Namespace, p.Key); err != nil {
				return nil, err
			}
			swept = append(swept, SweptPin{Namespace: p.Namespace, Key: p.Key, Version: p.Version, Reason: "released"})
		case !p.ExpiresAt.After(now):
			if err := c.store.DeletePin(p.Namespace, p.Key); err != nil {
				return nil, err
			}
			swept = append(swept, SweptPin{Namespace: p.Namespace, Key: p.Key, Version: p.Version, Reason: "expired"})
			if err := c.store.AppendAudit(GCRecord{
				Time: now, Action: GCPinExpired, Namespace: p.Namespace, Key: p.Key,
				Version: p.Version,
			}); err != nil {
				return nil, err
			}
		}
	}
	return swept, nil
}

// lookupPinRequest 处理请求号重放：同号同参返回首次结果，同号异参冲突。
// 比较的是请求中**给出的参数**（绝对截止时间或相对时长各自原样比较），
// 因此使用相对 ttl 的重试即使到达时刻不同也算同一请求。
// 第二个返回值为 true 表示命中重放。调用方持有 c.mu。
func (c *Cache) lookupPinRequest(requestID, op string, opts PinOptions, now time.Time) (pinRequestRecord, bool, error) {
	rec, ok := c.pinReqs[requestID]
	if !ok {
		return pinRequestRecord{}, false, nil
	}
	sameTime := rec.exp.Equal(opts.ExpiresAt) && rec.ttl == opts.TTL
	if rec.op != op || rec.ns != opts.ns() || rec.key != opts.Key || !sameTime ||
		rec.digest != opts.ExpectedDigest ||
		rec.hasVer != (opts.ExpectedVersion != nil) ||
		(opts.ExpectedVersion != nil && rec.ver != *opts.ExpectedVersion) {
		return pinRequestRecord{}, false, &RequestConflictError{RequestID: requestID}
	}
	return rec, true, nil
}

func (c *Cache) rememberPinRequest(requestID, op string, opts PinOptions, now time.Time, res PinResult) {
	if requestID == "" {
		return
	}
	rec := pinRequestRecord{
		op: op, ns: opts.ns(), key: opts.Key,
		exp: opts.ExpiresAt, ttl: opts.TTL, digest: opts.ExpectedDigest, result: res,
	}
	if opts.ExpectedVersion != nil {
		rec.hasVer = true
		rec.ver = *opts.ExpectedVersion
	}
	c.pinReqs[requestID] = rec
}

// ---- 内容块引用查询 ----

// BlobEntryRef 是已发布条目对块的引用。
type BlobEntryRef struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   uint64 `json:"version"`
	Index     int    `json:"index"`
}

// BlobSessionRef 是活跃上传会话对块的引用。
type BlobSessionRef struct {
	SessionID string `json:"session_id"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Index     int    `json:"index"`
}

// BlobPinRef 是有效固定租约对块的引用。
type BlobPinRef struct {
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	Version   uint64    `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
	Index     int       `json:"index"`
}

// BlobReferences 汇总一个内容块当前的全部引用来源。
// 三类引用都为空时，该块是下一次 GC 的回收对象。
type BlobReferences struct {
	Digest   Digest           `json:"digest"`
	Entries  []BlobEntryRef   `json:"entries"`
	Sessions []BlobSessionRef `json:"sessions"`
	Pins     []BlobPinRef     `json:"pins"`
}

// Referenced 报告块是否仍被任一已发布条目、活跃上传或有效固定引用。
func (r *BlobReferences) Referenced() bool {
	return len(r.Entries) > 0 || len(r.Sessions) > 0 || len(r.Pins) > 0
}

// BlobReferences 查询一个内容块的全部引用来源（GC 判定的只读视图）。
func (c *Cache) BlobReferences(d Digest) (*BlobReferences, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	refs := &BlobReferences{Digest: d}

	entries, err := c.store.ListEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		for _, ref := range e.Chunks {
			if ref.Digest == d {
				refs.Entries = append(refs.Entries, BlobEntryRef{
					Namespace: e.Namespace, Key: e.Key, Version: e.Version, Index: ref.Index,
				})
			}
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
			if rec.Digest == d {
				refs.Sessions = append(refs.Sessions, BlobSessionRef{
					SessionID: s.ID, Namespace: s.Namespace, Key: s.Key, Index: rec.Index,
				})
			}
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
			if ref.Digest == d {
				refs.Pins = append(refs.Pins, BlobPinRef{
					Namespace: p.Namespace, Key: p.Key, Version: p.Version,
					ExpiresAt: p.ExpiresAt, Index: ref.Index,
				})
			}
		}
	}
	return refs, nil
}
