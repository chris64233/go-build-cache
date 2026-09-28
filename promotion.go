package buildcache

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// PromoteOptions 是跨命名空间晋级请求参数。
type PromoteOptions struct {
	// SourceNamespace / SourceKey / ExpectedDigest 来源条目身份与其当前摘要。
	// 来源条目不存在、摘要不符或该键存在未完成上传会话时，晋级不得建立。
	SourceNamespace string
	SourceKey       string
	ExpectedDigest  Digest

	// TargetNamespace / TargetKey 目标条目身份；目标命名空间必须已注册。
	TargetNamespace string
	TargetKey       string
	// Mode 为 PromotionModeCopy（复制为新键，目标键必须不存在）或
	// PromotionModeReplace（替换同名旧条目）。
	Mode string

	// RequestID 外部请求号：相同请求号 + 相同请求内容返回首次结果；
	// 相同请求号 + 不同内容返回 PromotionRequestConflictError。
	RequestID string
}

// PromotionQuotaChange 描述一次晋级前后目标命名空间的配额变化。
type PromotionQuotaChange struct {
	Namespace        string `json:"namespace"`
	MaxBytes         int64  `json:"max_bytes"`
	UsedBefore       int64  `json:"used_before"`
	UsedAfter        int64  `json:"used_after"`
	FreedBytes       int64  `json:"freed_bytes"` // 本次晋级配套淘汰实际释放的字节
	EntryCountBefore int    `json:"entry_count_before"`
	EntryCountAfter  int    `json:"entry_count_after"`
}

// PromotionEnd 是晋级一端（来源/目标）的条目关系快照。
type PromotionEnd struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Exists    bool   `json:"exists"`
	Version   uint64 `json:"version,omitempty"`
	Digest    Digest `json:"digest,omitempty"`
	Size      int64  `json:"size,omitempty"`
}

// PromotionChunkSource 描述晋级依赖的单个内容块当前由哪些引用持有（内容来源查询）。
type PromotionChunkSource struct {
	Index  int    `json:"index"`
	Size   int64  `json:"size"`
	Digest Digest `json:"digest"`
	// ReferencedByPromotion 该块是否仍被本晋级的冻结快照引用（prepared 期间恒为 true）。
	ReferencedByPromotion bool            `json:"referenced_by_promotion"`
	References            []BlobReference `json:"references"`
}

// PromotionDetail 是晋级的完整查询结果：晋级结果、两端条目关系、配额变化与内容来源。
type PromotionDetail struct {
	Promotion Promotion              `json:"promotion"`
	Source    PromotionEnd           `json:"source"`
	Target    PromotionEnd           `json:"target"`
	Quota     PromotionQuotaChange   `json:"quota"`
	Chunks    []PromotionChunkSource `json:"chunks"`
}

// PreparePromotion 建立一次跨命名空间晋级（两阶段的第一阶段）：
// 校验来源存在、摘要匹配且未在上传后，冻结来源条目的版本/摘要/内容引用，
// 按目标配额计算淘汰候选（候选不得是有效固定项、目标键本身或与晋级内容共享的块）。
// 建立后晋级处于 prepared，其来源块快照立即成为 GC 的存活引用。
func (c *Cache) PreparePromotion(opts PromoteOptions) (*Promotion, error) {
	opts.SourceNamespace = normNamespace(opts.SourceNamespace)
	opts.TargetNamespace = normNamespace(opts.TargetNamespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	p, _, err := c.preparePromotionLocked(opts, c.clock.Now())
	return p, err
}

// Promote 是"建立 + 提交"一次完成的便捷入口（在同一临界区内完成，
// 因此不存在建立到提交之间被其他操作穿插的窗口）。
//
// 若请求号命中一个已存在的晋级，只返回首次结果：命中 prepared（例如此前通过显式两阶段
// 建立）时不会替调用方自动提交，保持"重放无副作用"。
func (c *Cache) Promote(opts PromoteOptions) (*Promotion, error) {
	opts.SourceNamespace = normNamespace(opts.SourceNamespace)
	opts.TargetNamespace = normNamespace(opts.TargetNamespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	p, replayed, err := c.preparePromotionLocked(opts, now)
	if err != nil {
		return nil, err
	}
	if replayed {
		return p, nil
	}
	return c.commitPromotionLocked(p, now)
}

func (c *Cache) preparePromotionLocked(opts PromoteOptions, now time.Time) (p *Promotion, replayed bool, err error) {
	if opts.SourceKey == "" || opts.TargetKey == "" {
		return nil, false, errors.New("buildcache: source and target cache keys are required")
	}
	if !opts.ExpectedDigest.Valid() {
		return nil, false, errors.New("buildcache: valid source digest is required")
	}
	if opts.Mode == "" {
		opts.Mode = PromotionModeCopy
	}
	if opts.Mode != PromotionModeCopy && opts.Mode != PromotionModeReplace {
		return nil, false, fmt.Errorf("buildcache: invalid promotion mode %q", opts.Mode)
	}
	if opts.SourceNamespace == opts.TargetNamespace && opts.SourceKey == opts.TargetKey {
		return nil, false, &PromotionConflictError{
			Namespace: opts.SourceNamespace, Key: opts.SourceKey,
			Reason: PromotionReasonSameNamespace, Detail: "source and target are identical",
		}
	}

	// 请求号幂等：同号同内容返回首次结果（含失败结果），同号异内容冲突。
	fp := promotionFingerprint(opts)
	if opts.RequestID != "" {
		if existing, ierr := c.lookupPromotionRequestLocked(opts.RequestID, fp); ierr != nil {
			return nil, false, ierr
		} else if existing != nil {
			// prepared / committed：原样返回首次结果（prepared 可随后显式提交，
			// 便捷入口也不替调用方自动提交）；failed：明确返回首次失败。
			if existing.Status == PromotionFailed {
				return nil, true, staleErrorFromPromotion(*existing)
			}
			return existing, true, nil
		}
	}

	// 两端命名空间都必须已注册。
	if _, err := c.requireNamespaceLocked(opts.SourceNamespace); err != nil {
		return nil, false, err
	}
	targetNS, err := c.requireNamespaceLocked(opts.TargetNamespace)
	if err != nil {
		return nil, false, err
	}

	// 来源条目正在上传（存在指向该键的未完成、未过期会话）时不得晋级——
	// 即使该键尚无已发布条目也算"正在上传"。晋级只能作用于已稳定发布的内容。
	if uploading, uerr := c.keyUploadingLocked(opts.SourceNamespace, opts.SourceKey, now); uerr != nil {
		return nil, false, uerr
	} else if uploading {
		return nil, false, &PromotionConflictError{
			Namespace: opts.SourceNamespace, Key: opts.SourceKey,
			Reason: PromotionReasonUploading, Detail: "an open upload session exists for the source key",
		}
	}

	// 来源条目必须存在且摘要匹配。
	source, err := c.store.GetEntry(opts.SourceNamespace, opts.SourceKey)
	if errors.Is(err, ErrEntryNotFound) {
		return nil, false, fmt.Errorf("%w: %s/%s", ErrEntryNotFound, opts.SourceNamespace, opts.SourceKey)
	}
	if err != nil {
		return nil, false, err
	}
	if source.Digest != opts.ExpectedDigest {
		return nil, false, &PromotionDigestConflictError{
			Namespace: opts.SourceNamespace, Key: opts.SourceKey,
			ExpectedDigest: opts.ExpectedDigest, CurrentDigest: source.Digest,
		}
	}

	// 目标键状态与模式校验。
	var baseVersion uint64
	var oldSize int64
	var existingDigest Digest
	target, err := c.store.GetEntry(opts.TargetNamespace, opts.TargetKey)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		// copy 与 replace（目标缺省视为新建）都允许目标不存在。
		baseVersion, oldSize = 0, 0
	case err != nil:
		return nil, false, err
	default:
		if opts.Mode == PromotionModeCopy {
			return nil, false, &PromotionConflictError{
				Namespace: opts.TargetNamespace, Key: opts.TargetKey,
				Reason: PromotionReasonTargetExists,
				Detail: fmt.Sprintf("target already published at v%d", target.Version),
			}
		}
		if target.Digest == source.Digest {
			return nil, false, &PromotionConflictError{
				Namespace: opts.TargetNamespace, Key: opts.TargetKey,
				Reason: PromotionReasonSameDigest,
				Detail: "target already holds an entry with the same digest",
			}
		}
		baseVersion = target.Version
		oldSize = target.TotalSize
		existingDigest = target.Digest
	}

	used, err := c.namespaceUsedLocked(opts.TargetNamespace)
	if err != nil {
		return nil, false, err
	}

	promo := Promotion{
		ID:                   c.newPromotionID(),
		RequestID:            opts.RequestID,
		SourceNamespace:      opts.SourceNamespace,
		SourceKey:            opts.SourceKey,
		SourceVersion:        source.Version,
		SourceDigest:         source.Digest,
		SourceSize:           source.TotalSize,
		SourceChunks:         append([]ChunkRef(nil), source.Chunks...),
		TargetNamespace:      opts.TargetNamespace,
		TargetKey:            opts.TargetKey,
		Mode:                 opts.Mode,
		TargetBaseVersion:    baseVersion,
		TargetExistingDigest: existingDigest,
		TargetExistingSize:   oldSize,
		Quota:                PromotionQuota{MaxBytes: targetNS.MaxBytes, UpdatedAt: targetNS.UpdatedAt},
		UsedBefore:           used,
		Status:               PromotionPrepared,
		CreatedAt:            now,
	}
	p = &promo

	// 配额：投影用量 = 目标已用 - 旧目标条目体量（replace）+ 来源条目体量。
	// 不足则建立（仅建立，不提交）未固定 LRU 淘汰决策。
	projected := used - oldSize + source.TotalSize
	if targetNS.MaxBytes > 0 && projected > targetNS.MaxBytes {
		need := projected - targetNS.MaxBytes
		// 晋级依赖的内容块：淘汰候选不得删除引用这些块的目标条目，
		// 也不得淘汰目标键本身（它由本次提交原子覆盖）。
		protected := make(map[string]bool, len(source.Chunks))
		for _, ref := range source.Chunks {
			protected[ref.Digest.String()] = true
		}
		decision, err := c.planPromotionEvictionLocked(targetNS, need, opts.TargetKey, protected, now)
		if err != nil {
			return nil, false, err
		}
		// 建立阶段即判定：即使淘汰全部可选候选也腾不出空间时，明确失败且不删除任何东西
		// （决策保持 proposed，绝不提交）。这与发布"不足则拒绝"一致，但晋级在提交前不动数据。
		var freeable int64
		for _, cand := range decision.Candidates {
			freeable += cand.Size
		}
		if freeable < need {
			return nil, false, c.quotaErrorLocked(targetNS, used, need, freeable)
		}
		p.Eviction = decision
	}

	if err := c.store.SavePromotion(*p); err != nil {
		return nil, false, err
	}
	// 请求号在建立时即占用：同号重复建立（哪怕首次仍 prepared）返回同一晋级，
	// 同号异内容立即冲突；提交阶段的重记只是幂等覆盖为相同内容。
	if err := c.recordPromotionRequestLocked(p, now); err != nil {
		return nil, false, err
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPromotion, Key: opts.TargetNamespace,
		Detail: fmt.Sprintf("promotion=%s phase=prepared src=%s/%s tgt=%s/%s mode=%s request=%s",
			p.ID, opts.SourceNamespace, opts.SourceKey, opts.TargetNamespace, opts.TargetKey,
			opts.Mode, opts.RequestID),
	}); err != nil {
		return nil, false, err
	}
	return p, false, nil
}

// keyUploadingLocked 报告 (namespace,key) 当前是否存在未完成且未过期的上传会话。
func (c *Cache) keyUploadingLocked(namespace, key string, now time.Time) (bool, error) {
	sessions, err := c.store.ListSessions()
	if err != nil {
		return false, err
	}
	for _, s := range sessions {
		if normNamespace(s.Namespace) == namespace && s.Key == key &&
			s.Status == SessionOpen && !c.expired(s, now) {
			return true, nil
		}
	}
	return false, nil
}

// planPromotionEvictionLocked 与 planEvictionLocked 相同，但额外排除引用晋级内容块的候选。
func (c *Cache) planPromotionEvictionLocked(ns Namespace, needBytes int64, excludeKey string,
	protectedChunks map[string]bool, now time.Time) (*EvictionDecision, error) {
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
		if e.Namespace != ns.Name || e.Key == excludeKey {
			continue
		}
		if _, isPinned := pinned[e.Key]; isPinned {
			continue // 有效固定项永不参与淘汰
		}
		if entrySharesChunks(e, protectedChunks) {
			continue // 本次晋级依赖的内容不得淘汰
		}
		cands = append(cands, e)
	}
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
		ID: c.newEvictionID(), Namespace: ns.Name, Reason: "quota_on_promotion",
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
			Version: e.Version, Size: e.TotalSize, LastAccessedAt: e.LastAccessedAt,
		})
		accumulated += e.TotalSize
	}
	if err := c.store.SaveEvictionDecision(decision); err != nil {
		return nil, err
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCEviction, Key: ns.Name,
		Detail: fmt.Sprintf("decision=%s phase=proposed reason=quota_on_promotion candidates=%d need=%d",
			decision.ID, len(decision.Candidates), needBytes),
	}); err != nil {
		return nil, err
	}
	return &decision, nil
}

func entrySharesChunks(e Entry, protected map[string]bool) bool {
	if len(protected) == 0 {
		return false
	}
	for _, ref := range e.Chunks {
		if protected[ref.Digest.String()] {
			return true
		}
	}
	return false
}

// CommitPromotion 提交一次已建立的晋级（两阶段的第二阶段）。
//
// 提交在同一把服务锁内复核并原子落盘：
//  1. 来源条目版本/摘要必须与冻结快照一致且仍存在（否则 source_changed/source_missing）；
//  2. 目标配额版本（max_bytes/updated_at）必须未变（否则 quota_version_changed）；
//  3. 目标键必须仍处于冻结的基线版本（否则 target_conflict）；
//  4. 淘汰候选在决策后若被访问、固定或重新发布，旧淘汰决定失效（eviction_stale）；
//  5. 写入目标条目（带 CAS）→ 提交淘汰 → 记录结果，全程由重做日志保护。
//
// 提交是幂等的：已 committed 的晋级重复提交返回首次结果。
func (c *Cache) CommitPromotion(promotionID string) (*Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.store.GetPromotion(promotionID)
	if err != nil {
		return nil, err
	}
	return c.commitPromotionLocked(&p, c.clock.Now())
}

func (c *Cache) commitPromotionLocked(p *Promotion, now time.Time) (*Promotion, error) {
	// 已终结：committed 原样返回（幂等）；failed 返回首次失败结果。
	if p.Status == PromotionCommitted {
		return p, nil
	}
	if p.Status == PromotionFailed {
		return p, staleErrorFromPromotion(*p)
	}

	// 恢复路径：若已存在重做日志，说明此前提交已开始（淘汰可能已删除候选）。
	// 此时不能再用"决策后状态"做旧决定失效判定（候选被本提交自己删除/访问属正常），
	// 直接进入 apply，由它结合权威存储状态幂等续做。
	if _, terr := c.store.GetPromotionTxn(p.ID); terr == nil {
		if err := c.applyPromotionLocked(p, now); err != nil {
			return nil, err
		}
		return p, nil
	} else if !errors.Is(terr, ErrNotFound) {
		return nil, terr
	}

	// ---- 提交前复核（仅判定，不修改状态）----
	if reason, detail, err := c.validatePromotionLocked(p, now); err != nil {
		return nil, err
	} else if reason != "" {
		return c.failPromotionLocked(p, reason, detail, now)
	}

	// 配额淘汰候选复核：任何候选在决策后被访问 / 固定 / 重新发布 / 删除，
	// 旧淘汰决定即失效——旧请求明确失败，绝不带着新状态勉强提交。
	if p.Eviction != nil {
		if stale, detail, err := c.evictionStillValidLocked(p.Eviction, now); err != nil {
			return nil, err
		} else if !stale {
			return c.failPromotionLocked(p, PromotionFailEvictionStale, detail, now)
		}
	}

	// ---- 原子提交（重做日志驱动）----
	if err := c.applyPromotionLocked(p, now); err != nil {
		return nil, err
	}
	return p, nil
}

// validatePromotionLocked 复核来源/配额/目标三类乐观条件；返回空 reason 表示全部通过。
//
// 来源条目在建立后若被删除，提交并不失败：晋级依赖的内容块由 prepared 冻结快照保护
// （GC 第四类引用），提交直接用冻结的版本/摘要/块引用完成。只有来源"被覆盖发布为
// 不同版本或摘要"才属于来源摘要变化，必须明确失败。
func (c *Cache) validatePromotionLocked(p *Promotion, now time.Time) (reason, detail string, err error) {
	source, gerr := c.store.GetEntry(p.SourceNamespace, p.SourceKey)
	switch {
	case errors.Is(gerr, ErrEntryNotFound):
		// 来源条目已删除：以冻结快照为准继续（块仍被 prepared 引用保护）。
	case gerr != nil:
		return "", "", gerr
	default:
		if source.Version != p.SourceVersion || source.Digest != p.SourceDigest {
			return PromotionFailSourceChanged, fmt.Sprintf(
				"source now at v%d %s, frozen v%d %s",
				source.Version, source.Digest, p.SourceVersion, p.SourceDigest), nil
		}
	}

	targetNS, gerr := c.requireNamespaceLocked(p.TargetNamespace)
	if gerr != nil {
		return "", "", gerr
	}
	if targetNS.MaxBytes != p.Quota.MaxBytes || !targetNS.UpdatedAt.Equal(p.Quota.UpdatedAt) {
		return PromotionFailQuotaVersion, fmt.Sprintf(
			"quota now max=%d updated=%s, frozen max=%d updated=%s",
			targetNS.MaxBytes, targetNS.UpdatedAt.Format(time.RFC3339Nano),
			p.Quota.MaxBytes, p.Quota.UpdatedAt.Format(time.RFC3339Nano)), nil
	}

	target, gerr := c.store.GetEntry(p.TargetNamespace, p.TargetKey)
	switch {
	case errors.Is(gerr, ErrEntryNotFound):
		if p.TargetBaseVersion != 0 {
			return PromotionFailTargetConflict, "target entry disappeared after prepare", nil
		}
	case gerr != nil:
		return "", "", gerr
	default:
		// 恢复路径：目标条目可能已被本晋级在崩溃前写入（带本晋级来源标记），
		// 此时版本 = 基线+1 属于预期，校验视为通过，apply 阶段会跳过重复写入。
		if entryIsOurPromotion(target, p) {
			return "", "", nil
		}
		if target.Version != p.TargetBaseVersion {
			return PromotionFailTargetConflict, fmt.Sprintf(
				"target now at v%d, frozen v%d", target.Version, p.TargetBaseVersion), nil
		}
		// 基线版本相同但摘要被原地改动也视为冲突（正常发布只会 +1 版本，此为防御性校验）。
		if target.Digest != p.TargetExistingDigest {
			return PromotionFailTargetConflict, fmt.Sprintf(
				"target digest changed at v%d: now %s, frozen %s",
				target.Version, target.Digest, p.TargetExistingDigest), nil
		}
	}
	return "", "", nil
}

// entryIsOurPromotion 报告目标条目是否已由本晋级写入（用于崩溃恢复识别半提交）。
func entryIsOurPromotion(e Entry, p *Promotion) bool {
	o := e.PromotedFrom
	return o != nil &&
		o.Namespace == p.SourceNamespace && o.Key == p.SourceKey &&
		o.Version == p.SourceVersion && o.Digest == p.SourceDigest &&
		o.RequestID == p.RequestID && e.Digest == p.SourceDigest
}

// evictionStillValidLocked 复核晋级配套淘汰决策是否仍可原样提交。
func (c *Cache) evictionStillValidLocked(decision *EvictionDecision, now time.Time) (bool, string, error) {
	pins, err := c.store.ListPins()
	if err != nil {
		return false, "", err
	}
	activePin := make(map[string]Pin)
	for _, p := range pins {
		if p.Namespace == decision.Namespace && p.Active(now) {
			activePin[p.Key] = p
		}
	}
	for _, cand := range decision.Candidates {
		entry, gerr := c.store.GetEntry(decision.Namespace, cand.Key)
		switch {
		case errors.Is(gerr, ErrEntryNotFound):
			return false, "candidate " + cand.Key + " went missing", nil
		case gerr != nil:
			return false, "", gerr
		}
		if entry.Version != cand.Version || entry.Digest != cand.Digest {
			return false, "candidate " + cand.Key + " was republished", nil
		}
		if entry.LastAccessedAt.After(cand.LastAccessedAt) {
			return false, "candidate " + cand.Key + " was accessed", nil
		}
		if _, ok := activePin[cand.Key]; ok {
			return false, "candidate " + cand.Key + " was pinned", nil
		}
	}
	return true, "", nil
}

// applyPromotionLocked 按重做日志阶段把晋级落盘。
//
// 阶段：committing（提交配套淘汰，先腾出空间）→ eviction_committed（写入目标条目，CAS）
// → entry_written（晋级结果与请求记录落盘并删除日志）。
//
// 重做日志只在提交真正开始（全部乐观校验通过、首个变更之前）创建，因此 prepared 但从未
// 提交的晋级重启后不会被自动提交；而日志存在意味着提交在进行中崩溃，恢复时重入本函数，
// 结合权威存储状态（淘汰决策是否已 committed、目标条目是否已带晋级标记）幂等跳过已完成
// 步骤。任一步持久化失败都沿同一序列重试，已提交端不回滚、未提交端不留痕。
func (c *Cache) applyPromotionLocked(p *Promotion, now time.Time) error {
	txn, err := c.store.GetPromotionTxn(p.ID)
	switch {
	case errors.Is(err, ErrNotFound):
		// 全新提交：在产生任何变更前先写下重做日志起点。
		txn = PromotionTxn{PromotionID: p.ID, Phase: promoTxnCommitting, UpdatedAt: now}
		if err := c.store.SavePromotionTxn(txn); err != nil {
			return err
		}
	case err != nil:
		return err
	}

	// 无论处于哪个阶段，都先以存储中的权威淘汰决策刷新内存副本（恢复时晋级记录里的
	// 副本可能还是 proposed）。只有在 committing 阶段才真正提交淘汰，后续阶段只取回。
	if p.Eviction != nil {
		fresh, gerr := c.store.GetEvictionDecision(p.Eviction.ID)
		if gerr != nil {
			return gerr
		}
		p.Eviction = &fresh
	}

	// 阶段 1：提交配额淘汰。已 committed 则直接跳过，绝不重复删除候选。
	if txn.Phase == promoTxnCommitting {
		if p.Eviction != nil && p.Eviction.Status != EvictionCommitted {
			if err := c.commitEvictionLocked(p.Eviction, now); err != nil {
				return err
			}
		}
		if p.Eviction != nil {
			// 按"当前实际用量 + 来源体量 - 将被覆盖的旧目标体量"复核配额。
			// 用实际用量而非决策的 FreedBytes：恢复重入时部分候选可能已被上一次崩溃前的
			// 提交删除（重放会记为 skipped_missing），FreedBytes 不再等于真实释放量；
			// 实际用量才是权威。不足则明确失败。
			usedNow, gerr := c.namespaceUsedLocked(p.TargetNamespace)
			if gerr != nil {
				return gerr
			}
			if usedNow-p.TargetExistingSize+p.SourceSize > p.Quota.MaxBytes {
				_, ferr := c.failPromotionLocked(p, PromotionFailQuota, fmt.Sprintf(
					"after eviction used=%d old_target=%d source=%d exceeds max=%d",
					usedNow, p.TargetExistingSize, p.SourceSize, p.Quota.MaxBytes), now)
				return ferr
			}
		}
		txn.Phase = promoTxnEviction
		txn.UpdatedAt = now
		if err := c.store.SavePromotionTxn(txn); err != nil {
			return err
		}
	}

	// 阶段 2：写入目标条目（CAS 条件 = 冻结基线版本）。
	if txn.Phase == promoTxnEviction {
		written, err := c.promotionEntryPresentLocked(p)
		if err != nil {
			return err
		}
		if !written {
			entry := Entry{
				Namespace: p.TargetNamespace, Key: p.TargetKey,
				Version: p.TargetBaseVersion + 1, Digest: p.SourceDigest,
				TotalSize: p.SourceSize, Chunks: append([]ChunkRef(nil), p.SourceChunks...),
				PublishedAt: now, LastAccessedAt: now,
				PromotedFrom: &PromotionOrigin{
					RequestID: p.RequestID,
					Namespace: p.SourceNamespace, Key: p.SourceKey,
					Version: p.SourceVersion, Digest: p.SourceDigest,
				},
			}
			// CAS：新建（基线 0）要求目标键不存在（-1）；替换要求当前版本恰为冻结基线。
			cas := int64(p.TargetBaseVersion)
			if p.TargetBaseVersion == 0 {
				cas = -1
			}
			if err := c.store.PutEntry(entry, cas); err != nil {
				if errors.Is(err, ErrCASFailed) {
					// 并发越过本进程锁改了目标键：按目标冲突明确失败，不覆盖新状态。
					_, detail, _ := c.validatePromotionLocked(p, now)
					_, ferr := c.failPromotionLocked(p, PromotionFailTargetConflict, detail, now)
					return ferr
				}
				return err
			}
		}
		txn.Phase = promoTxnEntry
		txn.UpdatedAt = now
		if err := c.store.SavePromotionTxn(txn); err != nil {
			return err
		}
	}

	// 阶段 3：晋级结果与请求记录落盘。
	usedAfter, err := c.namespaceUsedLocked(p.TargetNamespace)
	if err != nil {
		return err
	}
	p.Status = PromotionCommitted
	p.FailReason = ""
	p.TargetVersion = p.TargetBaseVersion + 1
	p.CommittedAt = now
	p.UsedAfter = usedAfter
	if err := c.store.SavePromotion(*p); err != nil {
		return err
	}
	if err := c.recordPromotionRequestLocked(p, now); err != nil {
		return err
	}
	if err := c.store.DeletePromotionTxn(p.ID); err != nil {
		return err
	}
	var freed int64
	if p.Eviction != nil {
		freed = p.Eviction.FreedBytes
	}
	return c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPromotion, Key: p.TargetNamespace,
		Detail: fmt.Sprintf("promotion=%s phase=committed src=%s/%s tgt=%s/%s target_version=%d freed=%d used_before=%d used_after=%d",
			p.ID, p.SourceNamespace, p.SourceKey, p.TargetNamespace, p.TargetKey,
			p.TargetVersion, freed, p.UsedBefore, usedAfter),
	})
}

// promotionEntryPresentLocked 报告目标条目是否已由本晋级写入（崩溃恢复时识别半提交）。
func (c *Cache) promotionEntryPresentLocked(p *Promotion) (bool, error) {
	entry, err := c.store.GetEntry(p.TargetNamespace, p.TargetKey)
	if errors.Is(err, ErrEntryNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return entry.Digest == p.SourceDigest && entry.PromotedFrom != nil &&
		entry.PromotedFrom.Namespace == p.SourceNamespace &&
		entry.PromotedFrom.Key == p.SourceKey &&
		entry.PromotedFrom.RequestID == p.RequestID, nil
}

// failPromotionLocked 把晋级置为 failed 终态并记录请求号（失败也是首次结果，可重放），
// 清理可能存在的重做日志；两端条目不做任何修改。
func (c *Cache) failPromotionLocked(p *Promotion, reason, detail string, now time.Time) (*Promotion, error) {
	p.Status = PromotionFailed
	p.FailReason = reason
	p.CommittedAt = now
	if err := c.store.SavePromotion(*p); err != nil {
		return nil, err
	}
	if err := c.recordPromotionRequestLocked(p, now); err != nil {
		return nil, err
	}
	_ = c.store.DeletePromotionTxn(p.ID) // prepared 阶段失败：没有任何端被修改
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPromotion, Key: p.TargetNamespace,
		Detail: fmt.Sprintf("promotion=%s phase=failed reason=%s detail=%s", p.ID, reason, detail),
	}); err != nil {
		return nil, err
	}
	return p, staleErrorFromPromotion(*p)
}

// ---- 请求号幂等 ----

func promotionFingerprint(opts PromoteOptions) string {
	return "promote|" + opts.SourceNamespace + "|" + opts.SourceKey + "|" +
		opts.ExpectedDigest.String() + "|" + opts.TargetNamespace + "|" + opts.TargetKey + "|" + opts.Mode
}

// lookupPromotionRequestLocked 同号同指纹返回首次结果；同号异指纹返回冲突。
func (c *Cache) lookupPromotionRequestLocked(requestID, fingerprint string) (*Promotion, error) {
	if requestID == "" {
		return nil, nil
	}
	rec, err := c.store.GetPromotionRequest(requestID)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if rec.Fingerprint != fingerprint {
		return nil, &PromotionRequestConflictError{RequestID: requestID}
	}
	p, err := c.store.GetPromotion(rec.PromotionID)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (c *Cache) recordPromotionRequestLocked(p *Promotion, now time.Time) error {
	if p.RequestID == "" {
		return nil
	}
	rec := PromotionRequest{
		RequestID: p.RequestID,
		Fingerprint: promotionFingerprint(PromoteOptions{
			SourceNamespace: p.SourceNamespace, SourceKey: p.SourceKey,
			ExpectedDigest:  p.SourceDigest,
			TargetNamespace: p.TargetNamespace, TargetKey: p.TargetKey, Mode: p.Mode,
		}),
		PromotionID: p.ID, CreatedAt: now,
	}
	if err := c.store.SavePromotionRequest(rec); err != nil {
		return err
	}
	return nil
}

func staleErrorFromPromotion(p Promotion) error {
	if p.Status != PromotionFailed {
		return nil
	}
	return &PromotionStaleError{PromotionID: p.ID, Reason: p.FailReason}
}

// ---- 查询 ----

// GetPromotion 查询一次晋级决策与结果（prepared / committed / failed）。
func (c *Cache) GetPromotion(id string) (Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.store.GetPromotion(id)
	if errors.Is(err, ErrPromotionNotFound) {
		return Promotion{}, fmt.Errorf("%w: promotion %s", ErrPromotionNotFound, id)
	}
	return p, err
}

// PromotionFilters 限定晋级列表范围；零值表示不限。
type PromotionFilters struct {
	// Namespace 非空时，返回来源或目标命名空间等于该值的晋级。
	Namespace string
	// RequestID 非空时，仅返回该外部请求号的晋级。
	RequestID string
	// Status 非空时，仅返回该状态（prepared/committed/failed）的晋级。
	Status string
}

// ListPromotions 按过滤条件列出晋级，按 ID 升序。
func (c *Cache) ListPromotions(filters PromotionFilters) ([]Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ps, err := c.store.ListPromotions()
	if err != nil {
		return nil, err
	}
	out := make([]Promotion, 0, len(ps))
	for _, p := range ps {
		if filters.Namespace != "" &&
			p.SourceNamespace != filters.Namespace && p.TargetNamespace != filters.Namespace {
			continue
		}
		if filters.RequestID != "" && p.RequestID != filters.RequestID {
			continue
		}
		if filters.Status != "" && p.Status != filters.Status {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// GetPromotionDetail 返回晋级的完整视图：结果、两端条目关系、配额变化与逐块内容来源。
func (c *Cache) GetPromotionDetail(id string) (*PromotionDetail, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	p, err := c.store.GetPromotion(id)
	if errors.Is(err, ErrPromotionNotFound) {
		return nil, fmt.Errorf("%w: promotion %s", ErrPromotionNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	detail := &PromotionDetail{Promotion: p}

	detail.Source = c.promotionEndLocked(p.SourceNamespace, p.SourceKey)
	detail.Target = c.promotionEndLocked(p.TargetNamespace, p.TargetKey)

	var freed int64
	if p.Eviction != nil {
		freed = p.Eviction.FreedBytes
	}
	detail.Quota = PromotionQuotaChange{
		Namespace: p.TargetNamespace, MaxBytes: p.Quota.MaxBytes,
		UsedBefore: p.UsedBefore, UsedAfter: p.UsedAfter, FreedBytes: freed,
	}
	if p.Status == PromotionCommitted {
		entries, err := c.store.ListEntries()
		if err != nil {
			return nil, err
		}
		var before, after int
		for _, e := range entries {
			if e.Namespace != p.TargetNamespace {
				continue
			}
			after++
		}
		// 提交前条目数：copy 成功后 after = before+1-淘汰数；replace 后 after = before-淘汰数。
		before = after - len(committedEvicted(p))
		if p.TargetBaseVersion == 0 {
			before-- // copy 新建了一个条目
		}
		detail.Quota.EntryCountBefore = before
		detail.Quota.EntryCountAfter = after
	}

	// 逐块内容来源：块的全部当前引用（entry/session/pin/promotion）。
	refsByDigest := make(map[string][]BlobReference)
	for _, ref := range p.SourceChunks {
		refs, err := c.blobReferencesLocked(ref.Digest, now)
		if err != nil {
			return nil, err
		}
		refsByDigest[ref.Digest.String()] = refs
	}
	chunks := make([]ChunkRef, len(p.SourceChunks))
	copy(chunks, p.SourceChunks)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Index < chunks[j].Index })
	for _, ref := range chunks {
		detail.Chunks = append(detail.Chunks, PromotionChunkSource{
			Index: ref.Index, Size: ref.Size, Digest: ref.Digest,
			ReferencedByPromotion: p.Status == PromotionPrepared,
			References:            refsByDigest[ref.Digest.String()],
		})
	}
	return detail, nil
}

func committedEvicted(p Promotion) []EvictionCandidate {
	if p.Eviction == nil {
		return nil
	}
	var out []EvictionCandidate
	for _, cand := range p.Eviction.Candidates {
		if cand.Outcome == EvictDeleted {
			out = append(out, cand)
		}
	}
	return out
}

func (c *Cache) promotionEndLocked(namespace, key string) PromotionEnd {
	end := PromotionEnd{Namespace: namespace, Key: key}
	e, err := c.store.GetEntry(namespace, key)
	if errors.Is(err, ErrEntryNotFound) {
		return end
	}
	if err != nil {
		return end
	}
	end.Exists = true
	end.Version = e.Version
	end.Digest = e.Digest
	end.Size = e.TotalSize
	return end
}

// resumePromotionTxns 在启动时重做未完成的晋级提交（崩溃恢复）。
func (c *Cache) resumePromotionTxns() error {
	txns, err := c.store.ListPromotionTxns()
	if err != nil {
		return fmt.Errorf("buildcache: restore promotion txns: %w", err)
	}
	if len(txns) == 0 {
		return nil
	}
	now := c.clock.Now()
	for _, txn := range txns {
		p, gerr := c.store.GetPromotion(txn.PromotionID)
		if errors.Is(gerr, ErrPromotionNotFound) {
			// 决策记录已失而事务残留：清理孤儿事务。
			if err := c.store.DeletePromotionTxn(txn.PromotionID); err != nil {
				return err
			}
			continue
		}
		if gerr != nil {
			return gerr
		}
		c.mu.Lock()
		if p.Status == PromotionCommitted {
			_ = c.store.DeletePromotionTxn(p.ID)
			c.mu.Unlock()
			continue
		}
		// 日志存在意味着提交已开始（配套淘汰可能已删除候选），原子单元必须完成：
		// 直接结合权威存储状态幂等续做，不再重做乐观复核（旧决定此刻不可回退，
		// 且来源块在晋级仍 prepared 期间始终受 GC 保护）。apply 会识别已写入的目标条目。
		err := c.applyPromotionLocked(&p, now)
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) newPromotionID() string {
	c.promoSeq++
	return fmt.Sprintf("promo-%012d", c.promoSeq)
}
