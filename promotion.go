package buildcache

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// PromoteOptions 是跨命名空间晋级请求的参数。
type PromoteOptions struct {
	// SourceNamespace / SourceKey 来源条目身份。
	SourceNamespace string
	SourceKey       string
	// ExpectedDigest 来源条目必须匹配的当前摘要；空表示不校验（仍会冻结实际摘要）。
	ExpectedDigest Digest
	// TargetNamespace / TargetKey 目标条目身份。
	TargetNamespace string
	// TargetKey 为空时退化为与来源键同名。
	TargetKey string
	// Mode 目标模式：PromoteCopy（新键）或 PromoteReplace（同名替换）。
	Mode string
	// RequestID 外部请求号：相同请求号 + 相同内容重放返回首次结果，
	// 相同请求号 + 不同内容冲突。
	RequestID string
}

// 晋级是两阶段操作：CreatePromotion 在请求建立时冻结来源条目摘要与内容引用、
// 目标配额版本并算定淘汰候选；CommitPromotion 在提交时基于最新状态逐项复核后
// 原子落定。两阶段之间来源被覆盖、目标配额变更、目标条目被推进、淘汰候选被
// 重新访问/固定/发布，都会让旧请求在提交时明确失败而不是覆盖新状态。

// validatePromoteOptions 规范化并校验参数，返回规范化后的选项。
func validatePromoteOptions(opts PromoteOptions) (PromoteOptions, string, string, error) {
	if opts.SourceKey == "" || opts.TargetNamespace == "" || opts.RequestID == "" {
		return opts, "", "", errors.New("buildcache: source key, target namespace and request id are required")
	}
	if opts.Mode != PromoteCopy && opts.Mode != PromoteReplace {
		return opts, "", "", fmt.Errorf("buildcache: invalid promotion mode %q", opts.Mode)
	}
	srcNs := normNamespace(opts.SourceNamespace)
	dstNs := normNamespace(opts.TargetNamespace)
	dstKey := opts.TargetKey
	if dstKey == "" {
		dstKey = opts.SourceKey
	}
	opts.SourceNamespace = srcNs
	opts.TargetNamespace = dstNs
	opts.TargetKey = dstKey
	if srcNs == dstNs && opts.SourceKey == dstKey {
		return opts, srcNs, dstNs, &PromotionConflictError{
			Reason: PromotionReasonSameEntry, RequestID: opts.RequestID,
			Detail: srcNs + "/" + opts.SourceKey,
		}
	}
	return opts, srcNs, dstNs, nil
}

// CreatePromotion 建立晋级请求并冻结所有提交依据（不写入目标条目、不淘汰任何条目）。
//
// 来源条目不存在、摘要不符或正在上传（存在未过期 open 会话）时拒绝；
// copy 模式下目标键已存在也拒绝。
func (c *Cache) CreatePromotion(opts PromoteOptions) (*Promotion, error) {
	opts, _, _, err := validatePromoteOptions(opts)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createPromotionLocked(opts)
}

// createPromotionLocked 是建立请求的持锁内核。调用方持有 c.mu。
func (c *Cache) createPromotionLocked(opts PromoteOptions) (*Promotion, error) {
	srcNs := opts.SourceNamespace
	dstNs := opts.TargetNamespace
	dstKey := opts.TargetKey
	now := c.clock.Now()

	fp := promotionFingerprint(opts.Mode, srcNs, opts.SourceKey, opts.ExpectedDigest, dstNs, dstKey)
	if existing, gerr := c.store.GetPromotion(opts.RequestID); gerr == nil {
		// 同请求号：指纹相同走重放语义（提交状态由 CommitPromotion / 查询给出），
		// 指纹不同即冲突。
		if existing.Fingerprint != fp {
			return nil, &PromotionRequestConflictError{RequestID: opts.RequestID}
		}
		return &existing, nil
	} else if !errors.Is(gerr, ErrNotFound) {
		return nil, gerr
	}

	// 来源与目标命名空间都必须已注册。
	if _, err := c.requireNamespaceLocked(srcNs); err != nil {
		return nil, err
	}
	dstNamespace, err := c.requireNamespaceLocked(dstNs)
	if err != nil {
		return nil, err
	}

	// 来源条目必须存在且摘要匹配。
	src, err := c.store.GetEntry(srcNs, opts.SourceKey)
	if errors.Is(err, ErrEntryNotFound) {
		return nil, &PromotionConflictError{
			Reason: PromotionReasonMissing, RequestID: opts.RequestID,
			Detail: srcNs + "/" + opts.SourceKey,
		}
	}
	if err != nil {
		return nil, err
	}
	if opts.ExpectedDigest.Valid() && opts.ExpectedDigest != src.Digest {
		return nil, &PromotionConflictError{
			Reason: PromotionReasonDigest, RequestID: opts.RequestID,
			Detail: "want " + opts.ExpectedDigest.String() + ", current " + src.Digest.String(),
		}
	}

	// 来源键正在上传（存在未过期 open 会话）时不得晋级。
	if err := c.checkSourceNotUploadingLocked(srcNs, opts.SourceKey, now); err != nil {
		return nil, err
	}

	// 目标键状态：copy 要求不存在；replace 记录当前版本与旧条目体量快照。
	var targetVersion uint64
	var replacedOldSize int64
	target, err := c.store.GetEntry(dstNs, dstKey)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		// copy 要求目标不存在；replace 旧条目不存在时退化为新建。
	case err != nil:
		return nil, err
	default:
		if opts.Mode == PromoteCopy {
			return nil, &PromotionConflictError{
				Reason: PromotionReasonTargetExists, RequestID: opts.RequestID,
				Detail: dstNs + "/" + dstKey + " already exists at v" + itoa(int64(target.Version)),
			}
		}
		targetVersion = target.Version
		replacedOldSize = target.TotalSize
	}

	used, err := c.namespaceUsedLocked(dstNs)
	if err != nil {
		return nil, err
	}

	// 冻结来源快照（复制块引用，后续来源被覆盖/删除不影响本请求依据）。
	prom := Promotion{
		RequestID:       opts.RequestID,
		Mode:            opts.Mode,
		Source:          promotionSourceOf(src),
		TargetNamespace: dstNs,
		TargetKey:       dstKey,
		Fingerprint:     fp,
		TargetVersion:   targetVersion,
		QuotaVersion:    namespaceVersion(dstNamespace),
		MaxBytes:        dstNamespace.MaxBytes,
		UsedBefore:      used,
		SharedChunks:    len(src.Chunks),
		Status:          PromotionProposed,
		CreatedAt:       now,
	}

	// 提交前按目标配额计算淘汰候选（投影用量 = 已用 - 将被替换旧条目体量 + 来源体量）。
	exclude := map[string]bool{dstKey: true}
	if srcNs == dstNs {
		// 同命名空间内复制：来源条目是本次晋级依赖的内容，不得作为淘汰候选。
		exclude[opts.SourceKey] = true
	}
	projected := used - replacedOldSize + src.TotalSize
	if dstNamespace.MaxBytes > 0 && projected > dstNamespace.MaxBytes {
		need := projected - dstNamespace.MaxBytes
		decision, err := c.planEvictionLocked(dstNamespace, need, "quota_on_promotion", exclude, now)
		if err != nil {
			return nil, err
		}
		prom.Eviction = decision
	}

	if err := c.store.SavePromotion(prom); err != nil {
		return nil, err
	}
	return &prom, nil
}

// uint64Size 占位类型已移除。

func promotionSourceOf(e Entry) PromotionSource {
	return PromotionSource{
		Namespace: e.Namespace, Key: e.Key, Version: e.Version,
		Digest: e.Digest, TotalSize: e.TotalSize,
		Chunks: append([]ChunkRef(nil), e.Chunks...),
	}
}

func (c *Cache) checkSourceNotUploadingLocked(namespace, key string, now time.Time) error {
	sessions, err := c.store.ListSessions()
	if err != nil {
		return err
	}
	for _, s := range sessions {
		if normNamespace(s.Namespace) == namespace && s.Key == key &&
			s.Status == SessionOpen && !c.expired(s, now) {
			return &PromotionConflictError{
				Reason: PromotionReasonUploading,
				Detail: "open session " + s.ID,
			}
		}
	}
	return nil
}

func namespaceVersion(ns Namespace) int64 { return ns.UpdatedAt.UnixNano() }

func promotionFingerprint(mode, srcNs, srcKey string, srcDigest Digest, dstNs, dstKey string) string {
	return strings.Join([]string{
		mode, srcNs, srcKey, "digest=" + srcDigest.String(), dstNs, dstKey,
	}, "|")
}

// CommitPromotion 提交晋级请求：复核来源摘要、目标配额版本、目标条目版本与淘汰
// 候选均未漂移后，原子写入目标条目、落定淘汰结果并提交本记录。
//
// 任一前置状态已变化都返回 PromotionStaleError（旧请求不会覆盖新状态），
// 并把记录置为 failed 终态；同号重放始终返回同一结果。
func (c *Cache) CommitPromotion(requestID string) (*Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitPromotionLocked(requestID)
}

// commitPromotionLocked 是提交请求的持锁内核。调用方持有 c.mu。
func (c *Cache) commitPromotionLocked(requestID string) (*Promotion, error) {
	prom, err := c.store.GetPromotion(requestID)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrPromotionNotFound, requestID)
	}
	if err != nil {
		return nil, err
	}
	switch prom.Status {
	case PromotionCommitted:
		return &prom, nil // 提交幂等：返回首次结果。
	case PromotionFailed:
		return nil, staleFromPromotion(&prom)
	}
	now := c.clock.Now()

	// 1) 来源条目仍存在，且摘要/版本与冻结快照一致（来源变化 -> 旧请求失效）。
	src, gerr := c.store.GetEntry(prom.Source.Namespace, prom.Source.Key)
	switch {
	case errors.Is(gerr, ErrEntryNotFound):
		return c.failPromotion(&prom, PromotionStaleSource,
			"source entry no longer exists")
	case gerr != nil:
		return nil, gerr
	}
	if src.Digest != prom.Source.Digest {
		return c.failPromotion(&prom, PromotionStaleSource,
			"source digest changed from "+prom.Source.Digest.String()+" to "+src.Digest.String())
	}

	// 2) 目标配额版本未变化（配额被调整 -> 旧请求依据失效）。
	dstNamespace, err := c.requireNamespaceLocked(prom.TargetNamespace)
	if err != nil {
		return nil, err
	}
	if namespaceVersion(dstNamespace) != prom.QuotaVersion || dstNamespace.MaxBytes != prom.MaxBytes {
		return c.failPromotion(&prom, PromotionStaleQuota,
			"target namespace quota changed since request creation")
	}

	// 3) 目标条目版本未被推进（copy 仍须不存在）。
	var oldTarget *Entry
	target, terr := c.store.GetEntry(prom.TargetNamespace, prom.TargetKey)
	switch {
	case errors.Is(terr, ErrEntryNotFound):
		if prom.Mode == PromoteReplace && prom.TargetVersion != 0 {
			return c.failPromotion(&prom, PromotionStaleTarget, "target entry disappeared")
		}
	case terr != nil:
		return nil, terr
	default:
		if prom.Mode == PromoteCopy {
			return c.failPromotion(&prom, PromotionStaleTarget, "target key now exists")
		}
		if target.Version != prom.TargetVersion {
			return c.failPromotion(&prom, PromotionStaleTarget,
				"target version advanced to v"+itoa(int64(target.Version)))
		}
		old := target
		oldTarget = &old
	}

	// 4) 复核冻结的淘汰决定：期间被访问/固定/重新发布的候选跳过，不删除新状态。
	var victims []Entry
	var replacedOldSize int64
	if prom.Mode == PromoteReplace && prom.TargetVersion != 0 {
		replacedOldSize = target.TotalSize
	}
	if prom.Eviction != nil {
		vs, err := c.evaluateEvictionLocked(prom.Eviction, now)
		if err != nil {
			return nil, err
		}
		victims = vs
	}

	// 5) 配额终判：按复核后实际可释放量重算投影；旧淘汰决定已腾不出足够空间 -> 失败。
	currentUsed, err := c.namespaceUsedLocked(prom.TargetNamespace)
	if err != nil {
		return nil, err
	}
	var freed int64
	if prom.Eviction != nil {
		freed = prom.Eviction.FreedBytes
	}
	projected := currentUsed - replacedOldSize - freed + prom.Source.TotalSize
	if dstNamespace.MaxBytes > 0 && projected > dstNamespace.MaxBytes {
		detail := "stale eviction decision frees insufficient bytes: projected " +
			itoa(projected) + " > max " + itoa(dstNamespace.MaxBytes)
		if prom.Eviction == nil {
			detail = "target namespace usage grew since request creation: projected " +
				itoa(projected) + " > max " + itoa(dstNamespace.MaxBytes)
		}
		return c.failPromotion(&prom, PromotionStaleEviction, detail)
	}

	// 6) 原子落定：先建立目标条目引用（CAS 条件兜底并发），再删除淘汰条目，
	//    最后提交淘汰决定与晋级记录。任一步持久化失败都完整回滚，保持两端原状。
	newEntry := Entry{
		Namespace:      prom.TargetNamespace,
		Key:            prom.TargetKey,
		Version:        prom.TargetVersion + 1, // copy / 新建时 TargetVersion=0 -> v1
		Digest:         prom.Source.Digest,
		TotalSize:      prom.Source.TotalSize,
		Chunks:         append([]ChunkRef(nil), prom.Source.Chunks...),
		PublishedAt:    now,
		LastAccessedAt: now,
	}
	wantVersion := int64(-1) // 仅允许新建
	if prom.Mode == PromoteReplace && prom.TargetVersion != 0 {
		wantVersion = int64(prom.TargetVersion)
	}
	if err := c.store.PutEntry(newEntry, wantVersion); err != nil {
		if errors.Is(err, ErrCASFailed) {
			return c.failPromotion(&prom, PromotionStaleTarget, "target entry changed at commit")
		}
		return nil, err
	}

	// deletedVictims 记录真正删除成功的候选：只有这些条目需要在回滚时恢复。
	deletedVictims := []Entry{}
	rollback := func(stage string) error {
		// 撤销刚建立的目标条目；replace 时恢复被覆盖的旧版本，copy 时仅删除。
		if oldTarget != nil {
			if rerr := c.store.PutEntry(*oldTarget, int64(newEntry.Version)); rerr != nil {
				return fmt.Errorf("promotion rollback after %s failed: %w", stage, rerr)
			}
		} else if rerr := c.store.DeleteEntry(newEntry.Namespace, newEntry.Key); rerr != nil {
			return fmt.Errorf("promotion rollback after %s failed: %w", stage, rerr)
		}
		// 恢复已删除的淘汰条目（此刻这些键已不存在，-1 新建条件成立）。
		for _, v := range deletedVictims {
			if rerr := c.store.PutEntry(v, -1); rerr != nil {
				return fmt.Errorf("promotion rollback after %s failed: %w", stage, rerr)
			}
		}
		// 淘汰决定与晋级记录退回 proposed：本次提交未发生，请求可稍后重试。
		if prom.Eviction != nil {
			prom.Eviction.Status = EvictionProposed
			prom.Eviction.CommittedAt = time.Time{}
			for i := range prom.Eviction.Candidates {
				prom.Eviction.Candidates[i].Outcome = EvictPending
			}
			prom.Eviction.FreedBytes = 0
			if rerr := c.store.SaveEvictionDecision(*prom.Eviction); rerr != nil {
				return fmt.Errorf("promotion rollback after %s failed: %w", stage, rerr)
			}
		}
		prom.Status = PromotionProposed
		prom.CommittedAt = time.Time{}
		prom.UsedAfter = 0
		prom.ReplacedVersion = 0
		prom.ResultEntry = PromotionResultEntry{}
		_ = c.store.SavePromotion(prom)
		return fmt.Errorf("buildcache: promotion %s aborted at stage %s; state restored",
			prom.RequestID, stage)
	}

	for _, v := range victims {
		if err := c.store.DeleteEntry(prom.TargetNamespace, v.Key); err != nil {
			return nil, rollback("victim_delete")
		}
		deletedVictims = append(deletedVictims, v)
	}
	if prom.Eviction != nil {
		prom.Eviction.Status = EvictionCommitted
		prom.Eviction.CommittedAt = now
		if err := c.store.SaveEvictionDecision(*prom.Eviction); err != nil {
			return nil, rollback("eviction_save")
		}
	}

	finalUsed, err := c.namespaceUsedLocked(prom.TargetNamespace)
	if err != nil {
		return nil, err
	}
	prom.Status = PromotionCommitted
	prom.CommittedAt = now
	prom.UsedAfter = finalUsed
	prom.ReplacedVersion = prom.TargetVersion
	prom.ResultEntry = PromotionResultEntry{
		Namespace: newEntry.Namespace, Key: newEntry.Key,
		Version: newEntry.Version, Digest: newEntry.Digest,
	}
	if err := c.store.SavePromotion(prom); err != nil {
		return nil, rollback("promotion_save")
	}
	if err := c.store.AppendAudit(GCRecord{
		Time: now, Action: GCPromotion, Key: prom.TargetNamespace,
		Detail: fmt.Sprintf(
			"request=%s mode=%s source=%s/%s@%s target=%s/%s@v%d replaced=v%d shared_chunks=%d freed=%d used=%d/%d",
			prom.RequestID, prom.Mode, prom.Source.Namespace, prom.Source.Key, prom.Source.Digest,
			prom.TargetNamespace, prom.TargetKey, newEntry.Version, prom.ReplacedVersion,
			prom.SharedChunks, freed, finalUsed, prom.MaxBytes),
	}); err != nil {
		return nil, rollback("audit")
	}
	return &prom, nil
}

// failPromotion 把晋级记录置为 failed 终态并返回对应的 stale 错误。
func (c *Cache) failPromotion(prom *Promotion, reason, detail string) (*Promotion, error) {
	now := c.clock.Now()
	// 晋级失败意味着淘汰决定被整体放弃：没有任何候选被真正删除，
	// 全部标记 skipped_stale 并清零释放量，再把决策落为终态，避免悬挂的 proposed。
	if prom.Eviction != nil && prom.Eviction.Status == EvictionProposed {
		prom.Eviction.Status = EvictionCommitted
		prom.Eviction.CommittedAt = now
		prom.Eviction.FreedBytes = 0
		for i := range prom.Eviction.Candidates {
			prom.Eviction.Candidates[i].Outcome = EvictSkippedStale
		}
		if err := c.store.SaveEvictionDecision(*prom.Eviction); err != nil {
			return nil, err
		}
	}
	prom.Status = PromotionFailed
	prom.FailReason = reason
	prom.FailDetail = detail
	prom.CommittedAt = now
	if err := c.store.SavePromotion(*prom); err != nil {
		return nil, err
	}
	return nil, staleFromPromotion(prom)
}

func staleFromPromotion(prom *Promotion) error {
	detail := prom.FailDetail
	if detail == "" {
		detail = "promotion ended in failed state"
	}
	return &PromotionStaleError{
		Reason: prom.FailReason, RequestID: prom.RequestID, Detail: detail,
	}
}

// Promote 是"建立 + 立即提交"的便捷入口（典型调用路径）。
// 建立与提交在同一临界区内完成，因此不存在两者之间被其他操作穿插的窗口；
// 对已存在请求号则等价于重放——返回首次提交结果或首次失败。
func (c *Cache) Promote(opts PromoteOptions) (*Promotion, error) {
	opts, _, _, err := validatePromoteOptions(opts)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.createPromotionLocked(opts); err != nil {
		return nil, err
	}
	return c.commitPromotionLocked(opts.RequestID)
}

// ---- 查询 ----

// GetPromotion 按外部请求号查询晋级记录（含结果、两端关系、配额变化、淘汰结果）。
func (c *Cache) GetPromotion(requestID string) (Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.store.GetPromotion(requestID)
	if errors.Is(err, ErrNotFound) {
		return Promotion{}, fmt.Errorf("%w: %s", ErrPromotionNotFound, requestID)
	}
	return p, err
}

// ListPromotions 列出晋级记录；namespace 为空表示全部，非空时返回
// 来源或目标位于该命名空间的记录。
func (c *Cache) ListPromotions(namespace string) ([]Promotion, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ps, err := c.store.ListPromotions()
	if err != nil {
		return nil, err
	}
	out := make([]Promotion, 0, len(ps))
	for _, p := range ps {
		if namespace != "" &&
			normNamespace(p.Source.Namespace) != normNamespace(namespace) &&
			p.TargetNamespace != normNamespace(namespace) {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// PromotionLinks 查询一个条目通过晋级与哪些另一端条目建立了共享关系。
// 返回的每条 Link 描述该 (namespace,key) 作为来源或目标参与的一次晋级。
func (c *Cache) PromotionLinks(namespace, key string) ([]PromotionLink, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	namespace = normNamespace(namespace)
	ps, err := c.store.ListPromotions()
	if err != nil {
		return nil, err
	}
	var out []PromotionLink
	for _, p := range ps {
		if p.Status != PromotionCommitted {
			continue
		}
		if normNamespace(p.Source.Namespace) == namespace && p.Source.Key == key {
			out = append(out, PromotionLink{
				RequestID: p.RequestID, Namespace: p.TargetNamespace, Key: p.TargetKey,
				Version: p.ResultEntry.Version, Digest: p.ResultEntry.Digest,
				SharedChunks: p.SharedChunks, Role: PromotionRoleTarget, CommittedAt: p.CommittedAt,
			})
		}
		if p.TargetNamespace == namespace && p.TargetKey == key {
			out = append(out, PromotionLink{
				RequestID: p.RequestID,
				Namespace: normNamespace(p.Source.Namespace), Key: p.Source.Key,
				Version: p.Source.Version, Digest: p.Source.Digest,
				SharedChunks: p.SharedChunks, Role: PromotionRoleSource, CommittedAt: p.CommittedAt,
			})
		}
	}
	return out, nil
}
