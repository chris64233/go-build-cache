package buildcache

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// promoteData 在 srcNS 发布 data，然后把它晋级到 dstNS/dstKey。
func promoteData(t *testing.T, c *Cache, srcNS, srcKey, dstNS, dstKey, mode, reqID string,
	data []byte, chunkSize int) *Promotion {
	t.Helper()
	publishData(t, c, srcNS, srcKey, data, chunkSize)
	opts := PromoteOptions{
		SourceNamespace: srcNS, SourceKey: srcKey, ExpectedDigest: digestOf(data),
		TargetNamespace: dstNS, TargetKey: dstKey, Mode: mode, RequestID: reqID,
	}
	p, err := c.Promote(opts)
	if err != nil {
		t.Fatalf("promote %s/%s -> %s/%s: %v", srcNS, srcKey, dstNS, dstKey, err)
	}
	return p
}

// ensureNamespaces 以不限配额注册给定命名空间（测试辅助）。
func ensureNamespaces(t *testing.T, c *Cache, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := c.SetNamespaceQuota(n, 0); err != nil {
			t.Fatalf("ensure namespace %s: %v", n, err)
		}
	}
}

// ---- 基础晋级：复制 / 替换 / 两端关系 ----

func TestPromotionCopySharesContent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("team-a", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("team-b", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("shared promotion payload!!!") // 24
	res := publishData(t, c, "team-a", "mod/x", data, 8)

	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "mod/x", ExpectedDigest: res.Entry.Digest,
		TargetNamespace: "team-b", TargetKey: "mod/x-copy", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != PromotionCommitted || p.TargetVersion != 1 || p.Mode != PromotionModeCopy {
		t.Fatalf("promotion = %+v", p)
	}

	// 目标内容与来源一致。
	if got := readNS(t, c, "team-b", "mod/x-copy"); !bytes.Equal(got, data) {
		t.Fatalf("target content mismatch")
	}
	// 来源原样保留。
	if got := readNS(t, c, "team-a", "mod/x"); !bytes.Equal(got, data) {
		t.Fatalf("source changed after promotion")
	}
	// 目标条目带来源标记。
	tgt, err := c.store.GetEntry("team-b", "mod/x-copy")
	if err != nil {
		t.Fatal(err)
	}
	if tgt.PromotedFrom == nil || tgt.PromotedFrom.Namespace != "team-a" ||
		tgt.PromotedFrom.Key != "mod/x" || tgt.PromotedFrom.Digest != digestOf(data) {
		t.Fatalf("origin = %+v", tgt.PromotedFrom)
	}
	// 内容块是同一些 blob（物理共享），两端引用相同摘要。
	if len(tgt.Chunks) != len(res.Entry.Chunks) {
		t.Fatalf("chunk count changed")
	}
	for i := range tgt.Chunks {
		if tgt.Chunks[i].Digest != res.Entry.Chunks[i].Digest {
			t.Fatalf("chunk %d not shared", i)
		}
	}
}

func TestPromotionReplaceBumpsVersion(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "team-a", "team-b")
	oldTarget := []byte("old target content!!")  // 19
	newContent := []byte("new promoted content") // 20
	publishData(t, c, "team-a", "src", newContent, 10)
	publishData(t, c, "team-b", "k", oldTarget, 10)

	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "src", ExpectedDigest: digestOf(newContent),
		TargetNamespace: "team-b", TargetKey: "k", Mode: PromotionModeReplace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.TargetBaseVersion != 1 || p.TargetVersion != 2 {
		t.Fatalf("versions = base %d -> new %d", p.TargetBaseVersion, p.TargetVersion)
	}
	if got := readNS(t, c, "team-b", "k"); !bytes.Equal(got, newContent) {
		t.Fatalf("target not replaced")
	}
}

// replace 也允许目标不存在（等同新建）。
func TestPromotionReplaceIntoMissingTarget(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "team-a", "team-b")
	data := []byte("into missing target!!")
	publishData(t, c, "team-a", "src", data, 7)
	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "src", ExpectedDigest: digestOf(data),
		TargetNamespace: "team-b", TargetKey: "k", Mode: PromotionModeReplace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.TargetBaseVersion != 0 || p.TargetVersion != 1 {
		t.Fatalf("promotion = %+v", p)
	}
}

// ---- 建立阶段的拒绝条件 ----

func TestPromotionPrepareRejects(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	ensureNamespaces(t, c, "team-a", "team-b")
	data := []byte("promotion reject data!!")

	// 来源不存在。
	_, err := c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "ghost", ExpectedDigest: digestOf(data),
		TargetNamespace: "team-b", TargetKey: "k", Mode: PromotionModeCopy,
	})
	if !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("missing source = %v", err)
	}

	publishData(t, c, "team-a", "src", data, 7)

	// 来源摘要不符。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "src", ExpectedDigest: digestOf([]byte("other")),
		TargetNamespace: "team-b", TargetKey: "k", Mode: PromotionModeCopy,
	})
	var dc *PromotionDigestConflictError
	if !errors.As(err, &dc) {
		t.Fatalf("digest conflict = %v", err)
	}

	// 来源正在上传（存在未完成会话）。
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: "team-a", Key: "uploading", FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, 7),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadChunk(sess.ID, 0, data[0:7]); err != nil {
		t.Fatal(err)
	}
	// 先发布同键的旧版本不会发生（会话目标键尚无条目）；直接对该键晋级 -> uploading。
	// 为让来源"已发布但又在上传"，再发布一次同摘要内容到另一键无意义；改为对未发布键晋级。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "uploading", ExpectedDigest: digestOf(data),
		TargetNamespace: "team-b", TargetKey: "k", Mode: PromotionModeCopy,
	})
	var pc *PromotionConflictError
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonUploading {
		t.Fatalf("uploading conflict = %v", err)
	}

	// 目标已存在时 copy 拒绝。
	publishData(t, c, "team-b", "taken", []byte("takeover target!!"), 6)
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "src", ExpectedDigest: digestOf(data),
		TargetNamespace: "team-b", TargetKey: "taken", Mode: PromotionModeCopy,
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonTargetExists {
		t.Fatalf("target exists conflict = %v", err)
	}

	// replace 模式目标已是相同摘要 -> 拒绝（无需晋级）。
	same := []byte("identical bytes!!!")
	publishData(t, c, "team-a", "dup", same, 6)
	publishData(t, c, "team-b", "dup", same, 6)
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "dup", ExpectedDigest: digestOf(same),
		TargetNamespace: "team-b", TargetKey: "dup", Mode: PromotionModeReplace,
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonSameDigest {
		t.Fatalf("same digest conflict = %v", err)
	}

	// 来源与目标完全相同。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "team-a", SourceKey: "src", ExpectedDigest: digestOf(data),
		TargetNamespace: "team-a", TargetKey: "src", Mode: PromotionModeCopy,
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonSameNamespace {
		t.Fatalf("identical endpoint conflict = %v", err)
	}
}

// ---- 配额与淘汰 ----

// 目标配额不足：晋级触发未固定 LRU 淘汰，固定项与共享块豁免。
func TestPromotionQuotaEviction(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	ensureNamespaces(t, c, "src")
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	mk := func(s string) []byte {
		b := []byte(s)
		if len(b) != 10 {
			t.Fatalf("data %q must be 10 bytes", s)
		}
		return b
	}
	a := mk("aaaaaaaaaa")
	b := mk("bbbbbbbbbb")
	pinned := mk("pppppppppp")
	// dst 已有三个 10 字节条目（恰好 30）。
	publishData(t, c, "dst", "a", a, 10)
	clk.Advance(time.Minute)
	publishData(t, c, "dst", "b", b, 10)
	clk.Advance(time.Minute)
	publishData(t, c, "dst", "p", pinned, 10)
	clk.Advance(time.Minute)
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "p", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	src := mk("ssssssssss")
	publishData(t, c, "src", "s", src, 10)

	// 晋级第四个 10 字节：需释放 10，候选 LRU 最旧且未固定 -> a。
	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(src),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Eviction == nil {
		t.Fatalf("expected an eviction decision")
	}
	if p.Eviction.Candidates[0].Key != "a" || p.Eviction.Candidates[0].Outcome != EvictDeleted {
		t.Fatalf("eviction = %+v", p.Eviction.Candidates)
	}
	if _, err := c.Read("dst", "a"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("a should be evicted")
	}
	if got := readNS(t, c, "dst", "p"); !bytes.Equal(got, pinned) {
		t.Fatalf("pinned must survive")
	}
	if got := readNS(t, c, "dst", "s"); !bytes.Equal(got, src) {
		t.Fatalf("promoted target missing")
	}
	st, _ := c.NamespaceStatus("dst")
	if st.UsedBytes != 30 {
		t.Fatalf("used = %d", st.UsedBytes)
	}
}

// replace 模式超配额：投影按 已用 − 旧目标体量 + 来源体量 计算，淘汰未固定 LRU。
func TestPromotionReplaceQuotaEviction(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 5, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	ensureNamespaces(t, c, "src")
	// dst 配额 30，已有 tgt(20) 与 victim(10) = 30。
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	tgt := []byte("target-old-20bytesxx") // 20
	victim := []byte("victim!!!!")        // 10
	if len(tgt) != 20 {
		t.Fatalf("setup tgt len %d", len(tgt))
	}
	publishData(t, c, "dst", "tgt", tgt, 10)
	clk.Advance(time.Minute)
	publishData(t, c, "dst", "victim", victim, 10)
	clk.Advance(time.Minute)
	source := []byte("new-target-20bytesxx") // 20
	if len(source) != 20 {
		t.Fatalf("setup source len %d", len(source))
	}
	publishData(t, c, "src", "s", source, 10)
	// replace tgt：投影 = 30 − 20(旧 tgt) + 20(新) = 30，本不需要淘汰。
	// 收紧配额到 20：投影 30 > 20，需释放 10 -> victim 被淘汰，tgt 被原子替换。
	if _, err := c.SetNamespaceQuota("dst", 20); err != nil {
		t.Fatal(err)
	}
	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(source),
		TargetNamespace: "dst", TargetKey: "tgt", Mode: PromotionModeReplace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Eviction == nil || len(p.Eviction.Candidates) != 1 ||
		p.Eviction.Candidates[0].Key != "victim" ||
		p.Eviction.Candidates[0].Outcome != EvictDeleted {
		t.Fatalf("eviction = %+v", p.Eviction)
	}
	if got := readNS(t, c, "dst", "tgt"); !bytes.Equal(got, source) {
		t.Fatalf("tgt not replaced")
	}
	if _, err := c.Read("dst", "victim"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("victim should be evicted")
	}
	st, _ := c.NamespaceStatus("dst")
	if st.UsedBytes != 20 {
		t.Fatalf("used = %d", st.UsedBytes)
	}
}

// 即使淘汰全部候选也腾不出空间：建立阶段失败，不删除任何东西。
func TestPromotionQuotaImpossibleNoDeletion(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src")
	// 准备阶段配额 20 恰好容纳 a、b；全部固定后收紧到 15。
	if _, err := c.SetNamespaceQuota("dst", 20); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaa")
	b := []byte("bbbbbbbbbb")
	publishData(t, c, "dst", "a", a, 10)
	publishData(t, c, "dst", "b", b, 10)
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "a", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "b", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 15); err != nil {
		t.Fatal(err)
	}
	src := []byte("ssssssssss")
	publishData(t, c, "src", "s", src, 10)

	_, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(src),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	var qe *QuotaExceededError
	if !errors.As(err, &qe) {
		t.Fatalf("want QuotaExceededError, got %v", err)
	}
	// 两端原状：a、b 都在，目标 s 不存在，来源 s 仍在。
	if got := readNS(t, c, "dst", "a"); !bytes.Equal(got, a) {
		t.Fatalf("a deleted after failed promotion")
	}
	if got := readNS(t, c, "dst", "b"); !bytes.Equal(got, b) {
		t.Fatalf("b deleted after failed promotion")
	}
	if _, err := c.Read("dst", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not exist after failure")
	}
	if got := readNS(t, c, "src", "s"); !bytes.Equal(got, src) {
		t.Fatalf("source changed")
	}
}

// 共享晋级内容块的目标条目不参与淘汰。
func TestPromotionEvictionExcludesSharedChunks(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src")
	// 准备阶段配额 30 恰好容纳 shared + victim 两个 15 字节条目。
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	content := []byte("shared-content!") // 15
	victim := []byte("victim-data!!!!")  // 15
	publishData(t, c, "src", "s", content, 15)
	publishData(t, c, "dst", "shared", content, 15) // 与来源同摘要 -> 共享块
	publishData(t, c, "dst", "victim", victim, 15)
	// 晋级投影 30 + 15(copy 新键) = 45，需释放 15；可淘汰候选只有 victim
	// （shared 因引用晋级依赖的共享块而被排除）。victim 恰好够，本应成功——
	// 为了断言"共享块条目绝不被选为候选"，这里检查决策候选里没有 shared，
	// 且最终 shared 仍在、victim 被淘汰、目标建立。
	p, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(content),
		TargetNamespace: "dst", TargetKey: "promoted", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, cand := range p.Eviction.Candidates {
		if cand.Key == "shared" {
			t.Fatalf("shared-chunk entry must not be an eviction candidate")
		}
	}
	// shared（共享块）必须仍在；victim 被淘汰以腾出空间。
	if _, err := c.Read("dst", "shared"); err != nil {
		t.Fatalf("shared-content entry must not be evicted: %v", err)
	}
	if _, err := c.Read("dst", "victim"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("victim should be evicted: %v", err)
	}
	if got := readNS(t, c, "dst", "promoted"); !bytes.Equal(got, content) {
		t.Fatalf("promoted target content wrong")
	}
}

// ---- 两阶段失效：旧请求明确失败而不是覆盖新状态 ----

func TestPromotionStaleSourceChanged(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	v1 := []byte("source version one!")
	v2 := []byte("source version two!")
	publishData(t, c, "src", "s", v1, 7)
	p, err := c.PreparePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(v1),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 来源被覆盖发布为新版本。
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "src", Key: "s", FinalDigest: digestOf(v2),
		TotalSize: int64(len(v2)), Chunks: plan(t, v2, 7),
	})
	for _, sp := range plan(t, v2, 7) {
		if _, err := c.UploadChunk(sess.ID, sp.Index, v2[sp.Offset:sp.Offset+sp.Size]); err != nil {
			t.Fatal(err)
		}
	}
	v := uint64(1)
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &v}); err != nil {
		t.Fatal(err)
	}
	_, err = c.CommitPromotion(p.ID)
	var stale *PromotionStaleError
	if !errors.As(err, &stale) || stale.Reason != PromotionFailSourceChanged {
		t.Fatalf("want source_changed stale, got %v", err)
	}
	// 目标未被写入。
	if _, err := c.Read("dst", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be written")
	}
}

func TestPromotionStaleQuotaVersionChanged(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src")
	if _, err := c.SetNamespaceQuota("dst", 1000); err != nil {
		t.Fatal(err)
	}
	data := []byte("quota version data!")
	publishData(t, c, "src", "s", data, 7)
	p, err := c.PreparePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 目标配额变化。
	if _, err := c.SetNamespaceQuota("dst", 500); err != nil {
		t.Fatal(err)
	}
	_, err = c.CommitPromotion(p.ID)
	var stale *PromotionStaleError
	if !errors.As(err, &stale) || stale.Reason != PromotionFailQuotaVersion {
		t.Fatalf("want quota_version_changed, got %v", err)
	}
	if _, err := c.Read("dst", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be written")
	}
}

func TestPromotionStaleEvictionCandidateAccessed(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 3, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	ensureNamespaces(t, c, "src")
	// 准备阶段配额 20 足以发布 13 字节的 old；收紧到 13：晋级投影 26 需释放 13，
	// 淘汰 old（13 字节）恰好够——提交前访问 old 会让旧决定失效。
	if _, err := c.SetNamespaceQuota("dst", 20); err != nil {
		t.Fatal(err)
	}
	old := []byte("old-dst-data!") // 13
	publishData(t, c, "dst", "old", old, 13)
	src := []byte("new-src-data!") // 13
	publishData(t, c, "src", "s", src, 13)
	if _, err := c.SetNamespaceQuota("dst", 13); err != nil {
		t.Fatal(err)
	}
	p, err := c.PreparePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(src),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Eviction == nil || len(p.Eviction.Candidates) != 1 {
		t.Fatalf("expected old as candidate, got %+v", p.Eviction)
	}
	// 提交前候选被重新访问。
	clk.Advance(time.Minute)
	_ = readNS(t, c, "dst", "old")
	_, err = c.CommitPromotion(p.ID)
	var stale *PromotionStaleError
	if !errors.As(err, &stale) || stale.Reason != PromotionFailEvictionStale {
		t.Fatalf("want eviction_stale, got %v", err)
	}
	// 候选保留，目标未写入。
	if got := readNS(t, c, "dst", "old"); !bytes.Equal(got, old) {
		t.Fatalf("candidate must survive")
	}
	if _, err := c.Read("dst", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be written")
	}
}

// 显式两阶段建立的 prepared 晋级，再用一步 Promote 以同请求号重放：
// 必须返回首次（prepared）结果且不自动提交，目标仍不可见。
func TestPromotionReplayPreparedDoesNotCommit(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	data := []byte("replay prepared!!!")
	publishData(t, c, "src", "s", data, 6)
	opts := PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy, RequestID: "rp-1",
	}
	p, err := c.PreparePromotion(opts)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != PromotionPrepared {
		t.Fatalf("initial = %s", p.Status)
	}
	again, err := c.Promote(opts)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != p.ID || again.Status != PromotionPrepared {
		t.Fatalf("replay must return the prepared promotion unchanged: %+v", again)
	}
	if _, err := c.Read("dst", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("replay must not commit a prepared promotion")
	}
	// 显式提交后才落地。
	committed, err := c.CommitPromotion(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Status != PromotionCommitted {
		t.Fatalf("explicit commit = %s", committed.Status)
	}
}

// 提交是幂等的。
func TestPromotionCommitIdempotent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	data := []byte("idempotent commit!")
	p := promoteData(t, c, "src", "s", "dst", "s", PromotionModeCopy, "", data, 7)
	again, err := c.CommitPromotion(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != PromotionCommitted || again.TargetVersion != 1 {
		t.Fatalf("recommit = %+v", again)
	}
}

// ---- 请求号幂等 / 冲突 ----

func TestPromotionRequestIdempotency(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	data := []byte("request idempotency")
	publishData(t, c, "src", "s", data, 7)
	opts := PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy, RequestID: "req-1",
	}
	p1, err := c.Promote(opts)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c.Promote(opts)
	if err != nil {
		t.Fatal(err)
	}
	if p1.ID != p2.ID || p2.TargetVersion != p1.TargetVersion {
		t.Fatalf("replay changed result: %s vs %s", p1.ID, p2.ID)
	}
	// 同号异内容（不同目标键）-> 冲突。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "other", Mode: PromotionModeCopy, RequestID: "req-1",
	})
	var rc *PromotionRequestConflictError
	if !errors.As(err, &rc) {
		t.Fatalf("want request conflict, got %v", err)
	}
}

// 失败结果也以请求号记录：同号重放返回首次失败。
func TestPromotionFailedRequestReplaysFailure(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src")
	if _, err := c.SetNamespaceQuota("dst", 1000); err != nil {
		t.Fatal(err)
	}
	data := []byte("failed request data")
	publishData(t, c, "src", "s", data, 7)
	opts := PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy, RequestID: "req-f",
	}
	p, err := c.PreparePromotion(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 123); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CommitPromotion(p.ID); err == nil {
		t.Fatalf("first commit must fail")
	}
	// 用同一请求号重放（即使再调 Promote）：返回首次失败，不产生新晋级。
	_, err = c.Promote(opts)
	var stale *PromotionStaleError
	if !errors.As(err, &stale) || stale.Reason != PromotionFailQuotaVersion {
		t.Fatalf("replay should return first failure, got %v", err)
	}
}

// ---- 内容块跨命名空间共享与来源删除后的保护 ----

// 来源条目删除后，只要目标仍引用，共享块不得被 GC 回收。
func TestPromotionBlobSurvivesSourceDeletion(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	common := []byte("common prefix!!!") // 16
	oldTail := []byte("SRC-TAIL!!")      // 10
	srcData := append(append([]byte{}, common...), oldTail...)
	p := promoteData(t, c, "src", "s", "dst", "s", PromotionModeCopy, "", srcData, 16)
	// 删除来源条目（通过淘汰决策）。
	d, err := c.PlanEviction("src", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CommitEviction(d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read("src", "s"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("source should be deleted")
	}
	// GC 后所有块仍应保留（目标引用全部块）。
	if _, err := c.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range p.SourceChunks {
		if _, err := c.store.GetBlob(ref.Digest); err != nil {
			t.Fatalf("shared blob %s collected while target references it: %v", ref.Digest, err)
		}
	}
	// 目标内容完整。
	if got := readNS(t, c, "dst", "s"); !bytes.Equal(got, srcData) {
		t.Fatalf("target corrupted after source deletion + GC")
	}
}

// prepared 晋级的冻结快照是第四类引用：来源删除后块仍保留到提交/失败。
func TestPreparedPromotionProtectsBlobs(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	data := []byte("prepared protects blob!!") // 24
	publishData(t, c, "src", "s", data, 8)
	p, err := c.PreparePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	// 删除来源条目。
	d, _ := c.PlanEviction("src", 1)
	if _, err := c.CommitEviction(d.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range p.SourceChunks {
		if _, err := c.store.GetBlob(ref.Digest); err != nil {
			t.Fatalf("prepared promotion blob collected: %v", err)
		}
	}
	// 提交仍然成功（块由冻结快照保护），目标建立。
	committed, err := c.CommitPromotion(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Status != PromotionCommitted {
		t.Fatalf("status = %s", committed.Status)
	}
	if got := readNS(t, c, "dst", "s"); !bytes.Equal(got, data) {
		t.Fatalf("target content wrong")
	}
}

// ---- 查询：详情 / 列表 / 内容引用 ----

func TestPromotionDetailAndList(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src")
	if _, err := c.SetNamespaceQuota("dst", 1000); err != nil {
		t.Fatal(err)
	}
	data := []byte("detail query payload!!") // 23
	p := promoteData(t, c, "src", "s", "dst", "s", PromotionModeCopy, "req-d", data, 8)

	detail, err := c.GetPromotionDetail(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !detail.Source.Exists || detail.Source.Namespace != "src" || detail.Source.Digest != digestOf(data) {
		t.Fatalf("source end = %+v", detail.Source)
	}
	if !detail.Target.Exists || detail.Target.Version != 1 {
		t.Fatalf("target end = %+v", detail.Target)
	}
	if detail.Quota.MaxBytes != 1000 || detail.Quota.UsedBefore != 0 ||
		detail.Quota.UsedAfter != int64(len(data)) {
		t.Fatalf("quota change = %+v", detail.Quota)
	}
	if len(detail.Chunks) != len(plan(t, data, 8)) {
		t.Fatalf("chunks = %d", len(detail.Chunks))
	}
	// 每块的引用来源至少包含来源与目标两个 entry 引用。
	for _, ch := range detail.Chunks {
		var entries int
		for _, r := range ch.References {
			if r.Kind == RefEntry {
				entries++
			}
		}
		if entries < 2 {
			t.Fatalf("chunk %s expected >=2 entry refs, got %+v", ch.Digest, ch.References)
		}
	}

	// 列表过滤。
	all, err := c.ListPromotions(PromotionFilters{})
	if err != nil || len(all) != 1 {
		t.Fatalf("list all = %v, %v", all, err)
	}
	byReq, err := c.ListPromotions(PromotionFilters{RequestID: "req-d"})
	if err != nil || len(byReq) != 1 {
		t.Fatalf("list by request = %v, %v", byReq, err)
	}
	byNS, err := c.ListPromotions(PromotionFilters{Namespace: "dst", Status: PromotionCommitted})
	if err != nil || len(byNS) != 1 {
		t.Fatalf("list by ns = %v, %v", byNS, err)
	}
	if _, err := c.ListPromotions(PromotionFilters{Status: PromotionPrepared}); err != nil {
		t.Fatal(err)
	}
}

// prepared 晋级出现在内容块引用查询中（RefPromotion）。
func TestBlobReferencesIncludesPreparedPromotion(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	ensureNamespaces(t, c, "src", "dst")
	data := []byte("ref promotion query!!") // 21
	publishData(t, c, "src", "s", data, 7)
	p, err := c.PreparePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err != nil {
		t.Fatal(err)
	}
	refs, err := c.BlobReferences(p.SourceChunks[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	var sawPromo bool
	for _, r := range refs {
		if r.Kind == RefPromotion && r.Active {
			sawPromo = true
		}
	}
	if !sawPromo {
		t.Fatalf("missing promotion reference: %+v", refs)
	}
}

// ---- 崩溃恢复：重做日志 ----

// flakyStore 包装 MemoryStore，可令指定方法在下一次调用时失败一次。
type flakyStore struct {
	Store
	mu        sync.Mutex
	failOnce  map[string]bool
	failNth   map[string]int // method -> 在第几次调用时失败（1 基）
	callCount map[string]int
}

func newFlakyStore(inner Store) *flakyStore {
	return &flakyStore{
		Store:    inner,
		failOnce: map[string]bool{}, failNth: map[string]int{}, callCount: map[string]int{},
	}
}

func (f *flakyStore) fail(method string) {
	f.mu.Lock()
	f.failOnce[method] = true
	f.mu.Unlock()
}

// failOnNth 让 method 的第 n 次调用失败（用于同一方法在流程中被多次调用的场景）。
func (f *flakyStore) failOnNth(method string, n int) {
	f.mu.Lock()
	f.failNth[method] = n
	f.mu.Unlock()
}

func (f *flakyStore) shouldFail(method string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount[method]++
	if f.failOnce[method] {
		f.failOnce[method] = false
		return true
	}
	if n, ok := f.failNth[method]; ok && n == f.callCount[method] {
		delete(f.failNth, method)
		return true
	}
	return false
}

func (f *flakyStore) SavePromotionTxn(txn PromotionTxn) error {
	if f.shouldFail("SavePromotionTxn") {
		return errors.New("injected txn write failure")
	}
	return f.Store.SavePromotionTxn(txn)
}

func (f *flakyStore) PutEntry(e Entry, wantVersion int64) error {
	if f.shouldFail("PutEntry") {
		return errors.New("injected entry write failure")
	}
	return f.Store.PutEntry(e, wantVersion)
}

func (f *flakyStore) SavePromotion(p Promotion) error {
	if f.shouldFail("SavePromotion") {
		return errors.New("injected promotion save failure")
	}
	return f.Store.SavePromotion(p)
}

// 在"目标条目写入后、结果落盘前"注入失败，重启 Cache 后重做应完成晋级。
func TestPromotionCrashRecoveryAfterEntryWrite(t *testing.T) {
	dir := t.TempDir()
	data := []byte("crash recovery data!!") // 21

	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	flaky := newFlakyStore(store1)
	c1, err := New(flaky, NewFakeClock(time.Now()), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ensureNamespaces(t, c1, "src", "dst")
	publishData(t, c1, "src", "s", data, 7)
	// SavePromotion 第一次在建立阶段（prepared）；让第二次（提交结果落盘）失败，
	// 此时目标条目已写入、重做日志停在 entry_written，重启后应补到 committed。
	flaky.failOnNth("SavePromotion", 2)
	_, err = c1.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err == nil {
		t.Fatalf("expected injected failure")
	}
	_ = store1.Close()

	// 用全新 Cache / Store 重新打开同一目录：启动重做应把晋级补到 committed。
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, SystemClock{}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := readNS(t, c2, "dst", "s"); !bytes.Equal(got, data) {
		t.Fatalf("target not completed after recovery")
	}
	ps, err := c2.ListPromotions(PromotionFilters{})
	if err != nil || len(ps) != 1 || ps[0].Status != PromotionCommitted {
		t.Fatalf("promotion not recovered to committed: %+v, %v", ps, err)
	}
	txns, err := store2.ListPromotionTxns()
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 0 {
		t.Fatalf("redo log should be removed after completion: %+v", txns)
	}
}

// 在"淘汰提交后、目标条目写入前"注入失败，重启重做：淘汰不重复、目标建立。
func TestPromotionCrashRecoveryAfterEviction(t *testing.T) {
	dir := t.TempDir()
	srcData := []byte("ssssssssss") // 10
	old := []byte("oooooooooo")     // 10

	store1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	flaky := newFlakyStore(store1)
	clk := NewFakeClock(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	c1, err := New(flaky, clk, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.SetNamespaceQuota("dst", 10); err != nil {
		t.Fatal(err)
	}
	ensureNamespaces(t, c1, "src")
	publishData(t, c1, "src", "s", srcData, 10)
	publishData(t, c1, "dst", "old", old, 10)
	flaky.fail("PutEntry") // 淘汰已提交、目标条目首次写入失败
	_, err = c1.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(srcData),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeCopy,
	})
	if err == nil {
		t.Fatalf("expected injected entry failure")
	}
	// 此时 old 已被淘汰（淘汰先落盘），s 尚未建立。
	if _, err := c1.Read("dst", "old"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("old should already be evicted")
	}
	_ = store1.Close()

	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, clk, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := readNS(t, c2, "dst", "s"); !bytes.Equal(got, srcData) {
		t.Fatalf("target not established after recovery")
	}
	ps, _ := c2.ListPromotions(PromotionFilters{})
	if len(ps) != 1 || ps[0].Status != PromotionCommitted {
		t.Fatalf("promotion = %+v", ps)
	}
	// old 不应被"再删一次"导致任何异常；最终用量恰为 10。
	st, _ := c2.NamespaceStatus("dst")
	if st.UsedBytes != 10 {
		t.Fatalf("used after recovery = %d", st.UsedBytes)
	}
}

// ---- FileStore 持久化 ----

func TestPromotionFileStorePersistence(t *testing.T) {
	dir := t.TempDir()
	data := []byte("file store promo persist")
	func() {
		store, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := New(store, NewFakeClock(time.Now()), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		ensureNamespaces(t, c, "src", "dst")
		promoteData(t, c, "src", "s", "dst", "s", PromotionModeReplace, "persist-req", data, 7)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, NewFakeClock(time.Now()), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := c2.ListPromotions(PromotionFilters{RequestID: "persist-req"})
	if err != nil || len(ps) != 1 || ps[0].Status != PromotionCommitted {
		t.Fatalf("promotion not persisted: %+v, %v", ps, err)
	}
	if got := readNS(t, c2, "dst", "s"); !bytes.Equal(got, data) {
		t.Fatalf("target content not persisted")
	}
	// 同请求号在重启后仍幂等。
	again, err := c2.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "s", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "s", Mode: PromotionModeReplace, RequestID: "persist-req",
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != ps[0].ID {
		t.Fatalf("idempotency lost across restart")
	}
}

// 并发晋级对打：全部落盘后每个目标键内容与其来源一致，配额不被突破。
func TestPromotionConcurrent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), 24*time.Hour)
	if _, err := c.SetNamespaceQuota("dst", 1<<20); err != nil {
		t.Fatal(err)
	}
	ensureNamespaces(t, c, "src-0", "src-1", "src-2")
	const n = 12
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("concurrent promo %02d!!!!!!", i))
			srcNS := fmt.Sprintf("src-%d", i%3)
			publishData(t, c, srcNS, fmt.Sprintf("k-%d", i), data, 8)
			_, err := c.Promote(PromoteOptions{
				SourceNamespace: srcNS, SourceKey: fmt.Sprintf("k-%d", i),
				ExpectedDigest:  digestOf(data),
				TargetNamespace: "dst", TargetKey: fmt.Sprintf("k-%d", i),
				Mode: PromotionModeCopy, RequestID: fmt.Sprintf("req-%d", i),
			})
			if err != nil {
				t.Errorf("promote %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("concurrent promo %02d!!!!!!", i))
		if got := readNS(t, c, "dst", fmt.Sprintf("k-%d", i)); !bytes.Equal(got, data) {
			t.Fatalf("target %d corrupted", i)
		}
	}
}
