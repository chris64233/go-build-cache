package buildcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// promoteSetup 在 src 命名空间发布来源条目。
func publishIn(t *testing.T, c *Cache, namespace, key string, data []byte, chunkSize int) *PublishResult {
	t.Helper()
	return publishData(t, c, namespace, key, data, chunkSize)
}

// ---- 基础晋级：copy 与 replace ----

// copy 晋级在目标命名空间产生共享同一组块的新条目；来源删除后目标内容仍可读。
func TestPromotionCopySharesContent(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}

	data := []byte("shared promotion content!!!") // 27，4 个块（8/8/8/3）
	pub := publishIn(t, c, "src", "art", data, 8)

	prom, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		ExpectedDigest:  pub.Entry.Digest,
		TargetNamespace: "dst", TargetKey: "art-copy",
		Mode: PromoteCopy, RequestID: "promo-1",
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if prom.Status != PromotionCommitted || prom.ResultEntry.Version != 1 {
		t.Fatalf("promotion = %+v", prom)
	}
	if prom.ResultEntry.Digest != pub.Entry.Digest || prom.SharedChunks != 4 {
		t.Fatalf("result = %+v shared=%d", prom.ResultEntry, prom.SharedChunks)
	}

	// 目标内容完整可读。
	if got := readNS(t, c, "dst", "art-copy"); !bytes.Equal(got, data) {
		t.Fatalf("target content mismatch")
	}
	// 来源仍然存在。
	if got := readNS(t, c, "src", "art"); !bytes.Equal(got, data) {
		t.Fatalf("source content lost")
	}

	// 两端条目关系可查。
	links, err := c.PromotionLinks("src", "art")
	if err != nil || len(links) != 1 || links[0].Role != PromotionRoleTarget ||
		links[0].Namespace != "dst" || links[0].Key != "art-copy" {
		t.Fatalf("source links = %+v, %v", links, err)
	}
	links, err = c.PromotionLinks("dst", "art-copy")
	if err != nil || len(links) != 1 || links[0].Role != PromotionRoleSource {
		t.Fatalf("target links = %+v, %v", links, err)
	}

	// 每个块同时被两个命名空间的条目引用。
	for _, ref := range pub.Entry.Chunks {
		rs, err := c.BlobReferences(ref.Digest)
		if err != nil {
			t.Fatal(err)
		}
		var entries int
		for _, r := range rs {
			if r.Kind == RefEntry {
				entries++
			}
		}
		if entries != 2 {
			t.Fatalf("chunk %s entry refs = %d (%+v)", ref.Digest, entries, rs)
		}
	}

	// 删除来源条目后 GC：块仍被目标引用，必须保留，目标内容不变。
	if err := c.store.DeleteEntry("src", "art"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	if got := readNS(t, c, "dst", "art-copy"); !bytes.Equal(got, data) {
		t.Fatalf("target corrupted after source deletion and GC")
	}
	for _, ref := range pub.Entry.Chunks {
		if _, err := c.store.BlobSize(ref.Digest); err != nil {
			t.Fatalf("shared blob reclaimed while target references it: %v", err)
		}
	}

	// 删除目标后再 GC：块被回收。
	if err := c.store.DeleteEntry("dst", "art-copy"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.BlobSize(pub.Entry.Chunks[0].Digest); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("blob should be reclaimed after both sides gone, got %v", err)
	}
}

// replace 晋级替换同名旧条目，版本递增，旧条目独有块可被 GC（无其他引用时）。
func TestPromotionReplace(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	old := []byte("old target content")  // 18
	new := []byte("brand new target!!!") // 18
	publishIn(t, c, "dst", "k", old, 9)
	publishIn(t, c, "src", "art", new, 9)

	prom, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k",
		Mode: PromoteReplace, RequestID: "r1",
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if prom.ResultEntry.Version != 2 || prom.ReplacedVersion != 1 {
		t.Fatalf("prom = %+v", prom)
	}
	if got := readNS(t, c, "dst", "k"); !bytes.Equal(got, new) {
		t.Fatalf("replace content = %q", got)
	}
	// 来源不受影响。
	if got := readNS(t, c, "src", "art"); !bytes.Equal(got, new) {
		t.Fatalf("source changed")
	}
}

// replace 模式目标键不存在时退化为新建（v1）。
func TestPromotionReplaceCreatesWhenAbsent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("fresh target entry!!!")
	publishIn(t, c, "src", "art", data, 7)
	prom, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k",
		Mode: PromoteReplace, RequestID: "r2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if prom.ResultEntry.Version != 1 || prom.ReplacedVersion != 0 {
		t.Fatalf("prom = %+v", prom)
	}
}

// ---- 前置校验 ----

func TestPromotionPreconditions(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("precondition data!!")
	pub := publishIn(t, c, "src", "art", data, 7)

	// 来源条目不存在。
	_, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "ghost",
		TargetNamespace: "dst", TargetKey: "g", Mode: PromoteCopy, RequestID: "p1",
	})
	var pc *PromotionConflictError
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonMissing {
		t.Fatalf("want source_missing, got %v", err)
	}

	// 摘要不符。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		ExpectedDigest:  digestOf([]byte("other")),
		TargetNamespace: "dst", TargetKey: "g", Mode: PromoteCopy, RequestID: "p2",
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonDigest {
		t.Fatalf("want source_digest_mismatch, got %v", err)
	}

	// 来源键正在上传（存在未过期 open 会话）。
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: "src", Key: "art", FinalDigest: digestOf(append(data, 'x')),
		TotalSize: int64(len(data)) + 1,
		Chunks:    plan(t, append(data, 'x'), 7),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		ExpectedDigest:  pub.Entry.Digest,
		TargetNamespace: "dst", TargetKey: "g2", Mode: PromoteCopy, RequestID: "p3",
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonUploading {
		t.Fatalf("want source_uploading, got %v", err)
	}
	// 取消上传会话后，后续前置检查才不会继续命中 uploading。
	if err := c.Cancel(sess.ID); err != nil {
		t.Fatal(err)
	}

	// copy 目标已存在。
	publishIn(t, c, "dst", "exists", data, 7)
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "exists", Mode: PromoteCopy, RequestID: "p4",
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonTargetExists {
		t.Fatalf("want target_exists, got %v", err)
	}

	// 来源与目标完全相同。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "src", TargetKey: "art", Mode: PromoteCopy, RequestID: "p5",
	})
	if !errors.As(err, &pc) || pc.Reason != PromotionReasonSameEntry {
		t.Fatalf("want same_entry, got %v", err)
	}

	// 目标命名空间不存在。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "nope", TargetKey: "x", Mode: PromoteCopy, RequestID: "p6",
	})
	if !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("want namespace not found, got %v", err)
	}

	// 会话取消后晋级恢复可行。
	if _, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		ExpectedDigest:  pub.Entry.Digest,
		TargetNamespace: "dst", TargetKey: "ok", Mode: PromoteCopy, RequestID: "p7",
	}); err != nil {
		t.Fatalf("promotion after upload canceled should succeed: %v", err)
	}
}

// ---- 目标配额淘汰 ----

// 目标配额不足：按未固定 LRU 淘汰，固定项豁免；来源内容与目标新条目不被淘汰。
func TestPromotionQuotaEviction(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	// dst：a、b 各 10，配额 30；来源 20 字节。投影 20+20=40 >30，需释放 10 -> 删 a。
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaa")
	b := []byte("bbbbbbbbbb")
	srcData := []byte("source-data-20-bytes!!")[:20]
	publishIn(t, c, "dst", "a", a, 10)
	publishIn(t, c, "dst", "b", b, 10)
	publishIn(t, c, "src", "art", srcData, 10)

	prom, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "q1",
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if prom.Eviction == nil {
		t.Fatalf("expected eviction decision")
	}
	outcomes := map[string]string{}
	for _, cand := range prom.Eviction.Candidates {
		outcomes[cand.Key] = cand.Outcome
	}
	if outcomes["a"] != EvictDeleted {
		t.Fatalf("outcomes = %v", outcomes)
	}
	if _, err := c.Read("dst", "a"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("a should be evicted")
	}
	if got := readNS(t, c, "dst", "b"); !bytes.Equal(got, b) {
		t.Fatalf("b lost")
	}
	if got := readNS(t, c, "dst", "k"); !bytes.Equal(got, srcData) {
		t.Fatalf("promoted target content bad")
	}
	st, _ := c.NamespaceStatus("dst")
	if st.UsedBytes != 30 {
		t.Fatalf("used = %d, want 30", st.UsedBytes)
	}
	if prom.UsedAfter != 30 {
		t.Fatalf("promotion used_after = %d", prom.UsedAfter)
	}
}

// 同命名空间内复制：来源条目是晋级依赖内容，即使它最旧也不得被淘汰。
func TestPromotionSameNamespaceSourceExcludedFromEviction(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	// 配额 40：a(10) 先发布，src(20) 后发布 => 已用 30；复制 src 为新键 20，
	// 投影 50 > 40，需释放 10。候选只有 a（src 被排除）。
	if _, err := c.SetNamespaceQuota("ns", 40); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaa")
	srcData := make([]byte, 20)
	for i := range srcData {
		srcData[i] = 's'
	}
	publishIn(t, c, "ns", "a", a, 10)
	publishIn(t, c, "ns", "src", srcData, 10)

	prom, err := c.Promote(PromoteOptions{
		SourceNamespace: "ns", SourceKey: "src",
		TargetNamespace: "ns", TargetKey: "src-copy",
		Mode: PromoteCopy, RequestID: "q2",
	})
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	for _, cand := range prom.Eviction.Candidates {
		if cand.Key == "src" {
			t.Fatalf("source entry must not be an eviction candidate")
		}
	}
	if got := readNS(t, c, "ns", "src"); len(got) != 20 {
		t.Fatalf("source evicted")
	}
	if _, err := c.Read("ns", "a"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("a should be evicted to fit quota")
	}
	st, _ := c.NamespaceStatus("ns")
	if st.UsedBytes != 40 {
		t.Fatalf("used = %d", st.UsedBytes)
	}
}

// 全部候选都被固定、腾不出空间：晋级失败且两端原状不变。
func TestPromotionQuotaAllPinnedFails(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 25); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaa")
	b := []byte("bbbbbbbbbb")
	publishIn(t, c, "dst", "a", a, 10)
	publishIn(t, c, "dst", "b", b, 10)
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "a", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "b", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)

	_, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "q3",
	})
	var se *PromotionStaleError
	if !errors.As(err, &se) || se.Reason != PromotionStaleEviction {
		t.Fatalf("want stale eviction, got %v", err)
	}
	// 目标未建立，来源与 a/b 原样。
	if _, err := c.Read("dst", "k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be created")
	}
	if got := readNS(t, c, "dst", "a"); !bytes.Equal(got, a) {
		t.Fatalf("a changed after failed promotion")
	}
	// 失败为终态：同号提交重放返回同一失败。
	if _, err := c.CommitPromotion("q3"); !errors.As(err, &se) {
		t.Fatalf("replay commit = %v", err)
	}
	got, err := c.GetPromotion("q3")
	if err != nil || got.Status != PromotionFailed || got.FailReason != PromotionStaleEviction {
		t.Fatalf("promotion record = %+v, %v", got, err)
	}
}

// ---- 两阶段：提交期间状态漂移使旧请求失败 ----

// 提交前访问淘汰候选 -> 旧淘汰决定失效，晋级失败，不删除任何条目。
func TestPromotionStaleWhenCandidateAccessed(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaa")
	b := []byte("bbbbbbbbbb")
	publishIn(t, c, "dst", "a", a, 10)
	publishIn(t, c, "dst", "b", b, 10)
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)

	proposed, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if proposed.Status != PromotionProposed || proposed.Eviction == nil {
		t.Fatalf("proposed = %+v", proposed)
	}

	// 提交前访问候选 a，使其相对淘汰快照变新。
	clk.Advance(time.Minute)
	_ = readNS(t, c, "dst", "a")

	_, err = c.CommitPromotion("s1")
	var se *PromotionStaleError
	if !errors.As(err, &se) || se.Reason != PromotionStaleEviction {
		t.Fatalf("commit = %v", err)
	}
	// 淘汰决定中 a 标记 skipped_stale（整体放弃），且没有任何条目被删。
	rec, _ := c.GetPromotion("s1")
	if rec.Eviction.Candidates[0].Outcome != EvictSkippedStale {
		t.Fatalf("outcome = %s", rec.Eviction.Candidates[0].Outcome)
	}
	if got := readNS(t, c, "dst", "a"); !bytes.Equal(got, a) {
		t.Fatalf("candidate deleted despite access")
	}
	if _, err := c.Read("dst", "k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target created on failed commit")
	}
}

// 提交前固定候选 -> 旧淘汰决定失效。
func TestPromotionStaleWhenCandidatePinned(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	publishIn(t, c, "dst", "a", []byte("aaaaaaaaaa"), 10)
	publishIn(t, c, "dst", "b", []byte("bbbbbbbbbb"), 10)
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)

	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s2",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pin(PinOptions{Namespace: "dst", Key: "a", TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s2"); !errors.As(err, &se) ||
		se.Reason != PromotionStaleEviction {
		t.Fatalf("commit = %v", err)
	}
	if _, err := c.Read("dst", "a"); err != nil {
		t.Fatalf("pinned candidate deleted: %v", err)
	}
}

// 提交前候选被重新发布 -> 旧淘汰决定失效。
func TestPromotionStaleWhenCandidateRepublished(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	a1 := []byte("aaaaaaaaaa")
	a2 := []byte("aaaaaaaaa2")
	b := []byte("bbbbbbbbbb")
	publishIn(t, c, "dst", "a", a1, 10)
	publishIn(t, c, "dst", "b", b, 10)
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)

	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s3",
	}); err != nil {
		t.Fatal(err)
	}
	v := uint64(1)
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "dst", Key: "a", FinalDigest: digestOf(a2),
		TotalSize: 10, Chunks: plan(t, a2, 10),
	})
	if _, err := c.UploadChunk(sess.ID, 0, a2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &v}); err != nil {
		t.Fatal(err)
	}
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s3"); !errors.As(err, &se) {
		t.Fatalf("commit = %v", err)
	}
	if got := readNS(t, c, "dst", "a"); !bytes.Equal(got, a2) {
		t.Fatalf("republished candidate touched")
	}
}

// 来源摘要在创建后变化 -> 旧请求按 source_changed 失败。
func TestPromotionStaleSourceChanged(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	v1 := []byte("source version one!")
	v2 := []byte("source version two!")
	publishIn(t, c, "src", "art", v1, 7)
	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s4",
	}); err != nil {
		t.Fatal(err)
	}
	// 来源被覆盖发布为新摘要。
	ver := uint64(1)
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "src", Key: "art", FinalDigest: digestOf(v2),
		TotalSize: int64(len(v2)), Chunks: plan(t, v2, 7),
	})
	for _, sp := range plan(t, v2, 7) {
		if _, err := c.UploadChunk(sess.ID, sp.Index, v2[sp.Offset:sp.Offset+sp.Size]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &ver}); err != nil {
		t.Fatal(err)
	}
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s4"); !errors.As(err, &se) ||
		se.Reason != PromotionStaleSource {
		t.Fatalf("commit = %v", err)
	}
	if _, err := c.Read("dst", "k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be created")
	}
}

// 目标配额版本在创建后变化 -> 旧请求按 quota_version_changed 失败。
func TestPromotionStaleQuotaChanged(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 1000); err != nil {
		t.Fatal(err)
	}
	data := []byte("quota version data!!")
	publishIn(t, c, "src", "art", data, 7)
	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s5",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 500); err != nil {
		t.Fatal(err)
	}
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s5"); !errors.As(err, &se) ||
		se.Reason != PromotionStaleQuota {
		t.Fatalf("commit = %v", err)
	}
}

// replace：创建后目标条目被推进到新版本 -> 旧请求按 target_changed 失败。
func TestPromotionStaleTargetAdvanced(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	oldTarget := []byte("old target target")
	srcData := []byte("source promotion!!")
	other := []byte("racer published!!")
	publishIn(t, c, "dst", "k", oldTarget, 6)
	publishIn(t, c, "src", "art", srcData, 6)
	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteReplace, RequestID: "s6",
	}); err != nil {
		t.Fatal(err)
	}
	// 并发方把目标推进到 v2。
	ver := uint64(1)
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "dst", Key: "k", FinalDigest: digestOf(other),
		TotalSize: int64(len(other)), Chunks: plan(t, other, 6),
	})
	for _, sp := range plan(t, other, 6) {
		if _, err := c.UploadChunk(sess.ID, sp.Index, other[sp.Offset:sp.Offset+sp.Size]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &ver}); err != nil {
		t.Fatal(err)
	}
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s6"); !errors.As(err, &se) ||
		se.Reason != PromotionStaleTarget {
		t.Fatalf("commit = %v", err)
	}
	if got := readNS(t, c, "dst", "k"); !bytes.Equal(got, other) {
		t.Fatalf("new target state overwritten by stale promotion")
	}
}

// 创建后目标命名空间用量增长（新建时无需淘汰），提交时超配额 -> 旧请求失败不覆盖。
func TestPromotionStaleUsageGrewAfterCreate(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	// 配额 40：创建晋级时 dst 为空，来源 20 字节 -> 投影 20<=40，无需淘汰。
	if _, err := c.SetNamespaceQuota("dst", 40); err != nil {
		t.Fatal(err)
	}
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)
	if _, err := c.CreatePromotion(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "s7",
	}); err != nil {
		t.Fatal(err)
	}
	// 提交前 dst 被写入 30 字节其他条目：提交投影 30+20=50 > 40，且无冻结淘汰决定。
	publishIn(t, c, "dst", "late", make([]byte, 30), 10)
	var se *PromotionStaleError
	if _, err := c.CommitPromotion("s7"); !errors.As(err, &se) ||
		se.Reason != PromotionStaleEviction {
		t.Fatalf("commit = %v", err)
	}
	if _, err := c.Read("dst", "k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("target must not be created")
	}
	if _, err := c.Read("dst", "late"); err != nil {
		t.Fatalf("late entry must remain untouched: %v", err)
	}
}

// ---- 请求号幂等 ----

func TestPromotionRequestIdempotency(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("idempotent promote!!")
	pub := publishIn(t, c, "src", "art", data, 7)
	opts := PromoteOptions{
		SourceNamespace: "src", SourceKey: "art", ExpectedDigest: pub.Entry.Digest,
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "idem-1",
	}
	first, err := c.Promote(opts)
	if err != nil {
		t.Fatal(err)
	}
	// 同号同内容重放：返回首次结果。
	second, err := c.Promote(opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.ResultEntry != first.ResultEntry || second.CommittedAt != first.CommittedAt {
		t.Fatalf("replay changed result: %+v vs %+v", second, first)
	}
	// 只产生一个目标条目（v1）。
	got := readNS(t, c, "dst", "k")
	if !bytes.Equal(got, data) {
		t.Fatalf("target content changed on replay")
	}

	// 同号不同内容（换目标键）-> 冲突。
	_, err = c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "other", Mode: PromoteCopy, RequestID: "idem-1",
	})
	var prc *PromotionRequestConflictError
	if !errors.As(err, &prc) {
		t.Fatalf("want PromotionRequestConflictError, got %v", err)
	}

	// 列表查询只含一条记录。
	ps, err := c.ListPromotions("")
	if err != nil || len(ps) != 1 {
		t.Fatalf("promotions = %d, %v", len(ps), err)
	}
	ps, err = c.ListPromotions("src")
	if err != nil || len(ps) != 1 {
		t.Fatalf("filter by src = %d, %v", len(ps), err)
	}
	ps, err = c.ListPromotions("unrelated")
	if err != nil || len(ps) != 0 {
		t.Fatalf("filter unrelated = %d", len(ps))
	}
}

// 提交幂等：晋级提交成功后重复提交返回同一结果，不重复淘汰或建条目。
func TestPromotionCommitIdempotent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
		t.Fatal(err)
	}
	publishIn(t, c, "dst", "a", []byte("aaaaaaaaaa"), 10)
	publishIn(t, c, "dst", "b", []byte("bbbbbbbbbb"), 10)
	srcData := make([]byte, 20)
	publishIn(t, c, "src", "art", srcData, 10)

	first, err := c.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art",
		TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "idem-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.CommitPromotion("idem-2")
	if err != nil {
		t.Fatal(err)
	}
	if second.ResultEntry != first.ResultEntry || second.UsedAfter != first.UsedAfter {
		t.Fatalf("recommit changed result")
	}
}

// ---- 持久化失败回滚：任一步失败保持两端原状，之后可重试成功 ----

// faultStore 在指定调用点注入一次失败，其余全部委托给 MemoryStore。
type faultStore struct {
	*MemoryStore
	mu        sync.Mutex
	failAfter map[string]int // 方法名 -> 第几次调用时失败（0 表示不注入）
	calls     map[string]int
}

func newFaultStore() *faultStore {
	return &faultStore{
		MemoryStore: NewMemoryStore(),
		failAfter:   map[string]int{},
		calls:       map[string]int{},
	}
}

func (f *faultStore) arm(method string, after int) { f.failAfter[method] = after }

var errInjected = errors.New("injected persistence failure")

func (f *faultStore) tick(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[method]++
	if after, ok := f.failAfter[method]; ok && after > 0 && f.calls[method] == after {
		return errInjected
	}
	return nil
}

func (f *faultStore) DeleteEntry(namespace, key string) error {
	if err := f.tick("DeleteEntry"); err != nil {
		return err
	}
	return f.MemoryStore.DeleteEntry(namespace, key)
}

func (f *faultStore) SaveEvictionDecision(d EvictionDecision) error {
	if err := f.tick("SaveEvictionDecision"); err != nil {
		return err
	}
	return f.MemoryStore.SaveEvictionDecision(d)
}

func (f *faultStore) SavePromotion(p Promotion) error {
	if err := f.tick("SavePromotion"); err != nil {
		return err
	}
	return f.MemoryStore.SavePromotion(p)
}

func (f *faultStore) AppendAudit(rec GCRecord) error {
	if err := f.tick("AppendAudit"); err != nil {
		return err
	}
	return f.MemoryStore.AppendAudit(rec)
}

func TestPromotionRollbackOnPersistFailure(t *testing.T) {
	for _, failAt := range []string{"DeleteEntry", "SaveEvictionDecision", "SavePromotion", "AppendAudit"} {
		t.Run(failAt, func(t *testing.T) {
			store := newFaultStore()
			c := newTestCache(t, store, NewFakeClock(time.Now()), time.Hour)
			if _, err := c.SetNamespaceQuota("src", 0); err != nil {
				t.Fatal(err)
			}
			if _, err := c.SetNamespaceQuota("dst", 30); err != nil {
				t.Fatal(err)
			}
			a := []byte("aaaaaaaaaa")
			b := []byte("bbbbbbbbbb")
			srcData := make([]byte, 20)
			publishIn(t, c, "dst", "a", a, 10)
			publishIn(t, c, "dst", "b", b, 10)
			pub := publishIn(t, c, "src", "art", srcData, 10)

			if _, err := c.CreatePromotion(PromoteOptions{
				SourceNamespace: "src", SourceKey: "art",
				TargetNamespace: "dst", TargetKey: "k", Mode: PromoteCopy, RequestID: "rb-1",
			}); err != nil {
				t.Fatal(err)
			}

			// 注入故障点：DeleteEntry 第一次调用（删 victim）即失败；
			// 其余方法在晋级落定阶段的相应调用失败。
			switch failAt {
			case "DeleteEntry":
				store.arm("DeleteEntry", 1)
			case "SaveEvictionDecision":
				// 第一次 SaveEvictionDecision 是 plan 阶段（CreatePromotion 已发生），
				// 因此让第二次失败。
				store.arm("SaveEvictionDecision", 2)
			case "SavePromotion":
				store.arm("SavePromotion", 2) // 第一次是 create 落盘
			case "AppendAudit":
				// 第一次 AppendAudit 是 plan 阶段（CreatePromotion），提交审计是第二次。
				store.arm("AppendAudit", 2)
			}

			if _, err := c.CommitPromotion("rb-1"); !errors.Is(err, errInjected) {
				// DeleteEntry 故障经回滚包装后仍应能识别为中止错误；
				// 这里要求提交确实失败（非 nil）即可。
				if err == nil {
					t.Fatalf("expected commit failure at %s", failAt)
				}
			}

			// 两端原状：目标不存在；victim a 仍在且内容一致；来源完好。
			if _, err := c.Read("dst", "k"); !errors.Is(err, ErrEntryNotFound) {
				t.Fatalf("[%s] target leaked after failed commit", failAt)
			}
			if got := readNS(t, c, "dst", "a"); !bytes.Equal(got, a) {
				t.Fatalf("[%s] victim not restored: %q", failAt, got)
			}
			if got := readNS(t, c, "dst", "b"); !bytes.Equal(got, b) {
				t.Fatalf("[%s] b changed", failAt)
			}
			if got := readNS(t, c, "src", "art"); !bytes.Equal(got, srcData) {
				t.Fatalf("[%s] source changed", failAt)
			}
			// 晋级记录退回 proposed，可在解除故障后重试成功。
			rec, err := c.GetPromotion("rb-1")
			if err != nil {
				t.Fatal(err)
			}
			if rec.Status != PromotionProposed {
				t.Fatalf("[%s] promotion status = %s, want proposed", failAt, rec.Status)
			}
			// 解除故障后重新提交应成功。
			store.failAfter = map[string]int{}
			prom, err := c.CommitPromotion("rb-1")
			if err != nil {
				t.Fatalf("[%s] retry commit: %v", failAt, err)
			}
			if prom.Status != PromotionCommitted {
				t.Fatalf("[%s] retry status = %s", failAt, prom.Status)
			}
			if got := readNS(t, c, "dst", "k"); !bytes.Equal(got, srcData) {
				t.Fatalf("[%s] target content after retry", failAt)
			}
			// 共享块仍在。
			if _, err := store.BlobSize(pub.Entry.Chunks[0].Digest); err != nil {
				t.Fatalf("[%s] shared blob lost: %v", failAt, err)
			}
		})
	}
}

// ---- 并发：多个晋级与发布对打，配额不被突破、存活内容完整 ----

func TestPromotionConcurrent(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), 24*time.Hour)
	if _, err := c.SetNamespaceQuota("src", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetNamespaceQuota("dst", 1000); err != nil {
		t.Fatal(err)
	}
	const goroutines = 8
	const iterations = 10
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("promo-concurrent-g%02d-i%02d!!", g, i))
				if len(data) > 40 {
					data = data[:40]
				}
				for len(data) < 40 {
					data = append(data, '!')
				}
				srcKey := fmt.Sprintf("s-%02d-%02d", g, i)
				publishIn(t, c, "src", srcKey, data, 20)
				_, err := c.Promote(PromoteOptions{
					SourceNamespace: "src", SourceKey: srcKey,
					TargetNamespace: "dst", TargetKey: "t-" + srcKey,
					Mode: PromoteCopy, RequestID: fmt.Sprintf("req-%d-%d", g, i),
				})
				if err != nil {
					t.Errorf("promote %s: %v", srcKey, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	entries, err := c.store.ListEntries()
	if err != nil {
		t.Fatal(err)
	}
	var used int64
	for _, e := range entries {
		if e.Namespace != "dst" {
			continue
		}
		used += e.TotalSize
		r, err := c.Read("dst", e.Key)
		if err != nil {
			t.Fatalf("read %s: %v", e.Key, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if int64(len(got)) != e.TotalSize || got[0] != 'p' {
			t.Fatalf("entry %s corrupted (len=%d)", e.Key, len(got))
		}
	}
	if used > 1000 {
		t.Fatalf("dst quota violated: used=%d", used)
	}
}
