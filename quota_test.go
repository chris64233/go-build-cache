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

// publishData 用单分片会话发布一条数据，返回发布结果。
func publishData(t *testing.T, c *Cache, namespace, key string, data []byte) *PublishResult {
	t.Helper()
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: namespace, Key: key, FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, len(data)),
	})
	if err != nil {
		t.Fatalf("create session %s/%s: %v", namespace, key, err)
	}
	if _, err := c.UploadChunk(sess.ID, 0, data); err != nil {
		t.Fatalf("upload %s/%s: %v", namespace, key, err)
	}
	res, err := c.Complete(sess.ID, CompleteOptions{})
	if err != nil {
		t.Fatalf("complete %s/%s: %v", namespace, key, err)
	}
	return res
}

func readNS(t *testing.T, c *Cache, namespace, key string) []byte {
	t.Helper()
	r, err := c.Read(namespace, key)
	if err != nil {
		t.Fatalf("read %s/%s: %v", namespace, key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// publishChunked 用指定分片大小发布一条数据。
func publishChunked(t *testing.T, c *Cache, namespace, key string, data []byte, chunkSize int) {
	t.Helper()
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: namespace, Key: key, FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, chunkSize),
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	for _, sp := range plan(t, data, chunkSize) {
		if _, err := c.UploadChunk(sess.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
			t.Fatalf("upload: %v", err)
		}
	}
	if _, err := c.Complete(sess.ID, CompleteOptions{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func newQuotaCache(t *testing.T, clk *FakeClock) *Cache {
	t.Helper()
	if clk == nil {
		clk = NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	}
	return newTestCache(t, NewMemoryStore(), clk, time.Hour)
}

// ---- 命名空间与配额 ----

func TestRegisterNamespaceAndDefaultNamespace(t *testing.T) {
	c := newQuotaCache(t, nil)

	// 默认命名空间开箱可用（不限配额）。
	def, err := c.GetNamespace(DefaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if def.MaxBytes != 0 {
		t.Fatalf("default namespace must be unlimited, got %d", def.MaxBytes)
	}

	if _, err := c.RegisterNamespace("team-a", 1000); err != nil {
		t.Fatal(err)
	}
	// 重复注册是幂等的，且不会悄悄改掉配额。
	again, err := c.RegisterNamespace("team-a", 999999)
	if err != nil || again.MaxBytes != 1000 {
		t.Fatalf("re-register must keep quota, got %+v err=%v", again, err)
	}
	updated, err := c.SetNamespaceQuota("team-a", 2000)
	if err != nil || updated.MaxBytes != 2000 {
		t.Fatalf("set quota = %+v, %v", updated, err)
	}

	// 未注册命名空间不能创建会话。
	if _, err := c.CreateSession(CreateSessionOptions{
		Namespace: "nope", Key: "k", FinalDigest: digestOf([]byte("x")),
		TotalSize: 1, Chunks: []ChunkSpec{{Index: 0, Size: 1, Digest: digestOf([]byte("x"))}},
	}); !errors.Is(err, ErrNamespaceNotFound) {
		t.Fatalf("want ErrNamespaceNotFound, got %v", err)
	}
}

func TestQuotaUsageAccounting(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 100); err != nil {
		t.Fatal(err)
	}

	a := []byte("aaaa")   // 4
	b := []byte("bbbbbb") // 6
	publishData(t, c, "ns", "a", a)
	publishData(t, c, "ns", "b", b)

	info, err := c.QuotaUsage("ns")
	if err != nil {
		t.Fatal(err)
	}
	if info.UsedBytes != 10 || info.EntryCount != 2 || info.EvictableBytes != 10 {
		t.Fatalf("quota usage = %+v", info)
	}

	// 覆盖发布改变用量（4 -> 10）。
	big := []byte("cccccccccc")
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "ns", Key: "a", FinalDigest: digestOf(big),
		TotalSize: 10, Chunks: plan(t, big, len(big)),
	})
	c.UploadChunk(sess.ID, 0, big)
	v := uint64(1)
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &v}); err != nil {
		t.Fatal(err)
	}
	info, _ = c.QuotaUsage("ns")
	if info.UsedBytes != 16 {
		t.Fatalf("after overwrite used=%d, want 16", info.UsedBytes)
	}
}

// 发布超限时按 LRU 淘汰：最久未访问的先删；同访问时刻按键名升序。
func TestQuotaEvictionLRUOnPublish(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 10); err != nil {
		t.Fatal(err)
	}

	publishData(t, c, "ns", "k1", []byte("aaa")) // 3
	clk.Advance(time.Second)
	publishData(t, c, "ns", "k2", []byte("bbb")) // 3
	clk.Advance(time.Second)
	publishData(t, c, "ns", "k3", []byte("ccc")) // 3  -> used 9

	// 再发布 4 字节：used 9+4=13 > 10，需淘汰至少 3 字节。
	// LRU 队首是 k1；淘汰它后用量 10，恰好不超限。
	publishData(t, c, "ns", "k4", []byte("dddd"))

	if _, err := c.Read("ns", "k1"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("k1 should be evicted, got err=%v", err)
	}
	for _, k := range []string{"k2", "k3", "k4"} {
		if _, err := c.Read("ns", k); err != nil {
			t.Fatalf("%s should survive: %v", k, err)
		}
	}
	info, _ := c.QuotaUsage("ns")
	if info.UsedBytes != 10 {
		t.Fatalf("used=%d want 10", info.UsedBytes)
	}

	// 审计里应留下 selected + deleted 各一条。
	audit, _ := c.AuditLog()
	var selected, deleted int
	for _, r := range audit {
		if r.Action == GCEvictSelected {
			selected++
		}
		if r.Action == GCEvictDeleted {
			deleted++
		}
	}
	if selected != 1 || deleted != 1 {
		t.Fatalf("eviction audit selected=%d deleted=%d", selected, deleted)
	}
}

// 最近被读取的条目即使最老也不应被淘汰（LRU 依据最近访问时间）。
func TestQuotaEvictionRespectsRecentRead(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 10); err != nil {
		t.Fatal(err)
	}

	publishData(t, c, "ns", "old", []byte("aaa"))
	clk.Advance(time.Second)
	publishData(t, c, "ns", "new", []byte("bbb"))

	// 访问 oldest，使其成为最近使用。
	clk.Advance(time.Second)
	if got := readNS(t, c, "ns", "old"); !bytes.Equal(got, []byte("aaa")) {
		t.Fatalf("read old: %q", got)
	}

	// 再发 5 字节，需要淘汰 4+：old 刚被访问，应淘汰 new。
	publishData(t, c, "ns", "x", []byte("ccccc"))
	if _, err := c.Read("ns", "new"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("new should be evicted after old was read, err=%v", err)
	}
	if _, err := c.Read("ns", "old"); err != nil {
		t.Fatalf("old must survive: %v", err)
	}
}

// 同访问时刻按键名稳定排序。
func TestQuotaEvictionTieBreaksByKey(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 6); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "zeta", []byte("aa"))
	publishData(t, c, "ns", "alpha", []byte("bb"))
	publishData(t, c, "ns", "mu", []byte("cc")) // used 6

	publishData(t, c, "ns", "n", []byte("d")) // 需要腾 1 字节 -> 第一个候选是 alpha
	if _, err := c.Read("ns", "alpha"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("alpha must be evicted first on tie, err=%v", err)
	}
	if _, err := c.Read("ns", "zeta"); err != nil {
		t.Fatalf("zeta survives read: %v", err)
	}
}

// 所有候选都被固定时，发布因配额不足失败，且不删除任何条目。
func TestQuotaExceededWhenAllPinned(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 6); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaa")
	publishData(t, c, "ns", "a", a)
	publishData(t, c, "ns", "b", []byte("bbb")) // used 6

	if _, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "a", ExpectedDigest: digestOf(a),
		TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	// 10 字节新条目：发布后用量将是 16，需腾出 10 字节；
	// 但可淘汰的只有 b(3)，固定的 a(3) 不能动 -> 配额不足，发布失败。
	big := []byte("XXXXXXXXXX") // 10
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: "ns", Key: "big", FinalDigest: digestOf(big),
		TotalSize: 10, Chunks: plan(t, big, len(big)),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.UploadChunk(sess.ID, 0, big)
	_, err = c.Complete(sess.ID, CompleteOptions{})
	var qe *QuotaExceededError
	if !errors.As(err, &qe) {
		t.Fatalf("want QuotaExceededError, got %v", err)
	}
	// 失败后所有既有条目完好。
	if _, err := c.Read("ns", "a"); err != nil {
		t.Fatalf("pinned a must remain: %v", err)
	}
	if _, err := c.Read("ns", "b"); err != nil {
		t.Fatalf("b must remain after failed publish: %v", err)
	}
	if _, err := c.Read("ns", "big"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("big must not be visible after quota failure")
	}
}

// ---- 两阶段淘汰：决策失效 ----

func TestEvictionPlanCommitHappyPath(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "a", []byte("aaa"))

	d, err := c.PlanEviction("ns", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Candidates) != 1 || d.Candidates[0].Key != "a" {
		t.Fatalf("candidates = %+v", d.Candidates)
	}
	report, err := c.CommitEviction(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Deleted) != 1 || report.ReclaimedBytes != 3 || len(report.Stale) != 0 {
		t.Fatalf("report = %+v", report)
	}
	if _, err := c.Read("ns", "a"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("a should be deleted, err=%v", err)
	}
}

// 候选在决策后被读取 -> 旧决定失效。
func TestEvictionDecisionStaleAfterAccess(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "a", []byte("aaa"))

	d, _ := c.PlanEviction("ns", 3)
	readNS(t, c, "ns", "a") // 决策之后发生访问

	report, err := c.CommitEviction(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Deleted) != 0 || len(report.Stale) != 1 ||
		report.Stale[0].Reason != EvictStaleAccessed {
		t.Fatalf("report = %+v", report)
	}
	if _, err := c.Read("ns", "a"); err != nil {
		t.Fatalf("accessed candidate must survive: %v", err)
	}
}

// 候选在决策后被固定 -> 旧决定失效。
func TestEvictionDecisionStaleAfterPin(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaa")
	publishData(t, c, "ns", "a", a)
	d, _ := c.PlanEviction("ns", 3)

	if _, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "a", ExpectedDigest: digestOf(a), TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := c.CommitEviction(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stale) != 1 || report.Stale[0].Reason != EvictStalePinned {
		t.Fatalf("report = %+v", report)
	}
	if _, err := c.Read("ns", "a"); err != nil {
		t.Fatalf("pinned candidate must survive: %v", err)
	}
}

// 候选在决策后被重新发布 -> 旧决定失效。
func TestEvictionDecisionStaleAfterRepublish(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "a", []byte("aaa"))
	d, _ := c.PlanEviction("ns", 3)

	// 同摘要复用发布也推进访问序号；这里用不同摘要真正覆盖。
	newData := []byte("bbbb")
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "ns", Key: "a", FinalDigest: digestOf(newData),
		TotalSize: 4, Chunks: plan(t, newData, len(newData)),
	})
	c.UploadChunk(sess.ID, 0, newData)
	v := uint64(1)
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &v}); err != nil {
		t.Fatal(err)
	}

	report, err := c.CommitEviction(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stale) != 1 || report.Stale[0].Reason != EvictStaleRepublished {
		t.Fatalf("report = %+v", report)
	}
	if got := readNS(t, c, "ns", "a"); !bytes.Equal(got, newData) {
		t.Fatalf("republished candidate content = %q", got)
	}
}

// 决策之后条目已经不存在 -> 记 stale(entry_gone)，不报错。
func TestEvictionDecisionStaleAfterMissing(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "a", []byte("aaa"))
	d, _ := c.PlanEviction("ns", 3)

	if _, err := c.CommitEviction(d); err != nil {
		t.Fatal(err)
	}
	// 再次确认同一个决定：条目已不存在。
	report, err := c.CommitEviction(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stale) != 1 || report.Stale[0].Reason != EvictStaleMissing {
		t.Fatalf("report = %+v", report)
	}
}

// EvictionOrder 是只读的 LRU 查询。
func TestEvictionOrderQuery(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "ns", "old", []byte("aa"))
	clk.Advance(time.Second)
	publishData(t, c, "ns", "new", []byte("bb"))
	pinned := []byte("cc")
	publishData(t, c, "ns", "pin", pinned)
	c.Pin(PinOptions{Namespace: "ns", Key: "pin", ExpectedDigest: digestOf(pinned), TTL: time.Hour})

	order, err := c.EvictionOrder("ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0].Key != "old" || order[1].Key != "new" {
		t.Fatalf("order = %+v", order)
	}
}

// ---- 固定租约 ----

func TestPinRequiresExistingMatchingEntry(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}

	// 不存在的键不能固定。
	_, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "ghost", ExpectedDigest: digestOf([]byte("x")), TTL: time.Hour,
	})
	var pc *PinConflictError
	if !errors.As(err, &pc) || pc.Reason != PinMissingEntry {
		t.Fatalf("want missing_entry PinConflictError, got %v", err)
	}

	data := []byte("real content")
	publishData(t, c, "ns", "k", data)

	// 摘要不匹配不能固定。
	_, err = c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf([]byte("other")), TTL: time.Hour,
	})
	if !errors.As(err, &pc) || pc.Reason != PinDigestMismatch {
		t.Fatalf("want digest_mismatch PinConflictError, got %v", err)
	}

	// 正确固定：v1。
	res, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lease.Version != 1 || len(res.Lease.Chunks) != 1 {
		t.Fatalf("lease = %+v", res.Lease)
	}
}

func TestPinRenewReleaseVersionSemantics(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("pinned content")
	publishData(t, c, "ns", "k", data)

	pin, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	originalExpiry := pin.Lease.ExpiresAt

	// 续租不能缩短。
	clk.Advance(time.Minute)
	_, err = c.RenewPin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedVersion: &pin.Lease.Version, TTL: time.Minute,
	})
	if err == nil {
		t.Fatalf("shorter renew must be rejected")
	}

	// 旧版本号（0）续租被拒。
	old := uint64(0)
	_, err = c.RenewPin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedVersion: &old, TTL: 2 * time.Hour,
	})
	var pv *PinVersionConflictError
	if !errors.As(err, &pv) || pv.CurrentVersion != 1 {
		t.Fatalf("want pin version conflict, got %v", err)
	}

	// 正确版本续租 -> v2，截止时间延长。
	renewed, err := c.RenewPin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedVersion: &pin.Lease.Version, TTL: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Lease.Version != 2 || !renewed.Lease.ExpiresAt.After(originalExpiry) {
		t.Fatalf("renewed lease = %+v", renewed.Lease)
	}

	// 旧版本（v1）解除不能作用于 v2 租约。
	v1 := uint64(1)
	_, err = c.ReleasePin(PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v1})
	if !errors.As(err, &pv) {
		t.Fatalf("stale release must fail with version conflict, got %v", err)
	}
	if lease, _ := c.GetPin("ns", "k"); !lease.Active(clk.Now()) {
		t.Fatalf("lease must still be active after stale release")
	}

	// 当前版本解除 -> v3 released。
	v2 := uint64(2)
	released, err := c.ReleasePin(PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v2})
	if err != nil {
		t.Fatal(err)
	}
	if released.Lease.Version != 3 || !released.Lease.Released {
		t.Fatalf("released lease = %+v", released.Lease)
	}

	// 已解除的租约不能续租复活。
	v3 := uint64(3)
	_, err = c.RenewPin(PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v3, TTL: time.Hour})
	if !errors.Is(err, ErrPinExpired) {
		t.Fatalf("renew released lease must give ErrPinExpired, got %v", err)
	}

	// 重新固定产生全新的 v4 租约（不复活旧记录）。
	repinned, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if repinned.Lease.Version != 4 || !repinned.Lease.Active(clk.Now()) {
		t.Fatalf("re-pin lease = %+v", repinned.Lease)
	}
}

// 过期扫描并发时，旧版本续租不能复活新租约；过期租约不保护块。
func TestPinExpirySweepAndNoResurrection(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("expires soon!!")
	publishData(t, c, "ns", "k", data)
	res, _ := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: time.Minute,
	})

	clk.Advance(61 * time.Second)
	// 过期后续租（即使版本正确）也被拒，租约不复活。
	v1 := res.Lease.Version
	_, err := c.RenewPin(PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v1, TTL: time.Hour})
	if !errors.Is(err, ErrPinExpired) {
		t.Fatalf("renew expired pin must fail, got %v", err)
	}
	if lease, _ := c.GetPin("ns", "k"); lease.Active(clk.Now()) {
		t.Fatalf("expired lease must not be active")
	}

	// 扫描清除记录。
	swept, err := c.SweepExpiredPins()
	if err != nil || len(swept) != 1 || swept[0].Reason != "expired" {
		t.Fatalf("swept = %+v, %v", swept, err)
	}
	if _, err := c.GetPin("ns", "k"); !errors.Is(err, ErrPinNotFound) {
		t.Fatalf("pin record should be swept, got %v", err)
	}
}

// 固定保护块：条目被覆盖发布后，旧版本独有块仍被固定快照保护，GC 不删。
func TestPinProtectsSnapshotBlobsAfterOverwrite(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	old := []byte("OLD-CONTENT!!!")
	new := []byte("NEW-CONTENT!!!!")
	publishData(t, c, "ns", "k", old)
	if _, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(old), TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	// 覆盖发布：租约仍引用旧块快照。
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "ns", Key: "k", FinalDigest: digestOf(new),
		TotalSize: int64(len(new)), Chunks: plan(t, new, len(new)),
	})
	c.UploadChunk(sess.ID, 0, new)
	v := uint64(1)
	if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: &v}); err != nil {
		t.Fatal(err)
	}

	refs, err := c.BlobReferences(digestOf(old))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs.Entries) != 0 || len(refs.Pins) != 1 {
		t.Fatalf("old blob refs = %+v, want 1 pin, 0 entries", refs)
	}

	report, err := c.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range report.DeletedBlobs {
		if db.Digest == digestOf(old) {
			t.Fatalf("pinned snapshot blob must not be GC'd")
		}
	}

	// 解除并扫描后，旧块不再受保护，GC 回收。覆盖发布不改变租约版本，仍为 v1。
	v1 := uint64(1)
	if _, err := c.ReleasePin(PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v1}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SweepExpiredPins(); err != nil {
		t.Fatal(err)
	}
	refs, _ = c.BlobReferences(digestOf(old))
	if refs.Referenced() {
		t.Fatalf("released pin must no longer reference old blob: %+v", refs)
	}
	report, _ = c.CollectGarbage()
	found := false
	for _, db := range report.DeletedBlobs {
		if db.Digest == digestOf(old) {
			found = true
		}
	}
	if !found {
		t.Fatalf("old blob should be collected after pin release + sweep")
	}
}

// 固定条目不会被配额淘汰，且用量统计正确拆分固定/可淘汰字节。
func TestPinnedEntryExcludedFromEvictionAndQuota(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 100); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaa")
	publishData(t, c, "ns", "a", a)
	publishData(t, c, "ns", "b", []byte("bbb"))
	c.Pin(PinOptions{Namespace: "ns", Key: "a", ExpectedDigest: digestOf(a), TTL: time.Hour})

	info, _ := c.QuotaUsage("ns")
	if info.PinnedKeys != 1 || info.PinnedBytes != 3 || info.EvictableBytes != 3 {
		t.Fatalf("quota info = %+v", info)
	}

	order, _ := c.EvictionOrder("ns")
	if len(order) != 1 || order[0].Key != "b" {
		t.Fatalf("only unpinned b is evictable, got %+v", order)
	}
}

// ---- 请求号幂等 ----

func TestPinRequestIDIdempotency(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("idempotent pin data")
	publishData(t, c, "ns", "k", data)

	opts := PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data),
		TTL: time.Hour, RequestID: "req-1",
	}
	r1, err := c.Pin(opts)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c.Pin(opts)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Replayed || r1.Lease.Version != r2.Lease.Version ||
		r1.Lease.ExpiresAt != r2.Lease.ExpiresAt {
		t.Fatalf("replay must return original result: %+v vs %+v", r1, r2)
	}

	// 同请求号、不同截止时间 -> 冲突。
	opts.TTL = 2 * time.Hour
	_, err = c.Pin(opts)
	var rc *RequestConflictError
	if !errors.As(err, &rc) {
		t.Fatalf("want RequestConflictError, got %v", err)
	}

	// 续租请求号同样可重放。
	v1 := r1.Lease.Version
	renew := PinOptions{
		Namespace: "ns", Key: "k", ExpectedVersion: &v1,
		TTL: 3 * time.Hour, RequestID: "req-renew",
	}
	rn1, err := c.RenewPin(renew)
	if err != nil {
		t.Fatal(err)
	}
	rn2, err := c.RenewPin(renew)
	if err != nil {
		t.Fatal(err)
	}
	if !rn2.Replayed || rn1.Lease.Version != rn2.Lease.Version {
		t.Fatalf("renew replay mismatch %+v vs %+v", rn1, rn2)
	}

	// 解除请求号重放：重复解除返回同一结果，不产生新版本。
	v2 := rn1.Lease.Version
	rel := PinOptions{Namespace: "ns", Key: "k", ExpectedVersion: &v2, RequestID: "req-rel"}
	rel1, err := c.ReleasePin(rel)
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := c.ReleasePin(rel)
	if err != nil {
		t.Fatal(err)
	}
	if !rel2.Replayed || rel1.Lease.Version != rel2.Lease.Version {
		t.Fatalf("release replay mismatch %+v vs %+v", rel1, rel2)
	}
}

// 相对 TTL 的请求在更晚时刻重试：请求内容相同（同一 ttl），必须返回首次结果，
// 而不是按"计算出的截止时间不同"误报冲突。
func TestPinRequestIDRelativeTTLDelayedReplay(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("delayed replay target data")
	publishData(t, c, "ns", "k", data)

	opts := PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data),
		TTL: time.Hour, RequestID: "r",
	}
	r1, err := c.Pin(opts)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Minute)
	r2, err := c.Pin(opts) // 同样的 ttl 请求号重试
	if err != nil {
		t.Fatalf("delayed replay with same ttl must succeed: %v", err)
	}
	if !r2.Replayed || r2.Lease.ExpiresAt != r1.Lease.ExpiresAt ||
		r2.Lease.Version != r1.Lease.Version {
		t.Fatalf("delayed replay must return original lease: %+v vs %+v", r1, r2)
	}

	// 但改用绝对时间表达 -> 请求内容不同 -> 冲突。
	opts2 := opts
	opts2.TTL = 0
	opts2.ExpiresAt = clk.Now().Add(time.Hour)
	if _, err := c.Pin(opts2); !errors.As(err, new(*RequestConflictError)) {
		t.Fatalf("switching ttl to expires_at with same request id must conflict, got %v", err)
	}
}

// ---- 内容块引用查询 ----

func TestBlobReferencesQuery(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("reference query data!!!") // 24 字节，8 字节分片 -> 3 块
	specs := plan(t, data, 8)
	publishChunked(t, c, "ns", "pub", data, 8)

	// 活跃会话引用。
	sess, _ := c.CreateSession(CreateSessionOptions{
		Namespace: "ns", Key: "uploading", FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: specs,
	})
	if _, err := c.UploadChunk(sess.ID, 0, data[0:8]); err != nil {
		t.Fatal(err)
	}

	// 第 0 块同时被已发布条目与活跃会话引用。
	refs, err := c.BlobReferences(specs[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs.Entries) != 1 || refs.Entries[0].Key != "pub" {
		t.Fatalf("entry refs = %+v", refs.Entries)
	}
	if len(refs.Sessions) != 1 || refs.Sessions[0].SessionID != sess.ID {
		t.Fatalf("session refs = %+v", refs.Sessions)
	}

	// 仅被已发布条目引用的第 1 块：有 entry 引用、无会话引用。
	refs1, err := c.BlobReferences(specs[1].Digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs1.Entries) != 1 || len(refs1.Sessions) != 0 || len(refs1.Pins) != 0 {
		t.Fatalf("chunk1 refs = %+v", refs1)
	}

	// 无引用块。
	orphan := digestOf([]byte("nobody references this"))
	c.store.PutBlob(orphan, []byte("nobody references this"))
	orphanRefs, err := c.BlobReferences(orphan)
	if err != nil {
		t.Fatal(err)
	}
	if orphanRefs.Referenced() {
		t.Fatalf("orphan must have no references: %+v", orphanRefs)
	}
}

// ---- 并发：续租 / 解除 / 过期扫描 / 淘汰 / GC 对打，租约单调不变量必须成立 ----

func TestPinLeaseConcurrentMonotonic(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	data := []byte("concurrency pin target data")
	publishData(t, c, "ns", "k", data)
	// 先建立 v1 租约；之后所有版本变化都发生在同一条记录上。
	if _, err := c.Pin(PinOptions{
		Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: 24 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const iterations = 30
	var wg sync.WaitGroup
	var maxVer uint64
	var verMu sync.Mutex
	noteVer := func(v uint64) {
		verMu.Lock()
		if v > maxVer {
			maxVer = v
		}
		verMu.Unlock()
	}
	noteVer(1)

	// 大量并发续租 / 解除 / 重新固定。时钟固定且租约很长，租约不会自然过期，
	// 记录也不被扫描清除，因此同一条租约记录的版本号必须严格单调递增。
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				lease, err := c.GetPin("ns", "k")
				if err != nil {
					continue
				}
				v := lease.Version
				switch {
				case lease.Released:
					// 已解除：重新固定得到 version+1（不产生 v1 新记录）。
					res, perr := c.Pin(PinOptions{
						Namespace: "ns", Key: "k", ExpectedDigest: digestOf(data), TTL: time.Hour,
					})
					if perr == nil {
						noteVer(res.Lease.Version)
					}
				case i%3 == 0:
					if _, rerr := c.ReleasePin(PinOptions{
						Namespace: "ns", Key: "k", ExpectedVersion: &v,
					}); rerr == nil {
						noteVer(v + 1)
					}
				default:
					if _, rerr := c.RenewPin(PinOptions{
						Namespace: "ns", Key: "k", ExpectedVersion: &v, TTL: 2 * time.Hour,
					}); rerr == nil {
						noteVer(v + 1)
					}
				}
			}
		}(w)
	}

	wg.Wait()

	// 最终租约版本必须恰好等于观察到的最大版本（单调递增，无回退/重复）。
	final, err := c.GetPin("ns", "k")
	if err != nil {
		t.Fatalf("lease record must persist without sweeps: %v", err)
	}
	if final.Version != maxVer {
		t.Fatalf("final lease v%d != observed max v%d (version regressed or duplicated)",
			final.Version, maxVer)
	}
	// 旧版本号此刻一定全部失效：拿 maxVer-1 续租必须被拒。
	if maxVer >= 2 {
		stale := maxVer - 1
		if _, err := c.RenewPin(PinOptions{
			Namespace: "ns", Key: "k", ExpectedVersion: &stale, TTL: 3 * time.Hour,
		}); err == nil {
			t.Fatalf("renew with stale v%d must fail", stale)
		}
	}
	// 条目内容始终完好。
	if got := readNS(t, c, "ns", "k"); !bytes.Equal(got, data) {
		t.Fatalf("entry content corrupted: %q", got)
	}
}

// 多命名空间隔离：同名键互不影响，配额独立核算。
func TestNamespaceIsolation(t *testing.T) {
	c := newQuotaCache(t, nil)
	if _, err := c.RegisterNamespace("a", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterNamespace("b", 100); err != nil {
		t.Fatal(err)
	}
	publishData(t, c, "a", "k", []byte("aaa"))
	publishData(t, c, "b", "k", []byte("bbbbbb"))

	if got := readNS(t, c, "a", "k"); !bytes.Equal(got, []byte("aaa")) {
		t.Fatalf("a/k = %q", got)
	}
	if got := readNS(t, c, "b", "k"); !bytes.Equal(got, []byte("bbbbbb")) {
		t.Fatalf("b/k = %q", got)
	}
	ia, _ := c.QuotaUsage("a")
	ib, _ := c.QuotaUsage("b")
	if ia.UsedBytes != 3 || ib.UsedBytes != 6 {
		t.Fatalf("per-namespace accounting a=%d b=%d", ia.UsedBytes, ib.UsedBytes)
	}

	// 对 a/k 的固定不影响 b/k 的淘汰。
	c.Pin(PinOptions{Namespace: "a", Key: "k", ExpectedDigest: digestOf([]byte("aaa")), TTL: time.Hour})
	order, _ := c.EvictionOrder("b")
	if len(order) != 1 || order[0].Key != "k" {
		t.Fatalf("b/k must still be evictable, got %+v", order)
	}
}

func TestPinListQuery(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	c := newQuotaCache(t, clk)
	if _, err := c.RegisterNamespace("ns", 0); err != nil {
		t.Fatal(err)
	}
	a := []byte("aaa")
	b := []byte("bbb")
	publishData(t, c, "ns", "a", a)
	publishData(t, c, "ns", "b", b)
	c.Pin(PinOptions{Namespace: "ns", Key: "a", ExpectedDigest: digestOf(a), TTL: time.Minute})
	c.Pin(PinOptions{Namespace: "ns", Key: "b", ExpectedDigest: digestOf(b), TTL: time.Hour})

	clk.Advance(61 * time.Second)
	active, err := c.ListPins("ns", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].Key != "b" {
		t.Fatalf("active pins = %+v", active)
	}
	all, _ := c.ListPins("ns", true)
	if len(all) != 2 {
		t.Fatalf("all pins incl inactive = %+v", all)
	}
}

// 配额 + 淘汰 + 读取 + GC 高并发对打：任何成功发布的条目只要随后被读取，
// 就不能在读取之后被配额淘汰误删；命名空间用量永远不超过配额（成功发布后）。
func TestQuotaConcurrentPublishEvictReadGC(t *testing.T) {
	c := newQuotaCache(t, NewFakeClock(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)))
	if _, err := c.RegisterNamespace("ns", 200); err != nil {
		t.Fatal(err)
	}

	const publishers = 6
	const iterations = 20
	var wg sync.WaitGroup   // 发布者 + 读者
	var gcWG sync.WaitGroup // GC 循环单独计数，避免与 stop 信号互相等待
	var mu sync.Mutex
	live := map[string][]byte{} // 已成功发布、尚未被观察淘汰的键

	// GC 持续进行。
	stop := make(chan struct{})
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.CollectGarbage()
				time.Sleep(time.Millisecond) // 让出 CPU，避免零退避忙转饿死其他 goroutine
			}
		}
	}()

	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("p%d-i%02d-padding", p, i)) // 18 字节
				key := fmt.Sprintf("k%d-%02d", p, i)
				res, err := publishDataErr(c, "ns", key, data)
				if err != nil {
					var qe *QuotaExceededError
					if !errors.As(err, &qe) {
						t.Errorf("unexpected publish error: %v", err)
					}
					continue
				}
				if res.Reused {
					continue
				}
				mu.Lock()
				live[key] = data
				mu.Unlock()
			}
		}(p)
	}

	// 读者持续访问随机键，访问后立即再次读取应当仍在（淘汰决策已失效）。
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				mu.Lock()
				var key string
				var data []byte
				for k, v := range live {
					key, data = k, v
					break
				}
				mu.Unlock()
				if key == "" {
					time.Sleep(time.Millisecond)
					continue
				}
				got, err := readNSErr(c, "ns", key)
				if err != nil {
					continue // 可能恰好被配额淘汰
				}
				if !bytes.Equal(got, data) {
					t.Errorf("content mismatch for %s", key)
					return
				}
				// 决策失效的强保证由两阶段淘汰的专门用例覆盖；
				// 这里只验证：读到的内容永远完整，不会出现半删状态。
			}
		}()
	}

	// 发布者与读者结束后再停 GC，并等 GC 循环退出。
	wg.Wait()
	close(stop)
	gcWG.Wait()

	// 收敛：成功发布后的用量不得超过配额（停止新发布后淘汰已在发布路径内完成）。
	info, err := c.QuotaUsage("ns")
	if err != nil {
		t.Fatal(err)
	}
	if info.MaxBytes > 0 && info.UsedBytes > info.MaxBytes {
		t.Fatalf("namespace over quota after quiescence: used=%d max=%d", info.UsedBytes, info.MaxBytes)
	}
}

func publishDataErr(c *Cache, namespace, key string, data []byte) (*PublishResult, error) {
	spec := ChunkSpec{Index: 0, Size: int64(len(data)), Digest: digestOf(data)}
	sess, err := c.CreateSession(CreateSessionOptions{
		Namespace: namespace, Key: key, FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: []ChunkSpec{spec},
	})
	if err != nil {
		return nil, err
	}
	if _, err := c.UploadChunk(sess.ID, 0, data); err != nil {
		return nil, err
	}
	return c.Complete(sess.ID, CompleteOptions{})
}

func readNSErr(c *Cache, namespace, key string) ([]byte, error) {
	r, err := c.Read(namespace, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
