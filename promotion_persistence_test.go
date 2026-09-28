package buildcache

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// FileStore：晋级记录、共享块在重启后可恢复；proposed 请求在重启后仍可提交或失败。
func TestPromotionFileStorePersistence(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClock(base)

	data := []byte("promotion persist across restart!!!") // 36
	key := "art"

	// 第一轮：注册命名空间、发布来源、建立并提交一个 replace 晋级，
	// 再建立一个 proposed 晋级（提交前状态持久化）。
	var firstRequestID = "persist-committed"
	var secondRequestID = "persist-proposed"
	func() {
		store, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := New(store, clk, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.SetNamespaceQuota("src", 0); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SetNamespaceQuota("dst", 0); err != nil {
			t.Fatal(err)
		}
		pub := publishData(t, c, "src", key, data, 9)

		prom, err := c.Promote(PromoteOptions{
			SourceNamespace: "src", SourceKey: key,
			ExpectedDigest:  pub.Entry.Digest,
			TargetNamespace: "dst", TargetKey: "k",
			Mode: PromoteCopy, RequestID: firstRequestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if prom.Status != PromotionCommitted {
			t.Fatalf("first promotion = %+v", prom)
		}

		// 第二个请求只建立不提交：重启后应仍是 proposed。
		p2, err := c.CreatePromotion(PromoteOptions{
			SourceNamespace: "src", SourceKey: key,
			TargetNamespace: "dst", TargetKey: "k2",
			Mode: PromoteCopy, RequestID: secondRequestID,
		})
		if err != nil || p2.Status != PromotionProposed {
			t.Fatalf("second create = %+v, %v", p2, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// 第二轮：重启。
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, clk, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 已提交的晋级记录恢复，目标内容完整，且与来源共享块。
	p1, err := c2.GetPromotion(firstRequestID)
	if err != nil {
		t.Fatalf("get committed promotion: %v", err)
	}
	if p1.Status != PromotionCommitted || p1.ResultEntry.Version != 1 {
		t.Fatalf("p1 = %+v", p1)
	}
	r, err := c2.Read("dst", "k")
	if err != nil {
		t.Fatalf("read target after restart: %v", err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("target content differs after restart")
	}

	// 两端条目关系恢复（此时只有第一个请求已提交）。
	links, err := c2.PromotionLinks("src", key)
	if err != nil || len(links) != 1 {
		t.Fatalf("links = %+v, %v", links, err)
	}

	// proposed 的第二个请求仍可提交成功（时钟未推进，状态未漂移）。
	p2, err := c2.CommitPromotion(secondRequestID)
	if err != nil {
		t.Fatalf("commit proposed after restart: %v", err)
	}
	if p2.Status != PromotionCommitted {
		t.Fatalf("p2 = %+v", p2)
	}
	if got := readNS(t, c2, "dst", "k2"); !bytes.Equal(got, data) {
		t.Fatalf("second target content bad")
	}

	// 第二个请求提交后，两端关系变为两条。
	links, err = c2.PromotionLinks("src", key)
	if err != nil || len(links) != 2 {
		t.Fatalf("links after second commit = %+v, %v", links, err)
	}

	// 删除来源后 GC：共享块仍被两个目标引用，不能回收。
	if err := store2.DeleteEntry("src", key); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	r2, err := c2.Read("dst", "k")
	if err != nil {
		t.Fatalf("target k unreadable after GC: %v", err)
	}
	r2.Close()
	r3, err := c2.Read("dst", "k2")
	if err != nil {
		t.Fatalf("target k2 unreadable after GC: %v", err)
	}
	r3.Close()

	// 列表恢复两条记录。
	ps, err := c2.ListPromotions("")
	if err != nil || len(ps) != 2 {
		t.Fatalf("promotions after restart = %d, %v", len(ps), err)
	}
}

// 重启后已提交晋级的同号重放返回首次结果；配额在重启后变化也不影响已提交结果。
func TestPromotionReplayAfterRestart(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC))
	data := []byte("replay after restart data!!!")
	requestID := "replay-1"
	func() {
		store, _ := NewFileStore(dir)
		c, _ := New(store, clk, time.Minute)
		c.SetNamespaceQuota("src", 0)
		c.SetNamespaceQuota("dst", 0)
		pub := publishData(t, c, "src", "art", data, 7)
		if _, err := c.Promote(PromoteOptions{
			SourceNamespace: "src", SourceKey: "art", ExpectedDigest: pub.Entry.Digest,
			TargetNamespace: "dst", TargetKey: "k",
			Mode: PromoteCopy, RequestID: requestID,
		}); err != nil {
			t.Fatal(err)
		}
		store.Close()
	}()
	store2, _ := NewFileStore(dir)
	defer store2.Close()
	c2, _ := New(store2, clk, time.Minute)

	first, err := c2.GetPromotion(requestID)
	if err != nil {
		t.Fatal(err)
	}
	// 即使配额版本变化，已提交结果的同号同内容重放（再 Promote）也返回首次结果。
	c2.SetNamespaceQuota("dst", 10)
	again, err := c2.Promote(PromoteOptions{
		SourceNamespace: "src", SourceKey: "art", ExpectedDigest: digestOf(data),
		TargetNamespace: "dst", TargetKey: "k",
		Mode: PromoteCopy, RequestID: requestID,
	})
	if err != nil {
		t.Fatalf("replay after restart: %v", err)
	}
	if again.ResultEntry != first.ResultEntry || again.CommittedAt != first.CommittedAt {
		t.Fatalf("replay changed result: %+v vs %+v", again, first)
	}
}
