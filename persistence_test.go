package buildcache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// FileStore：崩溃后重启，已发布条目、会话、审计日志都必须可恢复；
// 且过期判定基于持久化的租约时间。
func TestFileStorePersistence(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clk := NewFakeClock(base)

	data := []byte("persist me across restart please")
	key := "mod/github.com/x/y@v1.0.0"
	specs := func() []ChunkSpec { return plan(t, data, 9) }

	// 第一轮：创建、部分上传、发布另一个已完成键。
	func() {
		store, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := New(store, clk, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		open, err := c.CreateSession(CreateSessionOptions{
			Key: "open-key", IdempotencyKey: "idem-open",
			FinalDigest: digestOf(data), TotalSize: int64(len(data)), Chunks: specs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.UploadChunk(open.ID, 0, data[0:9]); err != nil {
			t.Fatal(err)
		}

		done, err := c.CreateSession(CreateSessionOptions{
			Key: key, FinalDigest: digestOf(data),
			TotalSize: int64(len(data)), Chunks: specs(),
		})
		if err != nil {
			t.Fatal(err)
		}
		s := specs()
		for _, sp := range s {
			if _, err := c.UploadChunk(done.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Complete(done.ID, CompleteOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.CollectGarbage(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// 第二轮：重启，状态应完整恢复。
	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, clk, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 已发布条目可读且内容完整。
	r, err := c2.Read(DefaultNamespace, key)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content changed across restart")
	}

	// 未完成会话可继续上传并完成（同一时钟、租约未过期）。
	sessions, err := store2.ListSessions()
	if err != nil || len(sessions) != 1 {
		t.Fatalf("sessions after restart = %d, %v", len(sessions), err)
	}
	openID := sessions[0].ID

	// 幂等索引已从磁盘恢复：同幂等键 + 同参数命中同一会话（必须在完成前验证，
	// 因为完成会释放幂等键）。
	again, err := c2.CreateSession(CreateSessionOptions{
		Key: "open-key", IdempotencyKey: "idem-open",
		FinalDigest: digestOf(data), TotalSize: int64(len(data)), Chunks: specs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != openID {
		t.Fatalf("idempotency index not restored: %s != %s", again.ID, openID)
	}

	s := specs()
	for i := 1; i < len(s); i++ {
		sp := s[i]
		if _, err := c2.UploadChunk(openID, i, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
			t.Fatalf("resume %d after restart: %v", i, err)
		}
	}
	if _, err := c2.Complete(openID, CompleteOptions{}); err != nil {
		t.Fatalf("complete after restart: %v", err)
	}

	// GC 代次已从审计日志恢复：下一次 GC 应为第 2 代。
	rep, err := c2.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Generation != 2 {
		t.Fatalf("GC generation after restart = %d, want 2", rep.Generation)
	}

	// 审计日志在重启后仍可完整读取。
	audit, err := c2.AuditLog()
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) < 3 {
		t.Fatalf("audit log lost across restart: %d records", len(audit))
	}
}

// 重启后，已过期的 open 会话依然死亡，不能借重启复活。
func TestFileStoreExpiredSessionStaysDeadAfterRestart(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clk := NewFakeClock(base)
	data := []byte("expire across restart!!")

	var sid string
	func() {
		store, _ := NewFileStore(dir)
		c, _ := New(store, clk, time.Minute)
		sess, err := c.CreateSession(CreateSessionOptions{
			Key: "k", FinalDigest: digestOf(data),
			TotalSize: int64(len(data)), Chunks: plan(t, data, 8),
		})
		if err != nil {
			t.Fatal(err)
		}
		sid = sess.ID
		store.Close()
	}()

	clk.Advance(2 * time.Minute) // 越过租约
	store, _ := NewFileStore(dir)
	defer store.Close()
	c, _ := New(store, clk, time.Minute)
	if _, err := c.UploadChunk(sid, 0, data[0:8]); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired session must not resurrect after restart, got %v", err)
	}
}

// FileStore 自身的 CAS 语义：不同进程/实例顺序打开同一目录也不能用旧版本覆盖。
func TestFileStoreEntryCAS(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := digestOf([]byte("x"))
	e := Entry{Namespace: DefaultNamespace, Key: "k", Version: 1, Digest: d, TotalSize: 1, PublishedAt: time.Now(), LastAccessedAt: time.Now()}
	if err := s1.PutEntry(e, -1); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 并发新建必须失败。
	if err := s1.PutEntry(e, -1); !errors.Is(err, ErrCASFailed) {
		t.Fatalf("re-create want CAS, got %v", err)
	}
	// 错误版本条件失败。
	e2 := e
	e2.Version = 2
	if err := s1.PutEntry(e2, 5); !errors.Is(err, ErrCASFailed) {
		t.Fatalf("stale CAS want failure, got %v", err)
	}
	// 正确版本条件成功。
	if err := s1.PutEntry(e2, 1); err != nil {
		t.Fatalf("CAS update: %v", err)
	}
	got, err := s1.GetEntry(DefaultNamespace, "k")
	if err != nil || got.Version != 2 {
		t.Fatalf("get = %+v, %v", got, err)
	}

	// 目录中不得残留临时文件。
	tmpHits := 0
	filepath.WalkDir(dir, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if len(de.Name()) >= 5 && de.Name()[:5] == ".tmp-" {
			tmpHits++
		}
		return nil
	})
	if tmpHits != 0 {
		t.Fatalf("temp files leaked: %d", tmpHits)
	}
}

func TestFileStoreBlobIntegrity(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	data := []byte("content addressed")
	d := digestOf(data)
	if err := s.PutBlob(d, data); err != nil {
		t.Fatal(err)
	}
	// 写入与声明摘要不符的数据必须被拒绝。
	if err := s.PutBlob(d, []byte("different")); err == nil {
		t.Fatalf("corrupt put must fail")
	}
	// 重复写相同内容幂等。
	if err := s.PutBlob(d, data); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	sz, err := s.BlobSize(d)
	if err != nil || sz != int64(len(data)) {
		t.Fatalf("size = %d, %v", sz, err)
	}
}

// 重启后命名空间配额、固定租约（含块快照）、淘汰决策、固定请求号都必须恢复。
func TestFileStorePinningStatePersistence(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClock(base)

	data := []byte("persist pins and quota please!!") // 30
	specs := plan(t, data, 10)

	func() {
		store, err := NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := New(store, clk, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.SetNamespaceQuota("team-a", 1234); err != nil {
			t.Fatal(err)
		}
		sess, err := c.CreateSession(CreateSessionOptions{
			Namespace: "team-a", Key: "k", FinalDigest: digestOf(data),
			TotalSize: int64(len(data)), Chunks: specs,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, sp := range specs {
			if _, err := c.UploadChunk(sess.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Complete(sess.ID, CompleteOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Pin(PinOptions{
			Namespace: "team-a", Key: "k", TTL: time.Hour, RequestID: "req-persist",
		}); err != nil {
			t.Fatal(err)
		}
		// 一个已提交的淘汰决策（手动建立后立刻提交，候选被删与否都应持久化）。
		if _, err := c.PlanEviction("team-a", 1); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	store2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	c2, err := New(store2, clk, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 配额恢复。
	st, err := c2.NamespaceStatus("team-a")
	if err != nil {
		t.Fatalf("namespace after restart: %v", err)
	}
	if st.MaxBytes != 1234 || st.UsedBytes != int64(len(data)) || st.PinnedKeys != 1 {
		t.Fatalf("namespace status after restart = %+v", st)
	}

	// 固定租约恢复：仍 active，快照块齐全。
	pin, err := c2.GetPin("team-a", "k")
	if err != nil {
		t.Fatalf("pin after restart: %v", err)
	}
	if pin.Version != 1 || !pin.Active(clk.Now()) || len(pin.Chunks) != len(specs) {
		t.Fatalf("pin after restart = %+v", pin)
	}

	// 请求号索引恢复：同号同参数返回原结果而非新建版本。
	again, err := c2.Pin(PinOptions{
		Namespace: "team-a", Key: "k", TTL: time.Hour, RequestID: "req-persist",
	})
	if err != nil {
		t.Fatalf("idempotent pin replay after restart: %v", err)
	}
	if again.Version != 1 {
		t.Fatalf("pin replay version = %d, want 1", again.Version)
	}

	// 固定快照仍保护块：GC 不删除任何已固定引用的块。
	report, err := c2.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if len(report.DeletedBlobs) != 0 {
		t.Fatalf("pinned blobs deleted after restart: %+v", report.DeletedBlobs)
	}

	// 淘汰决策恢复，且新决策序号在历史最大值之后继续递增。
	ds, err := c2.ListEvictionDecisions("team-a")
	if err != nil || len(ds) != 1 {
		t.Fatalf("eviction decisions after restart = %d, %v", len(ds), err)
	}
	next, err := c2.PlanEviction("team-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID <= ds[0].ID {
		t.Fatalf("new decision id %s must follow %s", next.ID, ds[0].ID)
	}
}

// 旧版（无命名空间）扁平条目文件在打开存储时迁移到默认命名空间。
func TestFileStoreMigratesLegacyEntries(t *testing.T) {
	dir := t.TempDir()
	d := digestOf([]byte("legacy"))
	// 手工写出 001 版本布局 entries/<escaped-key>.json（无 namespace 字段）。
	old := `{
  "key": "old/key",
  "version": 1,
  "digest": "` + d.String() + `",
  "total_size": 6,
  "chunks": [{"index":0,"size":6,"digest":"` + d.String() + `"}],
  "published_at": "2026-01-01T00:00:00Z"
}`
	if err := os.MkdirAll(filepath.Join(dir, "entries"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "entries", "old%2Fkey.json"), []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open with legacy entry: %v", err)
	}
	defer store.Close()
	e, err := store.GetEntry(DefaultNamespace, "old/key")
	if err != nil {
		t.Fatalf("legacy entry must be readable under default namespace: %v", err)
	}
	if e.Namespace != DefaultNamespace || e.Version != 1 {
		t.Fatalf("migrated entry = %+v", e)
	}
	// 旧扁平文件已被移走。
	if _, err := os.Stat(filepath.Join(dir, "entries", "old%2Fkey.json")); !os.IsNotExist(err) {
		t.Fatalf("legacy file should be removed after migration")
	}
}
