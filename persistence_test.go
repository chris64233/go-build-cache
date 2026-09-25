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
	r, err := c2.Read(key)
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
	e := Entry{Key: "k", Version: 1, Digest: d, TotalSize: 1, PublishedAt: time.Now()}
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
	got, err := s1.GetEntry("k")
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
