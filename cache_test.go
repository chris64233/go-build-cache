package buildcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---- 测试辅助 ----

func digestOf(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest{Algo: "sha256", Hex: hex.EncodeToString(sum[:])}
}

// plan 把 data 按 chunkSize 切分，返回创建会话所需的分片声明。
func plan(t *testing.T, data []byte, chunkSize int) []ChunkSpec {
	t.Helper()
	var specs []ChunkSpec
	var off int64
	for off < int64(len(data)) {
		end := off + int64(chunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		part := data[off:end]
		specs = append(specs, ChunkSpec{
			Index: len(specs), Offset: off, Size: int64(len(part)), Digest: digestOf(part),
		})
		off = end
	}
	return specs
}

func uploadAll(t *testing.T, c *Cache, sid string, data []byte, chunkSize int, order []int) {
	t.Helper()
	if order == nil {
		order = randishOrder(len(plan(t, data, chunkSize)))
	}
	for _, i := range order {
		off := int64(i * chunkSize)
		if off > int64(len(data)) {
			t.Fatalf("bad offset")
		}
		end := off + int64(chunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if _, err := c.UploadChunk(sid, i, data[off:end]); err != nil {
			t.Fatalf("upload chunk %d: %v", i, err)
		}
	}
}

func randishOrder(n int) []int {
	// 固定的"乱序"排列，避免测试依赖随机性同时覆盖乱序路径。
	order := make([]int, n)
	for i := range order {
		order[i] = (i*7 + 3) % n
	}
	seen := map[int]bool{}
	out := order[:0]
	for _, x := range order {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func readAll(t *testing.T, c *Cache, key string) []byte {
	t.Helper()
	r, err := c.Read(key)
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("readall %s: %v", key, err)
	}
	return data
}

func newTestCache(t *testing.T, store Store, clk *FakeClock, ttl time.Duration) *Cache {
	t.Helper()
	c, err := New(store, clk, ttl)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	return c
}

// ---- 基础 happy path：乱序上传、重传幂等、完成前不可见、原子发布 ----

func TestHappyPathOutOfOrderAndDuplicateUpload(t *testing.T) {
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := newTestCache(t, NewMemoryStore(), clk, time.Hour)

	data := []byte("the quick brown fox jumps over the lazy dog")
	key := "artifacts/abc"
	specs := plan(t, data, 10)

	sess, err := c.CreateSession(CreateSessionOptions{
		Key: key, FinalDigest: digestOf(data), TotalSize: int64(len(data)), Chunks: specs,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 完成前读者看不到数据。
	if _, err := c.Read(key); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("want ErrEntryNotFound before complete, got %v", err)
	}

	// 乱序上传。
	uploadAll(t, c, sess.ID, data, 10, nil)

	// 相同内容重传：幂等成功。
	rec, err := c.UploadChunk(sess.ID, 0, data[0:10])
	if err != nil {
		t.Fatalf("duplicate upload: %v", err)
	}
	if rec.Digest != specs[0].Digest {
		t.Fatalf("duplicate receipt digest mismatch")
	}

	// 仍然不可见。
	if _, err := c.Read(key); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("data visible before complete")
	}

	res, err := c.Complete(sess.ID, CompleteOptions{})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if res.Reused || res.Entry.Version != 1 {
		t.Fatalf("unexpected publish result: %+v", res)
	}

	got := readAll(t, c, key)
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch: got %q want %q", got, data)
	}
}

func TestCreateSessionValidation(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("hello world")
	specs := plan(t, data, 5)

	cases := []struct {
		name string
		opts func(CreateSessionOptions) CreateSessionOptions
	}{
		{"empty key", func(o CreateSessionOptions) CreateSessionOptions { o.Key = ""; return o }},
		{"bad digest", func(o CreateSessionOptions) CreateSessionOptions { o.FinalDigest = Digest{}; return o }},
		{"non-positive size", func(o CreateSessionOptions) CreateSessionOptions { o.TotalSize = 0; return o }},
		{"size sum mismatch", func(o CreateSessionOptions) CreateSessionOptions { o.TotalSize++; return o }},
		{"gap in chunks", func(o CreateSessionOptions) CreateSessionOptions {
			o.Chunks = append([]ChunkSpec(nil), o.Chunks[:len(o.Chunks)-1]...)
			o.Chunks = append(o.Chunks, ChunkSpec{
				Index: len(o.Chunks), Offset: 999,
				Size: 5, Digest: digestOf(data[5:10]),
			})
			return o
		}},
		{"duplicate index", func(o CreateSessionOptions) CreateSessionOptions {
			o.Chunks[1].Index = 0
			return o
		}},
	}
	base := CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data), TotalSize: int64(len(data)), Chunks: specs,
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.CreateSession(tc.opts(base)); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

// ---- 分片冲突：同号不同内容 ----

func TestChunkContentConflict(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("0123456789") // 两个分片各 5 字节
	specs := plan(t, data, 5)
	sess, err := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data), TotalSize: 10, Chunks: specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadChunk(sess.ID, 0, data[0:5]); err != nil {
		t.Fatal(err)
	}
	// 同号不同内容：已存在回执 -> 冲突。
	bad := []byte("xxxxx")
	_, err = c.UploadChunk(sess.ID, 0, bad)
	var cc *ChunkConflictError
	if !errors.As(err, &cc) || cc.Reason != ReasonContentMismatch {
		t.Fatalf("want content ChunkConflictError, got %v", err)
	}
	if cc.Existing != specs[0].Digest || cc.Got != digestOf(bad) {
		t.Fatalf("conflict digests wrong: %+v", cc)
	}

	// 尚未上传过的分片上传了与声明摘要不符的内容 -> 摘要错误。
	_, err = c.UploadChunk(sess.ID, 1, bad)
	var dm *DigestMismatchError
	if !errors.As(err, &dm) {
		t.Fatalf("want DigestMismatchError, got %v", err)
	}
}

// 直接构造"会话内同号两次不同合法内容"的冲突：两个不同会话声明同一分片号但摘要不同，
// 用于验证冲突错误类型的另一种形态——这里通过未声明分片号路径覆盖 ErrChunkNotDeclared。
func TestChunkNotDeclared(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("0123456789")
	sess, err := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data), TotalSize: 10, Chunks: plan(t, data, 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadChunk(sess.ID, 7, data[0:5]); !errors.Is(err, ErrChunkNotDeclared) {
		t.Fatalf("want ErrChunkNotDeclared, got %v", err)
	}
}

func TestChunkSizeMismatch(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("0123456789")
	sess, err := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data), TotalSize: 10, Chunks: plan(t, data, 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.UploadChunk(sess.ID, 0, data[0:4])
	var cc *ChunkConflictError
	if !errors.As(err, &cc) || cc.Reason != ReasonSizeMismatch {
		t.Fatalf("want size ChunkConflictError, got %v", err)
	}
}

// ---- 完成校验：缺片 / 总大小 / 最终摘要 ----

func TestCompleteIncomplete(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("0123456789")
	sess, _ := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data), TotalSize: 10, Chunks: plan(t, data, 5),
	})
	if _, err := c.UploadChunk(sess.ID, 0, data[0:5]); err != nil {
		t.Fatal(err)
	}
	_, err := c.Complete(sess.ID, CompleteOptions{})
	var cc *ChunkConflictError
	if !errors.As(err, &cc) || cc.Reason != ReasonIncomplete {
		t.Fatalf("want incomplete conflict, got %v", err)
	}
	if len(cc.Missing) != 1 || cc.Missing[0] != 1 {
		t.Fatalf("missing = %v", cc.Missing)
	}
}

func TestCompleteFinalDigestMismatch(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("0123456789")
	// 声明的最终摘要与真实拼接结果不符。
	sess, err := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf([]byte("something else")),
		TotalSize: 10, Chunks: plan(t, data, 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadAll(t, c, sess.ID, data, 5, []int{0, 1})
	_, err = c.Complete(sess.ID, CompleteOptions{})
	var dm *DigestMismatchError
	if !errors.As(err, &dm) || dm.Operation != "complete" {
		t.Fatalf("want complete DigestMismatchError, got %v", err)
	}
	// 校验失败不得发布。
	if _, err := c.Read("k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("entry must not be visible after failed complete")
	}
}

// ---- 并发发布：同摘要复用，不同摘要走版本条件 ----

func TestConcurrentPublishSameDigestReuses(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("shared content bytes")
	specs := plan(t, data, 7)

	var wg sync.WaitGroup
	results := make([]*PublishResult, 3)
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sess, err := c.CreateSession(CreateSessionOptions{
				Key: "k", FinalDigest: digestOf(data),
				TotalSize: int64(len(data)), Chunks: specs,
			})
			if err != nil {
				errs[i] = err
				return
			}
			uploadAll(t, c, sess.ID, data, 7, []int{0, 1, 2})
			results[i], errs[i] = c.Complete(sess.ID, CompleteOptions{})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
	}
	versions := map[uint64]int{}
	var reused int
	for _, r := range results {
		versions[r.Entry.Version]++
		if r.Reused {
			reused++
		}
	}
	if len(versions) != 1 || versions[1] != 3 || reused != 2 {
		t.Fatalf("want all 3 resolve to v1 (1 new + 2 reused), got versions=%v reused=%d", versions, reused)
	}
	if got := readAll(t, c, "k"); !bytes.Equal(got, data) {
		t.Fatalf("content mismatch")
	}
}

func TestPublishDifferentDigestRequiresVersion(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)

	v1 := []byte("first version content!!!!")
	v2 := []byte("the second version content!!")

	publish := func(data []byte, key string, cond *uint64) *PublishResult {
		t.Helper()
		sess, err := c.CreateSession(CreateSessionOptions{
			Key: key, FinalDigest: digestOf(data),
			TotalSize: int64(len(data)), Chunks: plan(t, data, 8),
		})
		if err != nil {
			t.Fatal(err)
		}
		uploadAll(t, c, sess.ID, data, 8, nil)
		res, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: cond})
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
		return res
	}

	r1 := publish(v1, "k", nil)
	if r1.Entry.Version != 1 || r1.Reused {
		t.Fatalf("v1 = %+v", r1)
	}

	// 不同摘要 + 无条件：拒绝覆盖。
	sess2, _ := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(v2),
		TotalSize: int64(len(v2)), Chunks: plan(t, v2, 8),
	})
	uploadAll(t, c, sess2.ID, v2, 8, nil)
	_, err := c.Complete(sess2.ID, CompleteOptions{})
	var vc *VersionConflictError
	if !errors.As(err, &vc) {
		t.Fatalf("want VersionConflictError, got %v", err)
	}
	if vc.CurrentVersion != 1 || vc.ExpectedVersion != nil {
		t.Fatalf("unexpected conflict: %+v", vc)
	}
	// 被拒绝后原内容必须原样可读。
	if got := readAll(t, c, "k"); !bytes.Equal(got, v1) {
		t.Fatalf("v1 must remain readable after rejected overwrite")
	}

	// 错误版本号：仍拒绝。
	old := uint64(99)
	_, err = c.Complete(sess2.ID, CompleteOptions{ExpectedVersion: &old})
	if !errors.As(err, &vc) || *vc.ExpectedVersion != 99 {
		t.Fatalf("want CAS conflict, got %v", err)
	}

	// 正确版本号：覆盖为 v2。
	cur := uint64(1)
	r2, err := c.Complete(sess2.ID, CompleteOptions{ExpectedVersion: &cur})
	if err != nil {
		t.Fatalf("CAS overwrite: %v", err)
	}
	if r2.Reused || r2.Entry.Version != 2 {
		t.Fatalf("want fresh v2, got %+v", r2)
	}
	if got := readAll(t, c, "k"); !bytes.Equal(got, v2) {
		t.Fatalf("v2 content mismatch")
	}
}

// ---- 租约与过期：统一时钟、不复活 ----

func TestLeaseExpiryAndNoResurrection(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClock(base)
	c := newTestCache(t, NewMemoryStore(), clk, time.Minute)

	data := []byte("lease-test-data")
	sess, _ := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, 5),
	})

	// 续期成功。
	if _, err := c.RenewSession(sess.ID, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.Advance(90 * time.Second)
	if _, err := c.UploadChunk(sess.ID, 0, data[0:5]); err != nil {
		t.Fatalf("upload within renewed lease: %v", err)
	}

	// 越过过期时刻。
	clk.Advance(40 * time.Second) // 总计 130s > 120s
	if _, err := c.UploadChunk(sess.ID, 1, data[5:10]); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("want ErrLeaseExpired, got %v", err)
	}
	// 过期后续期不能复活。
	if _, err := c.RenewSession(sess.ID, time.Hour); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after expiry must fail, got %v", err)
	}
	// 完成也不能复活。
	if _, err := c.Complete(sess.ID, CompleteOptions{}); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("complete after expiry must fail, got %v", err)
	}
	// 记录仍在但已死亡；统一清理后才消失。
	if got, err := c.GetSession(sess.ID); err != nil || got.Status != SessionOpen {
		t.Fatalf("expired session record should remain until sweep, got %v %v", got, err)
	}
	swept, err := c.SweepExpired()
	if err != nil || len(swept) != 1 {
		t.Fatalf("sweep = %v, %v", swept, err)
	}
	if _, err := c.GetSession(sess.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expired session should be gone after sweep, got %v", err)
	}

	// 同键新会话可以正常工作（旧会话没有挡住新发布）。
	sess2, err := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadAll(t, c, sess2.ID, data, 5, nil)
	if _, err := c.Complete(sess2.ID, CompleteOptions{}); err != nil {
		t.Fatalf("publish after old session expiry: %v", err)
	}
}

func TestSweepExpired(t *testing.T) {
	base := time.Now()
	clk := NewFakeClock(base)
	c := newTestCache(t, NewMemoryStore(), clk, time.Minute)

	expiringData := []byte("old")
	liveData := []byte("new and fresh")
	s1, _ := c.CreateSession(CreateSessionOptions{
		Key: "old", FinalDigest: digestOf(expiringData),
		TotalSize: 3, Chunks: plan(t, expiringData, 3),
	})
	s2, _ := c.CreateSession(CreateSessionOptions{
		Key: "new", FinalDigest: digestOf(liveData),
		TotalSize: int64(len(liveData)), Chunks: plan(t, liveData, 4),
	})

	// 中途给 s2 续期，之后再越过 s1 的过期时刻。
	clk.Advance(30 * time.Second)
	if _, err := c.RenewSession(s2.ID, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	clk.Advance(31 * time.Second) // 总计 61s：s1 已过期，s2 仍在续期窗口内
	swept, err := c.SweepExpired()
	if err != nil {
		t.Fatal(err)
	}
	if len(swept) != 1 || swept[0].ID != s1.ID || swept[0].Reason != "lease_expired" {
		t.Fatalf("swept = %+v", swept)
	}
	if _, err := c.GetSession(s1.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("s1 must be gone")
	}
	if _, err := c.GetSession(s2.ID); err != nil {
		t.Fatalf("s2 must survive, got %v", err)
	}
}

// ---- 取消 ----

func TestCancel(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("cancel me please")
	sess, _ := c.CreateSession(CreateSessionOptions{
		Key: "k", FinalDigest: digestOf(data),
		TotalSize: int64(len(data)), Chunks: plan(t, data, 5),
	})
	if err := c.Cancel(sess.ID); err != nil {
		t.Fatal(err)
	}
	// 重复取消幂等。
	if err := c.Cancel(sess.ID); err != nil {
		t.Fatalf("idempotent cancel: %v", err)
	}
	// 取消后上传/完成都被拒绝。
	if _, err := c.UploadChunk(sess.ID, 0, data[0:5]); !errors.Is(err, ErrSessionNotActive) {
		t.Fatalf("upload after cancel: %v", err)
	}
	if _, err := c.Complete(sess.ID, CompleteOptions{}); !errors.Is(err, ErrSessionNotActive) {
		t.Fatalf("complete after cancel: %v", err)
	}
	// 清理后记录消失。
	if swept, err := c.SweepExpired(); err != nil || len(swept) != 1 || swept[0].Reason != "canceled" {
		t.Fatalf("sweep canceled = %v, %v", swept, err)
	}
}

// ---- 幂等键 ----

func TestIdempotencyKey(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	data := []byte("idempotent payload bytes")
	opts := CreateSessionOptions{
		Key: "k", IdempotencyKey: "req-1",
		FinalDigest: digestOf(data), TotalSize: int64(len(data)), Chunks: plan(t, data, 6),
	}
	s1, err := c.CreateSession(opts)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := c.CreateSession(opts)
	if err != nil {
		t.Fatal(err)
	}
	if s1.ID != s2.ID {
		t.Fatalf("idempotent create must return same session: %s vs %s", s1.ID, s2.ID)
	}

	// 同幂等键不同参数 -> 冲突。
	opts2 := opts
	opts2.FinalDigest = digestOf([]byte("totally different"))
	_, err = c.CreateSession(opts2)
	var ic *IdempotencyConflictError
	if !errors.As(err, &ic) || ic.Existing != s1.ID {
		t.Fatalf("want IdempotencyConflictError, got %v", err)
	}

	// 完成后幂等键释放，允许以同键创建新会话。
	uploadAll(t, c, s1.ID, data, 6, nil)
	if _, err := c.Complete(s1.ID, CompleteOptions{}); err != nil {
		t.Fatal(err)
	}
	s3, err := c.CreateSession(opts)
	if err != nil || s3.ID == s1.ID {
		t.Fatalf("after completion idempotency key should be reusable, got %v %v", s3, err)
	}
}

// ---- 垃圾回收 ----

func TestGarbageCollection(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFakeClock(base)
	c := newTestCache(t, NewMemoryStore(), clk, time.Minute)

	published := []byte("published artifact data!!!")
	active := []byte("still uploading this one")
	orphan := []byte("nobody wants me anymore")
	expiring := []byte("dead session data")

	// 1) 已发布条目：其块必须保留。
	s1, _ := c.CreateSession(CreateSessionOptions{
		Key: "pub", FinalDigest: digestOf(published),
		TotalSize: int64(len(published)), Chunks: plan(t, published, 8),
	})
	uploadAll(t, c, s1.ID, published, 8, nil)
	if _, err := c.Complete(s1.ID, CompleteOptions{}); err != nil {
		t.Fatal(err)
	}

	// 2) 活跃会话已上传的块：必须保留。给它长续期，使其在时钟推进后仍活跃。
	s2, _ := c.CreateSession(CreateSessionOptions{
		Key: "act", FinalDigest: digestOf(active),
		TotalSize: int64(len(active)), Chunks: plan(t, active, 7),
	})
	if _, err := c.UploadChunk(s2.ID, 0, active[0:7]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RenewSession(s2.ID, time.Hour); err != nil {
		t.Fatal(err)
	}

	// 3) 过期会话的块：GC 应删会话并回收块。
	s3, _ := c.CreateSession(CreateSessionOptions{
		Key: "exp", FinalDigest: digestOf(expiring),
		TotalSize: int64(len(expiring)), Chunks: plan(t, expiring, 6),
	})
	if _, err := c.UploadChunk(s3.ID, 0, expiring[0:6]); err != nil {
		t.Fatal(err)
	}
	expChunk := digestOf(expiring[0:6])

	// 4) 彻底无主的块（模拟残留）。
	orphanD := digestOf(orphan)
	if err := c.store.PutBlob(orphanD, orphan); err != nil {
		t.Fatal(err)
	}

	clk.Advance(61 * time.Second)
	report, err := c.CollectGarbage()
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Generation != 1 {
		t.Fatalf("first GC generation = %d", report.Generation)
	}
	// 过期会话被清扫。
	if len(report.SweptSessions) != 1 || report.SweptSessions[0].ID != s3.ID {
		t.Fatalf("swept = %+v", report.SweptSessions)
	}
	// 无主块 + 过期会话块被删除（已发布块与活跃块保留）。
	deleted := map[string]bool{}
	for _, db := range report.DeletedBlobs {
		deleted[db.Digest.String()] = true
	}
	if !deleted[orphanD.String()] || !deleted[expChunk.String()] {
		t.Fatalf("expected orphan and expired chunks deleted, got %v", deleted)
	}
	for _, spec := range plan(t, published, 8) {
		if deleted[spec.Digest.String()] {
			t.Fatalf("published chunk %s must not be deleted", spec.Digest)
		}
	}
	if deleted[digestOf(active[0:7]).String()] {
		t.Fatalf("active session chunk must not be deleted")
	}

	// 已发布内容依旧可读。
	if got := readAll(t, c, "pub"); !bytes.Equal(got, published) {
		t.Fatalf("published content corrupted after GC")
	}
	// 活跃会话仍可继续上传并完成。
	for i := 1; i < len(plan(t, active, 7)); i++ {
		off := i * 7
		end := min(off+7, len(active))
		if off >= len(active) {
			break
		}
		if _, err := c.UploadChunk(s2.ID, i, active[off:end]); err != nil {
			t.Fatalf("resume upload %d: %v", i, err)
		}
	}
	if _, err := c.Complete(s2.ID, CompleteOptions{}); err != nil {
		t.Fatalf("complete active session after GC: %v", err)
	}

	// 第二次 GC：已无东西可删，代次递增。
	report2, err := c.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if report2.Generation != 2 || len(report2.DeletedBlobs) != 0 {
		t.Fatalf("second GC = %+v", report2)
	}

	// 审计记录可查，动作与代次齐全。
	audit, err := c.AuditLog()
	if err != nil {
		t.Fatal(err)
	}
	var starts, finishes, deletes, sweeps int
	for _, r := range audit {
		switch r.Action {
		case GCStart:
			starts++
		case GCFinish:
			finishes++
		case GCDelete:
			deletes++
		case GCSessionSwept:
			sweeps++
		}
		if r.Time.IsZero() {
			t.Fatalf("audit record without timestamp: %+v", r)
		}
	}
	if starts != 2 || finishes != 2 || deletes < 2 || sweeps != 1 {
		t.Fatalf("audit counts start=%d finish=%d delete=%d sweep=%d", starts, finishes, deletes, sweeps)
	}
}

// 发布覆盖后，旧版本独有的块应被 GC；新旧版本共享的块必须保留。
func TestGCAfterOverwrite(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), time.Hour)
	common := []byte("common prefix!!!") // 16 字节，恰好两个 8 字节分片
	oldTail := []byte("OLD-TAIL")
	newTail := []byte("NEW-TAIL!!")
	v1 := append(append([]byte{}, common...), oldTail...)
	v2 := append(append([]byte{}, common...), newTail...)

	pub := func(data []byte, ver *uint64) {
		t.Helper()
		sess, _ := c.CreateSession(CreateSessionOptions{
			Key: "k", FinalDigest: digestOf(data),
			TotalSize: int64(len(data)), Chunks: plan(t, data, 8),
		})
		specs := plan(t, data, 8)
		for i, sp := range specs {
			if _, err := c.UploadChunk(sess.ID, i, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Complete(sess.ID, CompleteOptions{ExpectedVersion: ver}); err != nil {
			t.Fatal(err)
		}
	}
	pub(v1, nil)
	oldOnly := digestOf(oldTail)
	v := uint64(1)
	pub(v2, &v)

	report, err := c.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range report.DeletedBlobs {
		if db.Digest == oldOnly {
			goto found
		}
	}
	t.Fatalf("old-version-only blob %s should be collected", oldOnly)
found:
	if got := readAll(t, c, "k"); !bytes.Equal(got, v2) {
		t.Fatalf("current version must remain intact after GC")
	}
}

// ---- 关键并发测试：GC 与上传/完成同时进行，绝不能误删已发布内容 ----

func TestGCConcurrentWithPublish(t *testing.T) {
	c := newTestCache(t, NewMemoryStore(), NewFakeClock(time.Now()), 24*time.Hour)

	const publishers = 8
	const iterations = 25

	var gcWG, pubWG sync.WaitGroup
	stop := make(chan struct{})
	var allKeys []string
	var keysMu sync.Mutex

	// GC 狂轰滥炸，直到所有发布者结束。
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := c.CollectGarbage(); err != nil {
					t.Errorf("gc: %v", err)
					return
				}
			}
		}
	}()

	for p := 0; p < publishers; p++ {
		pubWG.Add(1)
		go func(p int) {
			defer pubWG.Done()
			for i := 0; i < iterations; i++ {
				data := []byte(fmt.Sprintf("publisher-%d-iteration-%03d-padding-padding", p, i))
				key := fmt.Sprintf("p%d/key-%03d", p, i)
				specs := plan(t, data, 11)
				sess, err := c.CreateSession(CreateSessionOptions{
					Key: key, FinalDigest: digestOf(data),
					TotalSize: int64(len(data)), Chunks: specs,
				})
				if err != nil {
					t.Errorf("create: %v", err)
					return
				}
				idx := make([]int, len(specs))
				for j := range idx {
					idx[j] = j
				}
				// 打乱上传顺序。
				summary := p*31 + i
				sort.SliceStable(idx, func(a, b int) bool {
					return (a+summary)%5 < (b+summary)%5
				})
				for _, j := range idx {
					sp := specs[j]
					if _, err := c.UploadChunk(sess.ID, j, data[sp.Offset:sp.Offset+sp.Size]); err != nil {
						t.Errorf("upload: %v", err)
						return
					}
				}
				if _, err := c.Complete(sess.ID, CompleteOptions{}); err != nil {
					t.Errorf("complete: %v", err)
					return
				}
				keysMu.Lock()
				allKeys = append(allKeys, key)
				keysMu.Unlock()
			}
		}(p)
	}

	pubWG.Wait()
	close(stop)
	gcWG.Wait()

	// 最终再跑一次 GC：每个已发布条目的每个块都必须存在且内容完整。
	if _, err := c.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	keysMu.Lock()
	defer keysMu.Unlock()
	if len(allKeys) != publishers*iterations {
		t.Fatalf("published %d keys, want %d", len(allKeys), publishers*iterations)
	}
	for _, key := range allKeys {
		r, err := c.Read(key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatalf("readall %s: %v", key, err)
		}
		if digestOf(data) != r.Entry().Digest {
			t.Fatalf("key %s content digest digest mismatch", key)
		}
	}
}
