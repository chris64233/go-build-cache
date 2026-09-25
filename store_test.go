package buildcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------- 测试辅助 ----------------

func sha256Hex(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest(hex.EncodeToString(sum[:]))
}

// plan 把内容按固定大小切成 1 或多个分片声明。
func plan(t *testing.T, data []byte, chunkSize int) ([]ChunkSpec, [][]byte) {
	t.Helper()
	if chunkSize <= 0 {
		t.Fatal("chunkSize must be positive")
	}
	var specs []ChunkSpec
	var parts [][]byte
	for off := 0; off < len(data); off += chunkSize {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		part := data[off:end]
		specs = append(specs, ChunkSpec{Index: len(specs), Size: int64(len(part)), Hash: sha256Hex(part)})
		parts = append(parts, part)
	}
	return specs, parts
}

func newTestStore(t *testing.T) (*Store, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st, err := Open(t.TempDir(), Options{Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, clock
}

func createSession(t *testing.T, st *Store, key string, data []byte, chunkSize int) (*Session, [][]byte) {
	t.Helper()
	specs, parts := plan(t, data, chunkSize)
	sess, err := st.CreateSession(CreateSessionInput{
		Key:       key,
		Digest:    sha256Hex(data),
		TotalSize: int64(len(data)),
		Chunks:    specs,
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return sess, parts
}

func uploadAll(t *testing.T, st *Store, sess *Session, parts [][]byte, order ...int) {
	t.Helper()
	if len(order) == 0 {
		order = make([]int, len(parts))
		for i := range order {
			order[i] = i
		}
	}
	for _, i := range order {
		if err := st.PutChunkBytes(sess.ID, i, parts[i]); err != nil {
			t.Fatalf("PutChunk(%d): %v", i, err)
		}
	}
}

func readEntry(t *testing.T, st *Store, key string) []byte {
	t.Helper()
	rc, e, err := st.OpenEntry(key)
	if err != nil {
		t.Fatalf("OpenEntry(%q): %v", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read entry: %v", err)
	}
	if int64(len(data)) != e.Size {
		t.Fatalf("entry size: got %d, want %d", len(data), e.Size)
	}
	return data
}

// ---------------- 创建会话与参数校验 ----------------

func TestCreateSessionValidation(t *testing.T) {
	st, _ := newTestStore(t)
	good := Digest(strings.Repeat("a", 64))
	other := Digest(strings.Repeat("b", 64))

	cases := []struct {
		name string
		in   CreateSessionInput
		want error
	}{
		{"empty key", CreateSessionInput{Digest: good, TotalSize: 1, Chunks: []ChunkSpec{{0, 1, good}}}, ErrInvalidArgument},
		{"bad digest", CreateSessionInput{Key: "k", Digest: "xyz", TotalSize: 1, Chunks: []ChunkSpec{{0, 1, good}}}, ErrInvalidArgument},
		{"no chunks", CreateSessionInput{Key: "k", Digest: good}, ErrInvalidArgument},
		{"gap in chunks", CreateSessionInput{Key: "k", Digest: good, TotalSize: 1, Chunks: []ChunkSpec{{0, 1, good}, {2, 0, other}}}, ErrInvalidArgument},
		{"duplicate index", CreateSessionInput{Key: "k", Digest: good, TotalSize: 2, Chunks: []ChunkSpec{{0, 1, good}, {0, 1, other}}}, ErrInvalidArgument},
		{"size mismatch", CreateSessionInput{Key: "k", Digest: good, TotalSize: 99, Chunks: []ChunkSpec{{0, 1, good}}}, ErrInvalidArgument},
		{"bad chunk digest", CreateSessionInput{Key: "k", Digest: good, TotalSize: 1, Chunks: []ChunkSpec{{0, 1, "nope"}}}, ErrInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := st.CreateSession(tc.in); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	// 乱序提交分片声明也应被规范化（连续性校验）。
	sess, err := st.CreateSession(CreateSessionInput{
		Key: "k", Digest: good, TotalSize: 2,
		Chunks: []ChunkSpec{{1, 1, other}, {0, 1, good}},
	})
	if err != nil {
		t.Fatalf("unordered chunk specs should be accepted: %v", err)
	}
	if sess.Chunks[0].Index != 0 || sess.Chunks[1].Index != 1 {
		t.Fatalf("chunks not normalized: %+v", sess.Chunks)
	}
}

// ---------------- 分片：乱序、重传、冲突、摘要不符 ----------------

func TestChunkUploadOutOfOrderAndRetry(t *testing.T) {
	st, _ := newTestStore(t)
	data := bytes.Repeat([]byte("abc123XYZ"), 100) // 1000 字节
	sess, parts := createSession(t, st, "art:1", data, 128)

	// 乱序上传：逆序到达。
	order := make([]int, len(parts))
	for i := range order {
		order[i] = len(parts) - 1 - i
	}
	uploadAll(t, st, sess, parts, order...)
	got, err := st.Session(sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range got.Uploaded {
		if d == "" {
			t.Fatalf("chunk %d missing", i)
		}
	}

	// 相同内容重传：幂等成功。
	if err := st.PutChunkBytes(sess.ID, 0, parts[0]); err != nil {
		t.Fatalf("idempotent retransmit failed: %v", err)
	}

	entry, err := st.Complete(sess.ID)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if entry.Digest != sha256Hex(data) {
		t.Fatalf("published digest mismatch")
	}
	if !bytes.Equal(readEntry(t, st, "art:1"), data) {
		t.Fatalf("published content mismatch")
	}
}

func TestChunkConflictDifferentContent(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("hello world, this is chunked content!!")
	sess, parts := createSession(t, st, "art:2", data, 8)

	if err := st.PutChunkBytes(sess.ID, 1, parts[1]); err != nil {
		t.Fatal(err)
	}
	// 同一片号上传不同内容：必须报幂等冲突。
	different := bytes.Repeat([]byte("Q"), len(parts[1]))
	err := st.PutChunkBytes(sess.ID, 1, different)
	var cc *ChunkConflictError
	if !errors.As(err, &cc) {
		t.Fatalf("want ChunkConflictError, got %v", err)
	}
	if cc.ExistingDigest != sha256Hex(parts[1]) || cc.IncomingDigest != sha256Hex(different) {
		t.Fatalf("conflict details wrong: %+v", cc)
	}
}

func TestChunkDigestMismatch(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("0123456789")
	sess, parts := createSession(t, st, "art:3", data, 5)

	bad := []byte("XXXXX") // 与 parts[0] 长度相同但内容不同
	err := st.PutChunkBytes(sess.ID, 0, bad)
	var dm *DigestMismatchError
	if !errors.As(err, &dm) || dm.Scope != "chunk" || dm.Index != 0 {
		t.Fatalf("want chunk DigestMismatchError, got %v", err)
	}
	if dm.Expected != sha256Hex(parts[0]) || dm.Got != sha256Hex(bad) {
		t.Fatalf("mismatch detail wrong: %+v", dm)
	}

	// 校验失败的分片不得被记录，正确内容仍可补传。
	if err := st.PutChunkBytes(sess.ID, 0, parts[0]); err != nil {
		t.Fatalf("re-upload correct content: %v", err)
	}
	uploadAll(t, st, sess, parts, 1)
	if _, err := st.Complete(sess.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// 摘要不符的内容块不能残留在 CAS 中。
	if ok, _ := st.blobs.exists(sha256Hex(bad)); ok {
		t.Fatal("rejected blob is visible in CAS")
	}
}

func TestChunkIndexOutOfRange(t *testing.T) {
	st, _ := newTestStore(t)
	sess, parts := createSession(t, st, "k", []byte("ab"), 1)
	if err := st.PutChunkBytes(sess.ID, 5, []byte("x")); !errors.Is(err, ErrChunkIndex) {
		t.Fatalf("want ErrChunkIndex, got %v", err)
	}
	if err := st.PutChunkBytes(sess.ID, -1, []byte("x")); !errors.Is(err, ErrChunkIndex) {
		t.Fatalf("want ErrChunkIndex, got %v", err)
	}
	_ = parts
}

// ---------------- 发布前不可见 & 完成校验 ----------------

func TestInvisibleBeforePublish(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("not yet published content")
	sess, parts := createSession(t, st, "secret", data, 7)
	uploadAll(t, st, sess, parts)

	// 全部分片就位但尚未 Complete：读者必须看不到。
	if _, err := st.Get("secret"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("Get before complete: want ErrEntryNotFound, got %v", err)
	}
	if _, _, err := st.OpenEntry("secret"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("OpenEntry before complete: want ErrEntryNotFound, got %v", err)
	}
}

func TestCompleteMissingChunks(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("0123456789")
	sess, parts := createSession(t, st, "k", data, 5)
	if err := st.PutChunkBytes(sess.ID, 0, parts[0]); err != nil {
		t.Fatal(err)
	}
	_, err := st.Complete(sess.ID)
	var miss *ChunkMissingError
	if !errors.As(err, &miss) || len(miss.Missing) != 1 || miss.Missing[0] != 1 {
		t.Fatalf("want ChunkMissingError{[1]}, got %v", err)
	}
	// 校验失败不得产生条目。
	if _, err := st.Get("k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("entry appeared despite failed complete: %v", err)
	}
}

func TestCompleteFinalDigestMismatch(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("0123456789")
	specs, parts := plan(t, data, 5)

	// 声明一个错误的最终摘要：每个分片自身都合法，但拼接结果与声明不符。
	sess, err := st.CreateSession(CreateSessionInput{
		Key:       "k",
		Digest:    Digest(strings.Repeat("f", 64)),
		TotalSize: int64(len(data)),
		Chunks:    specs,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadAll(t, st, sess, parts)
	_, err = st.Complete(sess.ID)
	var dm *DigestMismatchError
	if !errors.As(err, &dm) || dm.Scope != "entry" || dm.Got != sha256Hex(data) {
		t.Fatalf("want entry-level DigestMismatchError, got %v", err)
	}
	if _, err := st.Get("k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("entry appeared despite digest mismatch: %v", err)
	}
}

func TestDoubleCompleteIsIdempotent(t *testing.T) {
	st, _ := newTestStore(t)
	data := []byte("double-complete")
	sess, parts := createSession(t, st, "k", data, 4)
	uploadAll(t, st, sess, parts)
	e1, err := st.Complete(sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Complete(sess.ID)
	if !errors.Is(err, ErrSessionCompleted) {
		t.Fatalf("second complete: want ErrSessionCompleted, got %v", err)
	}
	if e1.Version != 1 {
		t.Fatalf("first version = %d, want 1", e1.Version)
	}
}

// ---------------- 并发发布：同摘要复用 / 异值版本冲突 ----------------

func TestConcurrentCompleteSameDigestReuse(t *testing.T) {
	st, _ := newTestStore(t)
	data := bytes.Repeat([]byte("concurrent!"), 50)

	const n = 8
	sessions := make([]*Session, n)
	for i := range sessions {
		sess, parts := createSession(t, st, "shared-key", data, 64)
		uploadAll(t, st, sess, parts)
		sessions[i] = sess
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	entries := make([]*Entry, n)
	start := make(chan struct{})
	for i, sess := range sessions {
		wg.Add(1)
		go func(i int, sess *Session) {
			defer wg.Done()
			<-start
			entries[i], errs[i] = st.Complete(sess.ID)
		}(i, sess)
	}
	close(start)
	wg.Wait()

	published, reused := 0, 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		if entries[i].Digest != sha256Hex(data) {
			t.Fatalf("wrong digest")
		}
		// 标志记录在存储内部的会话副本上，重新取快照核对。
		snap, _ := st.Session(sessions[i].ID)
		if snap.PublishedKey {
			published++
		}
		if snap.ReusedEntry {
			reused++
		}
	}
	if published != 1 || reused != n-1 {
		t.Fatalf("want exactly 1 publish and %d reuses, got %d/%d", n-1, published, reused)
	}
	if got := readEntry(t, st, "shared-key"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
}

func TestConcurrentCompleteDifferentDigestVersionConflict(t *testing.T) {
	st, _ := newTestStore(t)
	dataA := bytes.Repeat([]byte("A-content"), 30)
	dataB := bytes.Repeat([]byte("B-content"), 30)

	sessA, partsA := createSession(t, st, "contested", dataA, 32)
	uploadAll(t, st, sessA, partsA)
	sessB, partsB := createSession(t, st, "contested", dataB, 32)
	uploadAll(t, st, sessB, partsB)

	// 串行完成两次，验证版本条件拒绝异值覆盖。
	if _, err := st.Complete(sessA.ID); err != nil {
		t.Fatalf("complete A: %v", err)
	}
	_, err := st.Complete(sessB.ID)
	var vc *VersionConflictError
	if !errors.As(err, &vc) {
		t.Fatalf("want VersionConflictError, got %v", err)
	}
	if vc.ExistingDigest != sha256Hex(dataA) || vc.RequestedDigest != sha256Hex(dataB) || vc.ExistingVersion != 1 {
		t.Fatalf("conflict detail wrong: %+v", vc)
	}
	if got := readEntry(t, st, "contested"); !bytes.Equal(got, dataA) {
		t.Fatal("existing entry must remain untouched")
	}

	// B 会话保持未完成状态，其独占块在过期后可被 GC，但条目的块不受影响。
	if _, err := st.Get("contested"); err != nil {
		t.Fatal(err)
	}
}

// ---------------- 租约与过期 ----------------

func TestLeaseExpiryAndNoResurrection(t *testing.T) {
	st, clock := newTestStore(t)
	data := []byte("lease lifecycle data!")
	sess, parts := createSession(t, st, "k", data, 7)
	uploadAll(t, st, sess, parts, 0)

	// 未过期时可以续期。
	clock.Advance(30 * time.Minute)
	if _, err := st.RenewLease(sess.ID); err != nil {
		t.Fatalf("renew: %v", err)
	}
	got, _ := st.Session(sess.ID)
	if !got.ExpiresAt.Equal(clock.Now().Add(time.Hour)) {
		t.Fatalf("renew did not extend lease: %v", got.ExpiresAt)
	}

	// 过期后：上传、完成、续期全部拒绝，会话不会复活。
	clock.Advance(time.Hour + time.Second)
	if err := st.PutChunkBytes(sess.ID, 1, parts[1]); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("upload after expiry: want ErrLeaseExpired, got %v", err)
	}
	if _, err := st.Complete(sess.ID); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("complete after expiry: want ErrLeaseExpired, got %v", err)
	}
	if _, err := st.RenewLease(sess.ID); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after expiry: want ErrLeaseExpired, got %v", err)
	}
	got, _ = st.Session(sess.ID)
	if got.State != SessionExpiredMark {
		t.Fatalf("state = %s, want expired", got.State)
	}

	// 显式过期清理是幂等的，且条目从未发布。
	if n := st.ExpireSessions(); n != 0 {
		t.Fatalf("lazy expiry should leave nothing for sweep, got %d", n)
	}
	if _, err := st.Get("k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expired session must not publish: %v", err)
	}
}

func TestExpiryDoesNotTouchCompletedEntry(t *testing.T) {
	st, clock := newTestStore(t)
	data := []byte("published forever")
	sess, parts := createSession(t, st, "k", data, 5)
	uploadAll(t, st, sess, parts)
	if _, err := st.Complete(sess.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Hour) // 远超租约
	if n := st.ExpireSessions(); n != 0 {
		t.Fatalf("completed session expired: %d", n)
	}
	if got := readEntry(t, st, "k"); !bytes.Equal(got, data) {
		t.Fatal("published entry vanished after lease elapsed")
	}
}

func TestExpireSessionsBatch(t *testing.T) {
	st, clock := newTestStore(t)
	var ids []string
	for i := 0; i < 3; i++ {
		sess, _ := createSession(t, st, fmt.Sprintf("k%d", i), []byte{byte(i)}, 1)
		ids = append(ids, sess.ID)
	}
	clock.Advance(2 * time.Hour)
	if n := st.ExpireSessions(); n != 3 {
		t.Fatalf("expired %d, want 3", n)
	}
	for _, id := range ids {
		got, _ := st.Session(id)
		if got.State != SessionExpiredMark {
			t.Fatalf("%s state = %s", id, got.State)
		}
	}
}

// 上传进行期间会话过期：CAS 写入完成后提交时必须重新确认活跃状态，失效会话不得复活。
func TestChunkArrivingExactlyAtExpiry(t *testing.T) {
	st, clock := newTestStore(t)
	data := []byte("race the lease!")
	sess, parts := createSession(t, st, "k", data, 6)

	// 直接构造“CAS 已写完、元数据尚未提交”窗口：先把块放进 CAS，再令租约过期，最后提交。
	spec := sess.Chunks[0]
	if _, _, ok, err := st.blobs.PutChecked(bytes.NewReader(parts[0]), spec.Hash, spec.Size); err != nil || !ok {
		t.Fatalf("seed blob: %v %v", ok, err)
	}
	clock.Advance(2 * time.Hour)
	st.ExpireSessions()
	if err := st.PutChunkBytes(sess.ID, 0, parts[0]); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("commit after expiry: want ErrLeaseExpired, got %v", err)
	}
	got, _ := st.Session(sess.ID)
	if got.Uploaded[0] != "" {
		t.Fatal("expired session was resurrected by late upload")
	}
}

// ---------------- 取消 ----------------

func TestCancel(t *testing.T) {
	st, _ := newTestStore(t)
	sess, parts := createSession(t, st, "k", []byte("cancel me!!"), 4)
	if err := st.PutChunkBytes(sess.ID, 0, parts[0]); err != nil {
		t.Fatal(err)
	}

	if err := st.Cancel(sess.ID, "client gone"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := st.Cancel(sess.ID, "again"); err != nil { // 幂等
		t.Fatalf("second cancel should be idempotent: %v", err)
	}
	if err := st.PutChunkBytes(sess.ID, 1, parts[1]); !errors.Is(err, ErrSessionCancelled) {
		t.Fatalf("upload after cancel: want ErrSessionCancelled, got %v", err)
	}
	if _, err := st.Complete(sess.ID); !errors.Is(err, ErrSessionCancelled) {
		t.Fatalf("complete after cancel: want ErrSessionCancelled, got %v", err)
	}
	got, _ := st.Session(sess.ID)
	if got.State != SessionCancelled || got.Reason != "client gone" {
		t.Fatalf("cancel state not persisted: %+v", got)
	}
}

func TestCancelCompletedRejected(t *testing.T) {
	st, _ := newTestStore(t)
	sess, parts := createSession(t, st, "k", []byte("done"), 2)
	uploadAll(t, st, sess, parts)
	if _, err := st.Complete(sess.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.Cancel(sess.ID, ""); !errors.Is(err, ErrSessionCompleted) {
		t.Fatalf("want ErrSessionCompleted, got %v", err)
	}
}

func TestUnknownSession(t *testing.T) {
	st, _ := newTestStore(t)
	if _, err := st.Session("nope"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal(err)
	}
	if err := st.PutChunkBytes("nope", 0, []byte("x")); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal(err)
	}
	if _, err := st.Complete("nope"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal(err)
	}
	if err := st.Cancel("nope", ""); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal(err)
	}
}

// ---------------- 垃圾回收 ----------------

// 构造两个条目：共享一个内容块；再制造一个仅被取消会话引用的孤儿块和一个无主块。
func gcFixture(t *testing.T, st *Store) (shared, onlyA, orphan, ownerless Digest) {
	t.Helper()
	sharedBlob := []byte("shared chunk payload")
	onlyABlob := []byte("only in entry A")
	orphanBlob := []byte("cancelled session only")
	ownerlessBlob := []byte("nobody owns me at all")

	shared = sha256Hex(sharedBlob)
	onlyA = sha256Hex(onlyABlob)
	orphan = sha256Hex(orphanBlob)
	ownerless = sha256Hex(ownerlessBlob)

	// 条目 A = shared + onlyA；条目 B = shared + 自有内容（内联即可）。
	bBlob := []byte("entry B tail data")
	dataA := bytes.Join([][]byte{sharedBlob, onlyABlob}, nil)
	dataB := bytes.Join([][]byte{sharedBlob, bBlob}, nil)

	sessA, partsA := createSession(t, st, "A", dataA, len(sharedBlob))
	uploadAll(t, st, sessA, partsA)
	if _, err := st.Complete(sessA.ID); err != nil {
		t.Fatal(err)
	}
	sessB, partsB := createSession(t, st, "B", dataB, len(sharedBlob))
	uploadAll(t, st, sessB, partsB)
	if _, err := st.Complete(sessB.ID); err != nil {
		t.Fatal(err)
	}

	// 取消的会话上传过 orphanBlob。
	sessC, partsC := createSession(t, st, "C", orphanBlob, len(orphanBlob))
	uploadAll(t, st, sessC, partsC)
	if err := st.Cancel(sessC.ID, "test"); err != nil {
		t.Fatal(err)
	}

	// 纯无主块直接放进 CAS。
	if _, _, ok, err := st.blobs.PutChecked(bytes.NewReader(ownerlessBlob), ownerless, int64(len(ownerlessBlob))); err != nil || !ok {
		t.Fatalf("seed ownerless blob: %v %v", ok, err)
	}
	return
}

func TestGCBasic(t *testing.T) {
	st, _ := newTestStore(t)
	shared, onlyA, orphan, ownerless := gcFixture(t, st)

	res, err := st.CollectGarbage()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if res.Deleted != 2 {
		t.Fatalf("deleted %d, want 2 (orphan + ownerless)", res.Deleted)
	}
	if res.Kept != 3 { // shared、onlyA、B-tail
		t.Fatalf("kept %d, want 3", res.Kept)
	}
	if res.Scanned != 5 {
		t.Fatalf("scanned %d, want 5", res.Scanned)
	}
	if res.DeletedSize != int64(len("cancelled session only")+len("nobody owns me at all")) {
		t.Fatalf("deleted size accounting wrong: %d", res.DeletedSize)
	}

	// 已发布条目引用的块必须完好（含两条目共享的块）。
	for _, d := range []Digest{shared, onlyA, sha256Hex([]byte("entry B tail data"))} {
		if ok, _ := st.blobs.exists(d); !ok {
			t.Fatalf("referenced blob %s was deleted", d)
		}
	}
	for _, d := range []Digest{orphan, ownerless} {
		if ok, _ := st.blobs.exists(d); ok {
			t.Fatalf("unreachable blob %s survived GC", d)
		}
	}

	// 条目内容仍然可读。
	if !bytes.Equal(readEntry(t, st, "A"), bytes.Join([][]byte{[]byte("shared chunk payload"), []byte("only in entry A")}, nil)) {
		t.Fatal("entry A corrupted after GC")
	}

	// 审计：每个块都有决策记录，删除与保留原因可核对。
	audit, err := st.AuditLog()
	if err != nil || len(audit) != 5 {
		t.Fatalf("audit decisions = %d, err=%v", len(audit), err)
	}
	byDigest := map[Digest]GCDecision{}
	for _, d := range audit {
		byDigest[d.Digest] = d
		if d.RunID != res.RunID || d.Time.IsZero() {
			t.Fatalf("audit record missing run metadata: %+v", d)
		}
	}
	if byDigest[shared].Action != GCKeepMarkedPublished || !byDigest[shared].Referenced {
		t.Fatalf("shared decision: %+v", byDigest[shared])
	}
	if byDigest[orphan].Action != GCDelete || byDigest[orphan].Referenced {
		t.Fatalf("orphan decision: %+v", byDigest[orphan])
	}

	// 第二轮 GC：稳定，无东西可删。
	res2, err := st.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if res2.Deleted != 0 || res2.Kept != 3 {
		t.Fatalf("second GC not stable: %+v", res2)
	}
}

func TestGCKeepsActiveSessionChunks(t *testing.T) {
	st, clock := newTestStore(t)
	data := []byte("half uploaded content right here")
	sess, parts := createSession(t, st, "k", data, 10)
	uploadAll(t, st, sess, parts, 0, 1) // 只传了两片
	// 未上传的声明块在 CAS 中不存在，已上传的两块必须被保留。
	res, err := st.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 0 {
		t.Fatalf("active session chunks deleted: %+v", res.Decisions)
	}
	for _, d := range res.Decisions {
		if d.Action != GCKeepMarkedSession {
			t.Fatalf("want keep_active_session, got %s", d.Action)
		}
	}

	// 过期后同一块应被回收。
	clock.Advance(2 * time.Hour)
	st.ExpireSessions()
	res2, err := st.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	if res2.Deleted != 2 {
		t.Fatalf("want 2 deleted after expiry, got %+v", res2)
	}
}

// GC 标记与删除之间的并发发布：屏障必须保护新建立的引用。
func TestGCBarrierProtectsInflightPublish(t *testing.T) {
	st, clock := newTestStore(t)

	// X 先由活跃会话 A 引用（第一轮标记可见）；B 会话将在屏障窗口内把 X 发布成条目。
	x := []byte("published during gc window!!")
	sessA, partsA := createSession(t, st, "A", x, len(x))
	uploadAll(t, st, sessA, partsA)
	sessB, partsB := createSession(t, st, "B", x, len(x))
	uploadAll(t, st, sessB, partsB)

	// Z 仅由会话 C 引用，并在屏障窗口内被取消——两轮之间失去引用，应安全删除。
	z := []byte("vanishes between marks")
	sessC, partsC := createSession(t, st, "C", z, len(z))
	uploadAll(t, st, sessC, partsC)

	release := make(chan struct{})
	st.inflight.Add(1) // 手工撑开屏障窗口
	st.testHooks.afterFirstMark = func() {
		go func() {
			// 在屏障等待期间完成发布 B（B 的 inflight 在 Wait 之前归零）。
			if _, err := st.Complete(sessB.ID); err != nil {
				t.Errorf("complete B during barrier: %v", err)
			}
			// A 与 C 都终结：X 现在只被新发布的条目 B 引用；Z 再无引用。
			clock.Advance(2 * time.Hour)
			st.ExpireSessions()
			close(release)
			st.inflight.Done()
		}()
	}

	done := make(chan struct{})
	var res *GCResult
	var gcErr error
	go func() {
		res, gcErr = st.CollectGarbage()
		close(done)
	}()

	select {
	case <-release:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not reach the barrier")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not finish after barrier release")
	}
	if gcErr != nil {
		t.Fatal(gcErr)
	}

	if ok, _ := st.blobs.exists(sha256Hex(x)); !ok {
		t.Fatal("blob published between marks was deleted")
	}
	if ok, _ := st.blobs.exists(sha256Hex(z)); ok {
		t.Fatal("blob whose only reference vanished should be deleted")
	}
	if res.Deleted != 1 {
		t.Fatalf("deleted %d, want 1: %+v", res.Deleted, res.Decisions)
	}
	// X 的保留原因必须来自第二轮重新标记到的已发布条目。
	var xDecision *GCDecision
	for i := range res.Decisions {
		if res.Decisions[i].Digest == sha256Hex(x) {
			xDecision = &res.Decisions[i]
		}
	}
	if xDecision == nil || xDecision.Action != GCKeepInFlight {
		t.Fatalf("X decision: %+v", xDecision)
	}
	if !bytes.Equal(readEntry(t, st, "B"), x) {
		t.Fatal("entry B unreadable after GC")
	}
}

func TestGCMutualExclusion(t *testing.T) {
	st, _ := newTestStore(t)
	gcFixture(t, st)

	proceed := make(chan struct{})
	st.testHooks.afterFirstMark = func() { <-proceed }
	firstErr := make(chan error, 1)
	go func() { _, err := st.CollectGarbage(); firstErr <- err }()

	// 等待第一轮通过第一次标记（gcRunning 已置位）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st.mu.Lock()
		running := st.gcRunning
		st.mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first GC never started")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := st.CollectGarbage(); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("concurrent GC: want ErrGCBusy, got %v", err)
	}
	close(proceed)
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
}

// ---------------- 持久化恢复 ----------------

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	data := bytes.Repeat([]byte("persist-me-"), 20)

	st, err := Open(dir, Options{Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	sess, parts := createSession(t, st, "persist:1", data, 37)
	uploadAll(t, st, sess, parts, 0, 1) // 只传两片就关闭
	// 元数据文件确实存在。
	if _, err := os.Stat(filepath.Join(dir, sessionsDir, sess.ID+".json")); err != nil {
		t.Fatalf("session not persisted: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开：会话可继续上传并完成；时钟未推进，租约仍有效。
	st2, err := Open(dir, Options{Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, err := st2.Session(sess.ID)
	if err != nil {
		t.Fatalf("session lost after reopen: %v", err)
	}
	if got.Uploaded[0] == "" || got.Uploaded[1] == "" || got.Uploaded[2] != "" {
		t.Fatalf("upload progress not recovered: %+v", got.Uploaded)
	}
	for i := 2; i < len(parts); i++ {
		if err := st2.PutChunkBytes(sess.ID, i, parts[i]); err != nil {
			t.Fatalf("resume upload %d: %v", i, err)
		}
	}
	entry, err := st2.Complete(sess.ID)
	if err != nil {
		t.Fatalf("complete after reopen: %v", err)
	}
	if entry.Digest != sha256Hex(data) {
		t.Fatal("digest mismatch after reopen")
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次重开：已发布条目仍可读取。
	st3, err := Open(dir, Options{Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	if !bytes.Equal(readEntry(t, st3, "persist:1"), data) {
		t.Fatal("entry content mismatch after reopen")
	}

	// 再造一个未完成会话，用它验证过期状态跨重开的判定。
	pending, pendingParts := createSession(t, st3, "persist:pending", []byte("never finished"), 5)
	if err := st3.PutChunkBytes(pending.ID, 0, pendingParts[0]); err != nil {
		t.Fatal(err)
	}
	if err := st3.Close(); err != nil {
		t.Fatal(err)
	}

	// 推进时钟超过租约后重开，旧会话直接判定过期。
	clock.Advance(2 * time.Hour)
	st4, err := Open(dir, Options{Clock: clock, LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer st4.Close()
	if n := st4.ExpireSessions(); n != 1 {
		t.Fatalf("expired %d unfinished sessions after reopen, want 1", n)
	}
	snap, err := st4.Session(pending.ID)
	if err != nil || snap.State != SessionExpiredMark {
		t.Fatalf("pending session not expired after reopen: %+v err=%v", snap, err)
	}
}

func TestGCAuditPersists(t *testing.T) {
	dir := t.TempDir()
	clock := NewFakeClock(time.Now())
	st, err := Open(dir, Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	blob := []byte("audit me")
	d := sha256Hex(blob)
	if _, _, ok, err := st.blobs.PutChecked(bytes.NewReader(blob), d, int64(len(blob))); err != nil || !ok {
		t.Fatalf("seed blob: %v %v", ok, err)
	}
	res, err := st.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	data, err := os.ReadFile(filepath.Join(dir, auditFile))
	if err != nil {
		t.Fatalf("audit file: %v", err)
	}
	if !strings.Contains(string(data), res.RunID) || !strings.Contains(string(data), `"delete"`) {
		t.Fatalf("audit content unexpected: %s", data)
	}
}

// ---------------- blobStore 直接测试 ----------------

func TestBlobStoreConcurrentSameContent(t *testing.T) {
	bs := newBlobStore(t.TempDir())
	content := bytes.Repeat([]byte("Z"), 5000)
	d := sha256Hex(content)

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, accepted, e := bs.PutChecked(bytes.NewReader(append([]byte(nil), content...)), d, int64(len(content)))
			if !accepted {
				e = fmt.Errorf("put %d unexpectedly rejected", i)
			}
			errs[i] = e
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	all, err := bs.listAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0] != d {
		t.Fatalf("CAS after concurrent same-content put: %v", all)
	}
}

func TestBlobStoreMismatchNotVisible(t *testing.T) {
	bs := newBlobStore(t.TempDir())
	declared := Digest(strings.Repeat("9", 64))
	got, n, accepted, err := bs.PutChecked(strings.NewReader("hello"), declared, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accepted {
		t.Fatal("content not matching declaration was accepted")
	}
	if got != sha256Hex([]byte("hello")) || n != 5 {
		t.Fatalf("actual digest/size not returned: %s %d", got, n)
	}
	if all, _ := bs.listAll(); len(all) != 0 {
		t.Fatalf("rejected content leaked into CAS: %v", all)
	}
}

// 汇总：完整流程 + -race 下的端到端冒烟。
func TestEndToEnd(t *testing.T) {
	st, _ := newTestStore(t)
	data := bytes.Repeat([]byte("0123456789ABCDEF"), 256) // 4KiB
	sess, parts := createSession(t, st, "bin/linux/amd64/abc", data, 100)

	// 并发乱序上传（含同内容重传）：每个片号至少一个上传者，0 号片额外重传一次。
	var wg sync.WaitGroup
	indices := make([]int, 0, len(parts)+1)
	for i := len(parts) - 1; i >= 0; i-- {
		indices = append(indices, i)
	}
	indices = append(indices, 0)
	start := make(chan struct{})
	for _, i := range indices {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := st.PutChunkBytes(sess.ID, i, parts[i]); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	e, err := st.Complete(sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	if e.Size != int64(len(data)) {
		t.Fatalf("size %d", e.Size)
	}
	if !bytes.Equal(readEntry(t, st, e.Key), data) {
		t.Fatal("content mismatch")
	}
	res, err := st.CollectGarbage()
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(res.Decisions, func(i, j int) bool { return res.Decisions[i].Digest < res.Decisions[j].Digest })
	if res.Deleted != 0 {
		t.Fatalf("nothing should be GC-able right after publish: %+v", res.Decisions)
	}
}
