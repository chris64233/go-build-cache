package buildcache

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperationsAfterClose(t *testing.T) {
	st, _ := newTestStore(t)
	sess, parts := createSession(t, st, "k", []byte("xy"), 1)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	d := Digest(strings.Repeat("a", 64))
	cases := map[string]func() error{
		"create": func() error {
			_, e := st.CreateSession(CreateSessionInput{
				Key: "k2", Digest: d, TotalSize: 1,
				Chunks: []ChunkSpec{{Index: 0, Size: 1, Hash: d}},
			})
			return e
		},
		"put":      func() error { return st.PutChunkBytes(sess.ID, 0, parts[0]) },
		"complete": func() error { _, e := st.Complete(sess.ID); return e },
		"cancel":   func() error { return st.Cancel(sess.ID, "") },
		"renew":    func() error { _, e := st.RenewLease(sess.ID); return e },
		"gc":       func() error { _, e := st.CollectGarbage(); return e },
	}
	for name, fn := range cases {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Fatalf("%s after close: want ErrClosed, got %v", name, err)
		}
	}
	if n := st.ExpireSessions(); n != 0 { // 关闭后的批处理静默空转
		t.Fatalf("expire after close returned %d", n)
	}
	// 只读接口在关闭后仍可用（不修改状态）。
	if _, err := st.Get("k"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("get after close: %v", err)
	}
	if err := st.Close(); err != nil { // 重复关闭无害
		t.Fatalf("double close: %v", err)
	}
}

func TestRealClockAdvances(t *testing.T) {
	c := RealClock{}
	t1 := c.Now()
	time.Sleep(2 * time.Millisecond)
	if !c.Now().After(t1) {
		t.Fatal("RealClock.Now did not advance")
	}
	if d := Digest("nope"); d.Valid() {
		t.Fatal("invalid digest accepted")
	}
	if !EmptyDigest.Valid() {
		t.Fatal("empty digest constant invalid")
	}
}

// 上次运行中断留下的上传临时文件，在重新 Open 时必须被清掉，不能被枚举成内容块。
func TestCleansStagingTempFilesOnOpen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir, Options{Clock: NewFakeClock(time.Now())})
	if err != nil {
		t.Fatal(err)
	}
	// 手工塞一个遗留临时文件和一个正常块（后者保留）。
	content := []byte("survivor")
	d := sha256Hex(content)
	if _, _, ok, err := st.blobs.PutChecked(bytes.NewReader(content), d, int64(len(content))); err != nil || !ok {
		t.Fatalf("seed: %v %v", ok, err)
	}
	tmp, err := os.CreateTemp(filepath.Join(dir, "blobs"), ".tmp-leftover")
	if err != nil {
		t.Fatal(err)
	}
	tmp.WriteString("partial upload")
	tmp.Close()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := Open(dir, Options{Clock: NewFakeClock(time.Now())})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	all, err := st2.blobs.listAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0] != d {
		t.Fatalf("after reopen blobs = %v, want only survivor", all)
	}
	var leftovers []string
	filepath.WalkDir(filepath.Join(dir, "blobs"), func(path string, de os.DirEntry, err error) error {
		if err == nil && !de.IsDir() && bytes.HasPrefix([]byte(de.Name()), []byte(".tmp-")) {
			leftovers = append(leftovers, path)
		}
		return nil
	})
	if len(leftovers) != 0 {
		t.Fatalf("staging temp files survived reopen: %v", leftovers)
	}
}

func TestOpenMissingBlobErrors(t *testing.T) {
	st, _ := newTestStore(t)
	if _, _, err := st.blobs.Open(Digest(strings.Repeat("a", 64))); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("open missing blob: want ErrEntryNotFound, got %v", err)
	}
	if _, _, err := st.blobs.Open("bad"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("open bad digest: want ErrInvalidArgument, got %v", err)
	}
}
