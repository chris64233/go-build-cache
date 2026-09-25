package buildcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// blobStore 是一个基于本地文件系统的内容寻址存储（CAS）。
//
// 每个内容块以其 SHA-256 摘要命名：blobs/<ab>/<64hex>。
// 写入采用 “写临时文件 -> fsync -> 原子 rename” 的方式，因此：
//   - 同内容并发写入天然幂等（rename 到同一最终路径）；
//   - 任何读取者都不可能读到半截内容；
//   - 未被引用的临时文件可安全清理。
type blobStore struct {
	dir string
}

func newBlobStore(root string) *blobStore {
	return &blobStore{dir: filepath.Join(root, "blobs")}
}

func (b *blobStore) ensure() error {
	return os.MkdirAll(b.dir, 0o755)
}

// path 返回摘要对应的最终路径。调用方需保证 d 合法。
func (b *blobStore) path(d Digest) string {
	s := string(d)
	return filepath.Join(b.dir, s[:2], s)
}

// exists 报告内容块是否已完整存在。
func (b *blobStore) exists(d Digest) (bool, error) {
	if !d.Valid() {
		return false, fmt.Errorf("%w: %q is not a valid sha256 digest", ErrInvalidArgument, d)
	}
	_, err := os.Stat(b.path(d))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// PutChecked 将 r 的全部内容写入暂存文件并计算其实际摘要与大小：
//   - 内容与声明一致（摘要、大小均匹配）时，原子发布到 CAS 最终路径并返回 accepted=true；
//   - 内容与声明不一致时，删除临时文件并返回 accepted=false 与实际摘要/大小
//     （CAS 中不会留下任何可见数据），由调用方决定这是“摘要不符”还是“幂等冲突”；
//   - 相同内容的重复/并发写入是幂等的。
func (b *blobStore) PutChecked(r io.Reader, expect Digest, expectSize int64) (got Digest, n int64, accepted bool, err error) {
	if !expect.Valid() {
		return "", 0, false, fmt.Errorf("%w: %q is not a valid sha256 digest", ErrInvalidArgument, expect)
	}
	if expectSize < 0 {
		return "", 0, false, fmt.Errorf("%w: negative chunk size", ErrInvalidArgument)
	}
	if err := b.ensure(); err != nil {
		return "", 0, false, err
	}

	// 临时文件放在与目标相同的目录下，保证 rename 是原子的同文件系统操作；
	// 在内容被接受之前，它对 listAll/Open 都不可见。
	f, err := os.CreateTemp(b.dir, ".tmp-*")
	if err != nil {
		return "", 0, false, err
	}
	tmpName := f.Name()
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(h, f), r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if copyErr != nil {
		os.Remove(tmpName)
		return "", 0, false, copyErr
	}
	if syncErr != nil {
		os.Remove(tmpName)
		return "", 0, false, syncErr
	}
	if closeErr != nil {
		os.Remove(tmpName)
		return "", 0, false, closeErr
	}
	got = Digest(hex.EncodeToString(h.Sum(nil)))

	if got != expect || n != expectSize {
		os.Remove(tmpName)
		return got, n, false, nil
	}

	final := b.path(expect)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		os.Remove(tmpName)
		return "", 0, false, err
	}
	if err := os.Rename(tmpName, final); err != nil {
		// 并发写入者可能抢先完成；若目标已存在（内容必然一致），视为成功。
		if ok, statErr := b.exists(expect); statErr == nil && ok {
			os.Remove(tmpName)
			return expect, n, true, nil
		}
		os.Remove(tmpName)
		return "", 0, false, err
	}
	return expect, n, true, nil
}

// Open 打开一个已存在的内容块供读取。
func (b *blobStore) Open(d Digest) (io.ReadCloser, int64, error) {
	if !d.Valid() {
		return nil, 0, fmt.Errorf("%w: %q is not a valid sha256 digest", ErrInvalidArgument, d)
	}
	f, err := os.Open(b.path(d))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("blob %s: %w", d, ErrEntryNotFound)
		}
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// stat 返回内容块大小；不存在时 ok=false。
func (b *blobStore) stat(d Digest) (size int64, ok bool, err error) {
	st, err := os.Stat(b.path(d))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return st.Size(), true, nil
}

// remove 删除内容块。不存在不视为错误。
func (b *blobStore) remove(d Digest) error {
	err := os.Remove(b.path(d))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// listAll 枚举所有已完整落盘的内容块摘要（忽略临时文件）。
func (b *blobStore) listAll() ([]Digest, error) {
	var out []Digest
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	for _, shard := range entries {
		if !shard.IsDir() || len(shard.Name()) != 2 || !isHex(shard.Name()) {
			continue
		}
		files, err := os.ReadDir(filepath.Join(b.dir, shard.Name()))
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || strings.HasPrefix(name, ".tmp-") || len(name) != 64 {
				continue
			}
			d := Digest(name)
			if d.Valid() {
				out = append(out, d)
			}
		}
	}
	return out, nil
}

// cleanTemp 删除所有遗留的临时文件（崩溃或中断的上传）。
func (b *blobStore) cleanTemp() error {
	return filepath.WalkDir(b.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			return os.Remove(path)
		}
		return nil
	})
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
