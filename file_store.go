package buildcache

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileStore 把所有状态持久化到一个目录：
//
//	root/
//	  blobs/sha256/<ab>/<full-hex>   内容寻址块，临时文件 + rename 原子落盘
//	  sessions/<id>.json             会话元数据，临时文件 + rename 原子覆盖
//	  entries/<key-escaped>.json     已发布条目，临时文件 + rename 原子覆盖
//	  audit.log                      追加式审计日志（O_APPEND）
//
// 重启后状态可完整恢复。
type FileStore struct {
	root string
	mu   sync.Mutex // 串行化元数据写/删，避免并发 rename 互相干扰
}

// NewFileStore 打开（必要时创建）基于目录的持久化存储。
func NewFileStore(root string) (*FileStore, error) {
	for _, sub := range []string{"blobs", "sessions", "entries"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			return nil, fmt.Errorf("buildcache: init store: %w", err)
		}
	}
	f, err := os.OpenFile(filepath.Join(root, "audit.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("buildcache: init audit log: %w", err)
	}
	_ = f.Close()
	return &FileStore{root: root}, nil
}

func (s *FileStore) blobPath(d Digest) string {
	prefix := d.Hex[:2]
	return filepath.Join(s.root, "blobs", d.Algo, prefix, d.Hex)
}

func (s *FileStore) writeAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path) // rename 在同一文件系统上是原子的
}

func (s *FileStore) PutBlob(d Digest, data []byte) error {
	if !d.Valid() {
		return errors.New("buildcache: invalid blob digest")
	}
	got, err := hashBytes(d.Algo, data)
	if err != nil {
		return err
	}
	if got != d {
		return &DigestMismatchError{Operation: "put_blob", Want: d, Got: got}
	}
	path := s.blobPath(d)
	// 已存在即视为幂等成功（内容寻址保证字节相同）。
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return s.writeAtomic(path, data, 0o644)
}

func (s *FileStore) GetBlob(d Digest) (io.ReadCloser, error) {
	f, err := os.Open(s.blobPath(d))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobNotFound
	}
	return f, err
}

func (s *FileStore) BlobSize(d Digest) (int64, error) {
	fi, err := os.Stat(s.blobPath(d))
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrBlobNotFound
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (s *FileStore) DeleteBlob(d Digest) error {
	err := os.Remove(s.blobPath(d))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListBlobs() ([]BlobInfo, error) {
	var out []BlobInfo
	blobsRoot := filepath.Join(s.root, "blobs")
	err := filepath.WalkDir(blobsRoot, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			return nil
		}
		name := de.Name()
		rel, err := filepath.Rel(blobsRoot, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 { // algo/ab/hex
			return nil
		}
		fi, err := de.Info()
		if err != nil {
			return err
		}
		d, err := ParseDigest(parts[0] + ":" + name)
		if err != nil {
			return nil // 容忍临时/异常文件
		}
		out = append(out, BlobInfo{Digest: d, Size: fi.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Digest.String() < out[j].Digest.String() })
	return out, nil
}

// ---- 会话 ----

func (s *FileStore) sessionPath(id string) string {
	return filepath.Join(s.root, "sessions", id+".json")
}

func (s *FileStore) SaveSession(sess Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(sess, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.sessionPath(sess.ID), data, 0o644)
}

func (s *FileStore) GetSession(id string) (Session, error) {
	var sess Session
	data, err := os.ReadFile(s.sessionPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return Session{}, ErrSessionNotFound
	}
	if err != nil {
		return Session{}, err
	}
	if err := json.Unmarshal(data, &sess); err != nil {
		return Session{}, err
	}
	return sess, nil
}

func (s *FileStore) DeleteSession(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.sessionPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListSessions() ([]Session, error) {
	dir := filepath.Join(s.root, "sessions")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Session
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var sess Session
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &sess); err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ---- 条目（带版本条件的原子写入）----

func escapeKey(k string) string {
	// 与 URL path segment 兼容的转义，避免键里出现 "/"。
	repl := strings.NewReplacer("%", "%25", "/", "%2F", "\\", "%5C", " ", "%20")
	return repl.Replace(k)
}

func (s *FileStore) entryPath(key string) string {
	return filepath.Join(s.root, "entries", escapeKey(key)+".json")
}

// PutEntry 使用独立的 entry 互斥：读-检查-写整个过程对同键必须串行，
// 同时借助临时文件 rename 保证落盘原子性。
func (s *FileStore) PutEntry(e Entry, wantVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.entryPath(e.Key)
	existing, err := s.readEntry(e.Key)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		if wantVersion >= 0 {
			return ErrCASFailed
		}
	case err != nil:
		return err
	default:
		if wantVersion < 0 || existing.Version != uint64(wantVersion) {
			return ErrCASFailed
		}
	}
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(path, data, 0o644)
}

func (s *FileStore) readEntry(key string) (Entry, error) {
	var e Entry
	data, err := os.ReadFile(s.entryPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return Entry{}, ErrEntryNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return Entry{}, err
	}
	return e, nil
}

func (s *FileStore) GetEntry(key string) (Entry, error) { return s.readEntry(key) }

func (s *FileStore) DeleteEntry(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.entryPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListEntries() ([]Entry, error) {
	dir := filepath.Join(s.root, "entries")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// ---- 审计日志（每行一条 JSON，原子追加）----

func (s *FileStore) AppendAudit(rec GCRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	f, err := os.OpenFile(filepath.Join(s.root, "audit.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

func (s *FileStore) ListAudit() ([]GCRecord, error) {
	f, err := os.Open(filepath.Join(s.root, "audit.log"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []GCRecord
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				var rec GCRecord
				if jerr := json.Unmarshal(line, &rec); jerr == nil {
					out = append(out, rec)
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *FileStore) Close() error { return nil }
