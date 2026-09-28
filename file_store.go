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
//	  blobs/sha256/<ab>/<full-hex>      内容寻址块，临时文件 + rename 原子落盘
//	  sessions/<id>.json                会话元数据，临时文件 + rename 原子覆盖
//	  namespaces/<name>.json            命名空间配额
//	  entries/<ns>/<key-escaped>.json   已发布条目，临时文件 + rename 原子覆盖
//	  pins/<ns>/<key-escaped>.json      固定租约
//	  pin_requests/<request-id>.json    固定类请求号幂等记录
//	  evictions/<id>.json               淘汰决策
//	  promotions/<request-id>.json      跨命名空间晋级记录
//	  audit.log                         追加式审计日志（O_APPEND）
//
// 重启后状态可完整恢复。
type FileStore struct {
	root string
	mu   sync.Mutex // 串行化元数据写/删，避免并发 rename 互相干扰
}

// NewFileStore 打开（必要时创建）基于目录的持久化存储。
func NewFileStore(root string) (*FileStore, error) {
	for _, sub := range []string{
		"blobs", "sessions", "namespaces", "entries",
		"pins", "pin_requests", "evictions", "promotions",
	} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			return nil, fmt.Errorf("buildcache: init store: %w", err)
		}
	}
	f, err := os.OpenFile(filepath.Join(root, "audit.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("buildcache: init audit log: %w", err)
	}
	_ = f.Close()
	s := &FileStore{root: root}
	if err := s.migrateLegacyEntries(); err != nil {
		return nil, err
	}
	return s, nil
}

// migrateLegacyEntries 把 001 版本遗留的扁平 entries/<key>.json
// 迁移到 entries/default/<key>.json（命名空间化布局）。
func (s *FileStore) migrateLegacyEntries() error {
	dir := filepath.Join(s.root, "entries")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	dstDir := filepath.Join(dir, escapeKey(DefaultNamespace))
	for _, de := range entries {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".json") {
			continue
		}
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return err
		}
		old := filepath.Join(dir, de.Name())
		// 读取并补上默认命名空间，再以新布局原子落盘。
		data, err := os.ReadFile(old)
		if err != nil {
			return err
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		if e.Namespace == "" {
			e.Namespace = DefaultNamespace
		}
		if err := s.PutEntry(e, -2); err != nil { // -2：内部无条件落盘（迁移用）
			return err
		}
		if err := os.Remove(old); err != nil {
			return err
		}
	}
	return nil
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
	if sess.Namespace == "" {
		sess.Namespace = DefaultNamespace
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
		if sess.Namespace == "" {
			sess.Namespace = DefaultNamespace
		}
		out = append(out, sess)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ---- 命名空间 ----

func (s *FileStore) namespacePath(name string) string {
	return filepath.Join(s.root, "namespaces", escapeKey(name)+".json")
}

func (s *FileStore) SaveNamespace(ns Namespace) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(ns, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.namespacePath(ns.Name), data, 0o644)
}

func (s *FileStore) GetNamespace(name string) (Namespace, error) {
	var ns Namespace
	data, err := os.ReadFile(s.namespacePath(name))
	if errors.Is(err, os.ErrNotExist) {
		return Namespace{}, ErrNotFound
	}
	if err != nil {
		return Namespace{}, err
	}
	if err := json.Unmarshal(data, &ns); err != nil {
		return Namespace{}, err
	}
	return ns, nil
}

func (s *FileStore) ListNamespaces() ([]Namespace, error) {
	dir := filepath.Join(s.root, "namespaces")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Namespace
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var ns Namespace
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &ns); err != nil {
			return nil, err
		}
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *FileStore) DeleteNamespace(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.namespacePath(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ---- 条目（带版本条件的原子写入，身份为命名空间 + 键）----

func escapeKey(k string) string {
	// 与 URL path segment 兼容的转义，避免键里出现 "/"。
	repl := strings.NewReplacer("%", "%25", "/", "%2F", "\\", "%5C", " ", "%20")
	return repl.Replace(k)
}

func (s *FileStore) entryPath(namespace, key string) string {
	return filepath.Join(s.root, "entries", escapeKey(namespace), escapeKey(key)+".json")
}

// PutEntry 使用独立的 entry 互斥：读-检查-写整个过程对同键必须串行，
// 同时借助临时文件 rename 保证落盘原子性。
// wantVersion == -2 为迁移保留的"无条件覆盖"入口，业务代码不得使用。
func (s *FileStore) PutEntry(e Entry, wantVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.entryPath(e.Namespace, e.Key)
	existing, err := s.readEntry(e.Namespace, e.Key)
	switch {
	case errors.Is(err, ErrEntryNotFound):
		if wantVersion >= 0 {
			return ErrCASFailed
		}
	case err != nil:
		return err
	default:
		if wantVersion != -2 && (wantVersion < 0 || existing.Version != uint64(wantVersion)) {
			return ErrCASFailed
		}
	}
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(path, data, 0o644)
}

func (s *FileStore) readEntry(namespace, key string) (Entry, error) {
	var e Entry
	data, err := os.ReadFile(s.entryPath(namespace, key))
	if errors.Is(err, os.ErrNotExist) {
		return Entry{}, ErrEntryNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	if err := json.Unmarshal(data, &e); err != nil {
		return Entry{}, err
	}
	if e.Namespace == "" {
		e.Namespace = DefaultNamespace
	}
	return e, nil
}

func (s *FileStore) GetEntry(namespace, key string) (Entry, error) {
	return s.readEntry(namespace, key)
}

func (s *FileStore) DeleteEntry(namespace, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.entryPath(namespace, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListEntries() ([]Entry, error) {
	dir := filepath.Join(s.root, "entries")
	var out []Entry
	err := filepath.WalkDir(dir, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".json") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil // 容忍异常文件
		}
		if e.Namespace == "" {
			e.Namespace = DefaultNamespace
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// ---- 固定租约 ----

func (s *FileStore) pinPath(namespace, key string) string {
	return filepath.Join(s.root, "pins", escapeKey(namespace), escapeKey(key)+".json")
}

func (s *FileStore) SavePin(p Pin) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.pinPath(p.Namespace, p.Key), data, 0o644)
}

func (s *FileStore) readPin(namespace, key string) (Pin, error) {
	var p Pin
	data, err := os.ReadFile(s.pinPath(namespace, key))
	if errors.Is(err, os.ErrNotExist) {
		return Pin{}, ErrNotFound
	}
	if err != nil {
		return Pin{}, err
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return Pin{}, err
	}
	return p, nil
}

func (s *FileStore) GetPin(namespace, key string) (Pin, error) {
	return s.readPin(namespace, key)
}

func (s *FileStore) DeletePin(namespace, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.pinPath(namespace, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListPins() ([]Pin, error) {
	dir := filepath.Join(s.root, "pins")
	var out []Pin
	err := filepath.WalkDir(dir, func(path string, de os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".json") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var p Pin
		if err := json.Unmarshal(data, &p); err != nil {
			return nil
		}
		out = append(out, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortPins(out)
	return out, nil
}

func sortPins(out []Pin) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Key < out[j].Key
	})
}

// ---- 固定请求号 ----

func (s *FileStore) pinRequestPath(id string) string {
	return filepath.Join(s.root, "pin_requests", escapeKey(id)+".json")
}

func (s *FileStore) SavePinRequest(rec PinRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.pinRequestPath(rec.RequestID), data, 0o644)
}

func (s *FileStore) GetPinRequest(id string) (PinRequest, error) {
	var rec PinRequest
	data, err := os.ReadFile(s.pinRequestPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return PinRequest{}, ErrNotFound
	}
	if err != nil {
		return PinRequest{}, err
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return PinRequest{}, err
	}
	return rec, nil
}

func (s *FileStore) DeletePinRequest(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.pinRequestPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *FileStore) ListPinRequests() ([]PinRequest, error) {
	dir := filepath.Join(s.root, "pin_requests")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []PinRequest
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var rec PinRequest
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out, nil
}

// ---- 淘汰决策 ----

func (s *FileStore) decisionPath(id string) string {
	return filepath.Join(s.root, "evictions", escapeKey(id)+".json")
}

func (s *FileStore) SaveEvictionDecision(d EvictionDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.decisionPath(d.ID), data, 0o644)
}

func (s *FileStore) GetEvictionDecision(id string) (EvictionDecision, error) {
	var d EvictionDecision
	data, err := os.ReadFile(s.decisionPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return EvictionDecision{}, ErrNotFound
	}
	if err != nil {
		return EvictionDecision{}, err
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return EvictionDecision{}, err
	}
	return d, nil
}

func (s *FileStore) ListEvictionDecisions() ([]EvictionDecision, error) {
	dir := filepath.Join(s.root, "evictions")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []EvictionDecision
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var d EvictionDecision
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *FileStore) DeleteEvictionDecision(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.decisionPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ---- 跨命名空间晋级 ----

func (s *FileStore) promotionPath(requestID string) string {
	return filepath.Join(s.root, "promotions", escapeKey(requestID)+".json")
}

func (s *FileStore) SavePromotion(p Promotion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return s.writeAtomic(s.promotionPath(p.RequestID), data, 0o644)
}

func (s *FileStore) GetPromotion(requestID string) (Promotion, error) {
	var p Promotion
	data, err := os.ReadFile(s.promotionPath(requestID))
	if errors.Is(err, os.ErrNotExist) {
		return Promotion{}, ErrNotFound
	}
	if err != nil {
		return Promotion{}, err
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return Promotion{}, err
	}
	return p, nil
}

func (s *FileStore) ListPromotions() ([]Promotion, error) {
	dir := filepath.Join(s.root, "promotions")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Promotion
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		var p Promotion
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
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
