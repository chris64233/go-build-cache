package buildcache

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"sync"
)

// MemoryStore 是进程内 Store 实现，主要用于测试。
type MemoryStore struct {
	mu         sync.Mutex
	blobs      map[string][]byte
	sessions   map[string]Session
	entries    map[string]Entry // 复合键：namespace + "\x00" + key
	namespaces map[string]Namespace
	pins       map[string]PinLease // 复合键：namespace + "\x00" + key
	audit      []GCRecord
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		blobs:      make(map[string][]byte),
		sessions:   make(map[string]Session),
		entries:    make(map[string]Entry),
		namespaces: make(map[string]Namespace),
		pins:       make(map[string]PinLease),
	}
}

func entryMapKey(namespace, key string) string { return namespace + "\x00" + key }
func pinMapKey(namespace, key string) string   { return namespace + "\x00" + key }

func (m *MemoryStore) PutBlob(d Digest, data []byte) error {
	if !d.Valid() {
		return errors.New("buildcache: invalid blob digest")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.blobs[d.String()] = cp // 内容寻址：同摘要重传直接覆盖为相同内容，天然幂等
	return nil
}

func (m *MemoryStore) GetBlob(d Digest) (io.ReadCloser, error) {
	m.mu.Lock()
	data, ok := m.blobs[d.String()]
	m.mu.Unlock()
	if !ok {
		return nil, ErrBlobNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryStore) BlobSize(d Digest) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[d.String()]
	if !ok {
		return 0, ErrBlobNotFound
	}
	return int64(len(data)), nil
}

func (m *MemoryStore) DeleteBlob(d Digest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, d.String())
	return nil
}

func (m *MemoryStore) ListBlobs() ([]BlobInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]BlobInfo, 0, len(m.blobs))
	for k, data := range m.blobs {
		d, err := ParseDigest(k)
		if err != nil {
			return nil, err
		}
		out = append(out, BlobInfo{Digest: d, Size: int64(len(data))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Digest.String() < out[j].Digest.String() })
	return out, nil
}

func (m *MemoryStore) SaveSession(s Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = cloneSession(s)
	return nil
}

func (m *MemoryStore) GetSession(id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	return cloneSession(s), nil
}

func (m *MemoryStore) DeleteSession(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
	return nil
}

func (m *MemoryStore) ListSessions() ([]Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, cloneSession(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *MemoryStore) PutEntry(e Entry, wantVersion int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ek := entryMapKey(e.Namespace, e.Key)
	existing, ok := m.entries[ek]
	if !ok {
		if wantVersion >= 0 {
			return ErrCASFailed
		}
	} else {
		if wantVersion < 0 || existing.Version != uint64(wantVersion) {
			return ErrCASFailed
		}
	}
	m.entries[ek] = cloneEntry(e)
	return nil
}

func (m *MemoryStore) GetEntry(namespace, key string) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[entryMapKey(namespace, key)]
	if !ok {
		return Entry{}, ErrEntryNotFound
	}
	return cloneEntry(e), nil
}

func (m *MemoryStore) DeleteEntry(namespace, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, entryMapKey(namespace, key))
	return nil
}

func (m *MemoryStore) ListEntries() ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Entry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, cloneEntry(e))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func (m *MemoryStore) ListNamespaceEntries(namespace string) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for _, e := range m.entries {
		if e.Namespace == namespace {
			out = append(out, cloneEntry(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// ---- 命名空间 ----

func (m *MemoryStore) SaveNamespace(ns Namespace) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.namespaces[ns.Name] = ns
	return nil
}

func (m *MemoryStore) GetNamespace(name string) (Namespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns, ok := m.namespaces[name]
	if !ok {
		return Namespace{}, ErrNamespaceNotFound
	}
	return ns, nil
}

func (m *MemoryStore) ListNamespaces() ([]Namespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Namespace, 0, len(m.namespaces))
	for _, ns := range m.namespaces {
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ---- 固定租约 ----

func (m *MemoryStore) SavePin(p PinLease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pins[pinMapKey(p.Namespace, p.Key)] = clonePin(p)
	return nil
}

func (m *MemoryStore) GetPin(namespace, key string) (PinLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pins[pinMapKey(namespace, key)]
	if !ok {
		return PinLease{}, ErrPinNotFound
	}
	return clonePin(p), nil
}

func (m *MemoryStore) DeletePin(namespace, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pins, pinMapKey(namespace, key))
	return nil
}

func (m *MemoryStore) ListPins() ([]PinLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PinLease, 0, len(m.pins))
	for _, p := range m.pins {
		out = append(out, clonePin(p))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func cloneEntry(e Entry) Entry {
	cp := e
	cp.Chunks = append([]ChunkRef(nil), e.Chunks...)
	return cp
}

func clonePin(p PinLease) PinLease {
	cp := p
	cp.Chunks = append([]ChunkRef(nil), p.Chunks...)
	return cp
}

func (m *MemoryStore) AppendAudit(rec GCRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = append(m.audit, rec)
	return nil
}

func (m *MemoryStore) ListAudit() ([]GCRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]GCRecord, len(m.audit))
	copy(out, m.audit)
	return out, nil
}

func (m *MemoryStore) Close() error { return nil }

func cloneSession(s Session) Session {
	cp := s
	cp.Chunks = append([]ChunkSpec(nil), s.Chunks...)
	cp.Received = make(map[int]ChunkReceipt, len(s.Received))
	for k, v := range s.Received {
		cp.Received[k] = v
	}
	return cp
}
