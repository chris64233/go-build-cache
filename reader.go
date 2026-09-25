package buildcache

import (
	"io"
	"sort"
)

// EntryReader 按分片顺序拼接读取一个已发布条目的内容。
// 读取过程中若某个块恰好被 GC 删除（实现应阻止这种情况），
// Read 会返回来自存储层的错误。
type EntryReader struct {
	store  Store
	entry  Entry
	cur    int // 当前分片下标（按 Index 排序后）
	rc     io.ReadCloser
	closed bool
}

func newEntryReader(store Store, e Entry) *EntryReader {
	chunks := append([]ChunkRef(nil), e.Chunks...)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Index < chunks[j].Index })
	e.Chunks = chunks
	return &EntryReader{store: store, entry: e}
}

// Entry 返回该读者对应的条目元数据。
func (r *EntryReader) Entry() Entry { return r.entry }

func (r *EntryReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	for r.cur < len(r.entry.Chunks) {
		if r.rc == nil {
			rc, err := r.store.GetBlob(r.entry.Chunks[r.cur].Digest)
			if err != nil {
				return 0, err
			}
			r.rc = rc
		}
		n, err := r.rc.Read(p)
		if err == io.EOF {
			_ = r.rc.Close()
			r.rc = nil
			r.cur++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
	return 0, io.EOF
}

// Close 释放当前打开的块句柄。
func (r *EntryReader) Close() error {
	r.closed = true
	if r.rc != nil {
		err := r.rc.Close()
		r.rc = nil
		return err
	}
	return nil
}

var _ io.ReadCloser = (*EntryReader)(nil)
