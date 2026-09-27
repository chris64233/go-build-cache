package buildcache

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Handler 把缓存服务暴露为一组 HTTP 接口：
//
//	POST   /v1/namespaces                          注册命名空间配额
//	GET    /v1/namespaces/                         列出命名空间
//	GET    /v1/namespaces/{ns}/quota               配额与用量
//	GET    /v1/namespaces/{ns}/eviction-order      LRU 淘汰候选查询
//	GET    /v1/namespaces/{ns}/entries/{key...}    读取命名空间内已发布内容
//	POST   /v1/namespaces/{ns}/pins/{key}          固定（租约）
//	POST   /v1/namespaces/{ns}/pins/{key}/renew    续租
//	POST   /v1/namespaces/{ns}/pins/{key}/release  解除
//	GET    /v1/namespaces/{ns}/pins[/{key}]        查询租约
//	POST   /v1/pins/sweep                          清除过期/已解除租约
//	GET    /v1/blobs/{digest}/references           内容块引用查询
//
//	POST   /v1/sessions                     创建会话
//	POST   /v1/sessions/{id}/chunks/{index} 上传分片（raw body）
//	POST   /v1/sessions/{id}/renew          续期
//	POST   /v1/sessions/{id}/complete       校验并原子发布
//	DELETE /v1/sessions/{id}                取消会话
//	GET    /v1/entries/{key...}             读取默认命名空间已发布内容
//	POST   /v1/gc                            触发一次垃圾回收
//	GET    /v1/audit                         查看审计记录
type Handler struct {
	Cache *Cache
	Mux   *http.ServeMux
}

// NewHandler 创建路由完备的 HTTP 处理器。
func NewHandler(c *Cache) *Handler {
	h := &Handler{Cache: c, Mux: http.NewServeMux()}
	h.Mux.HandleFunc("/v1/sessions", h.createSession)
	h.Mux.HandleFunc("/v1/sessions/", h.sessionSubroute)
	h.Mux.HandleFunc("/v1/entries/", h.readEntry)
	h.Mux.HandleFunc("/v1/namespaces", h.registerNamespace)
	h.Mux.HandleFunc("/v1/namespaces/", h.namespaceSubroute)
	h.Mux.HandleFunc("/v1/blobs/", h.blobReferences)
	h.Mux.HandleFunc("/v1/pins/sweep", h.sweepPins)
	h.Mux.HandleFunc("/v1/gc", h.collectGC)
	h.Mux.HandleFunc("/v1/audit", h.audit)
	h.Mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return h
}

func (h *Handler) createSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req struct {
		Namespace      string      `json:"namespace,omitempty"`
		Key            string      `json:"key"`
		FinalDigest    string      `json:"final_digest"`
		TotalSize      int64       `json:"total_size"`
		Chunks         []ChunkSpec `json:"chunks"`
		IdempotencyKey string      `json:"idempotency_key,omitempty"`
		LeaseSeconds   int64       `json:"lease_seconds,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d, err := ParseDigest(req.FinalDigest)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_digest", err.Error())
		return
	}
	var lease time.Duration
	if req.LeaseSeconds > 0 {
		lease = time.Duration(req.LeaseSeconds) * time.Second
	}
	sess, err := h.Cache.CreateSession(CreateSessionOptions{
		Namespace: req.Namespace,
		Key:       req.Key, FinalDigest: d, TotalSize: req.TotalSize,
		Chunks: req.Chunks, IdempotencyKey: req.IdempotencyKey, LeaseDuration: lease,
	})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sess)
}

func (h *Handler) sessionSubroute(w http.ResponseWriter, r *http.Request) {
	// 路径形态：/v1/sessions/{id}[/{action}]
	rest := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
	parts := strings.Split(rest, "/")
	if len(parts) < 1 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not_found", "session id required")
		return
	}
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch {
	case len(parts) == 3 && parts[1] == "chunks":
		h.uploadChunk(w, r, id, parts[2])
	case len(parts) == 2 && action == "complete":
		h.complete(w, r, id)
	case len(parts) == 2 && action == "renew":
		h.renew(w, r, id)
	case len(parts) == 1 && r.Method == http.MethodDelete:
		h.cancel(w, r, id)
	case len(parts) == 1 && r.Method == http.MethodGet:
		sess, err := h.Cache.GetSession(id)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, sess)
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown session route")
	}
}

func (h *Handler) uploadChunk(w http.ResponseWriter, r *http.Request, id, indexText string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	index, err := strconv.Atoi(indexText)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "chunk index must be an integer")
		return
	}
	// 限制单片大小由调用方在 http.Server 层配合 MaxBytesReader 完成。
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	rec, err := h.Cache.UploadChunk(id, index, data)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req struct {
		ExpectedVersion *uint64 `json:"expected_version,omitempty"`
	}
	// body 可为空。
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	result, err := h.Cache.Complete(id, CompleteOptions{ExpectedVersion: req.ExpectedVersion})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) renew(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req struct {
		ExtendSeconds int64 `json:"extend_seconds"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	exp, err := h.Cache.RenewSession(id, time.Duration(req.ExtendSeconds)*time.Second)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"expires_at": exp.UTC().Format(time.RFC3339Nano)})
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.Cache.Cancel(id); err != nil {
		writeCacheError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) readEntry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/entries/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "key required")
		return
	}
	reader, err := h.Cache.ReadDefault(key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+reader.Entry().Digest.String()+`"`)
	w.Header().Set("X-Cache-Version", strconv.FormatUint(reader.Entry().Version, 10))
	w.Header().Set("X-Cache-Digest", reader.Entry().Digest.String())
	if _, err := io.Copy(w, reader); err != nil {
		return // 头部已发出，只能中断连接。
	}
}

func (h *Handler) collectGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	report, err := h.Cache.CollectGarbage()
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// ---- 命名空间 / 配额 / 固定租约 / 引用查询 ----

func (h *Handler) registerNamespace(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
	case http.MethodGet:
		nss, err := h.Cache.ListNamespaces()
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, nss)
		return
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST or GET")
		return
	}
	var req struct {
		Name     string `json:"name"`
		MaxBytes int64  `json:"max_bytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	ns, err := h.Cache.RegisterNamespace(req.Name, req.MaxBytes)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, ns)
}

func (h *Handler) namespaceSubroute(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/namespaces/")
	parts := strings.Split(rest, "/")

	// GET /v1/namespaces/ —— 列出全部命名空间。
	if rest == "" || rest == "/" {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
			return
		}
		nss, err := h.Cache.ListNamespaces()
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, nss)
		return
	}

	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusNotFound, "not_found", "namespace subresource required")
		return
	}
	ns, resource := parts[0], parts[1]

	switch {
	case len(parts) == 2 && resource == "quota":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
			return
		}
		info, err := h.Cache.QuotaUsage(ns)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, info)

	case len(parts) == 2 && resource == "eviction-order":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
			return
		}
		candidates, err := h.Cache.EvictionOrder(ns)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, candidates)

	case len(parts) == 2 && resource == "evictions":
		h.evictions(w, r, ns)

	case resource == "entries":
		h.readNamespacedEntry(w, r, ns, strings.Join(parts[2:], "/"))

	case resource == "pins":
		h.pinSubroute(w, r, ns, parts[2:])

	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown namespace route")
	}
}

func (h *Handler) readNamespacedEntry(w http.ResponseWriter, r *http.Request, ns, key string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	if key == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "key required")
		return
	}
	reader, err := h.Cache.Read(ns, key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+reader.Entry().Digest.String()+`"`)
	w.Header().Set("X-Cache-Version", strconv.FormatUint(reader.Entry().Version, 10))
	w.Header().Set("X-Cache-Digest", reader.Entry().Digest.String())
	if _, err := io.Copy(w, reader); err != nil {
		return
	}
}

func (h *Handler) pinSubroute(w http.ResponseWriter, r *http.Request, ns string, tail []string) {
	switch {
	case len(tail) == 0:
		// GET /v1/namespaces/{ns}/pins[?all=1]
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
			return
		}
		pins, err := h.Cache.ListPins(ns, r.URL.Query().Get("all") == "1")
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, pins)

	case len(tail) == 1:
		key := tail[0]
		switch r.Method {
		case http.MethodGet:
			lease, err := h.Cache.GetPin(ns, key)
			if err != nil {
				writeCacheError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, lease)
		case http.MethodPost:
			h.pin(w, r, ns, key)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
		}

	case len(tail) >= 2 && r.Method == http.MethodPost &&
		(tail[len(tail)-1] == "renew" || tail[len(tail)-1] == "release"):
		// 键本身可以包含 "/"：最后一段是动作，其余全部拼成键。
		key := strings.Join(tail[:len(tail)-1], "/")
		if tail[len(tail)-1] == "renew" {
			h.renewPin(w, r, ns, key)
		} else {
			h.releasePin(w, r, ns, key)
		}

	case r.Method == http.MethodPost:
		// POST /v1/namespaces/{ns}/pins/{key...}：固定一个含 "/" 的键。
		h.pin(w, r, ns, strings.Join(tail, "/"))

	default:
		// GET /v1/namespaces/{ns}/pins/{key...}：键可以包含 "/"。
		if r.Method != http.MethodGet {
			writeError(w, http.StatusNotFound, "not_found", "unknown pin route")
			return
		}
		key := strings.Join(tail, "/")
		lease, err := h.Cache.GetPin(ns, key)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, lease)
	}
}

// evictions 暴露两阶段淘汰：
//   - POST /v1/namespaces/{ns}/evictions        body {"need_bytes": N}：选定候选（第一阶段）
//   - POST /v1/namespaces/{ns}/evictions/commit body 为第一阶段返回的决策对象：确认并删除
func (h *Handler) evictions(w http.ResponseWriter, r *http.Request, ns string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	commit := r.URL.Query().Get("commit") == "1"
	if commit {
		var d EvictionDecision
		if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if d.Namespace == "" {
			d.Namespace = ns
		}
		if d.Namespace != ns {
			writeError(w, http.StatusBadRequest, "bad_request", "decision namespace mismatch")
			return
		}
		report, err := h.Cache.CommitEviction(&d)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}
	var req struct {
		NeedBytes int64 `json:"need_bytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d, err := h.Cache.PlanEviction(ns, req.NeedBytes)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func (h *Handler) pin(w http.ResponseWriter, r *http.Request, ns, key string) {
	var req struct {
		ExpectedDigest string  `json:"expected_digest"`
		ExpiresAt      string  `json:"expires_at,omitempty"`
		TTLSeconds     float64 `json:"ttl_seconds,omitempty"`
		RequestID      string  `json:"request_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	d, err := ParseDigest(req.ExpectedDigest)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_digest", err.Error())
		return
	}
	opts := PinOptions{Namespace: ns, Key: key, ExpectedDigest: d, RequestID: req.RequestID}
	if req.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339Nano, req.ExpiresAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "expires_at must be RFC3339")
			return
		}
		opts.ExpiresAt = t
	}
	if req.TTLSeconds > 0 {
		opts.TTL = time.Duration(req.TTLSeconds * float64(time.Second))
	}
	res, err := h.Cache.Pin(opts)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, res)
}

func (h *Handler) renewPin(w http.ResponseWriter, r *http.Request, ns, key string) {
	var req struct {
		ExpiresAt       string  `json:"expires_at,omitempty"`
		TTLSeconds      float64 `json:"ttl_seconds,omitempty"`
		ExpectedVersion *uint64 `json:"expected_version"`
		RequestID       string  `json:"request_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	opts := PinOptions{Namespace: ns, Key: key, ExpectedVersion: req.ExpectedVersion, RequestID: req.RequestID}
	if req.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339Nano, req.ExpiresAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "expires_at must be RFC3339")
			return
		}
		opts.ExpiresAt = t
	}
	if req.TTLSeconds > 0 {
		opts.TTL = time.Duration(req.TTLSeconds * float64(time.Second))
	}
	res, err := h.Cache.RenewPin(opts)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) releasePin(w http.ResponseWriter, r *http.Request, ns, key string) {
	var req struct {
		ExpectedVersion *uint64 `json:"expected_version"`
		RequestID       string  `json:"request_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	res, err := h.Cache.ReleasePin(PinOptions{
		Namespace: ns, Key: key, ExpectedVersion: req.ExpectedVersion, RequestID: req.RequestID,
	})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) sweepPins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	swept, err := h.Cache.SweepExpiredPins()
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, swept)
}

func (h *Handler) blobReferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, "/v1/blobs/")
	// 形如 {digest}/references。
	if !strings.HasSuffix(tail, "/references") {
		writeError(w, http.StatusNotFound, "not_found", "use /v1/blobs/{digest}/references")
		return
	}
	digestText := strings.TrimSuffix(tail, "/references")
	d, err := ParseDigest(digestText)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_digest", err.Error())
		return
	}
	refs, err := h.Cache.BlobReferences(d)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, refs)
}

func (h *Handler) audit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	records, err := h.Cache.AuditLog()
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, records)
}

// ---- 错误映射：把领域错误翻译成稳定的错误码与 HTTP 状态 ----

func writeCacheError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSessionNotFound):
		writeError(w, http.StatusNotFound, "session_not_found", err.Error())
	case errors.Is(err, ErrEntryNotFound):
		writeError(w, http.StatusNotFound, "entry_not_found", err.Error())
	case errors.Is(err, ErrNamespaceNotFound):
		writeError(w, http.StatusNotFound, "namespace_not_found", err.Error())
	case errors.Is(err, ErrPinNotFound):
		writeError(w, http.StatusNotFound, "pin_not_found", err.Error())
	case errors.Is(err, ErrPinned):
		writeError(w, http.StatusConflict, "entry_pinned", err.Error())
	case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrPinExpired):
		writeError(w, http.StatusGone, "lease_expired", err.Error())
	case errors.Is(err, ErrSessionNotActive):
		writeError(w, http.StatusConflict, "session_not_active", err.Error())
	case errors.Is(err, ErrChunkNotDeclared):
		writeError(w, http.StatusBadRequest, "chunk_not_declared", err.Error())
	default:
		var dm *DigestMismatchError
		var cc *ChunkConflictError
		var vc *VersionConflictError
		var ic *IdempotencyConflictError
		var qe *QuotaExceededError
		var pc *PinConflictError
		var pv *PinVersionConflictError
		var rc *RequestConflictError
		switch {
		case errors.As(err, &dm):
			writeError(w, http.StatusUnprocessableEntity, "digest_mismatch", err.Error())
		case errors.As(err, &cc):
			status := http.StatusUnprocessableEntity
			code := "chunk_conflict"
			if cc.Reason == ReasonIncomplete {
				status = http.StatusConflict
				code = "chunks_incomplete"
			}
			writeError(w, status, code, err.Error())
		case errors.As(err, &vc):
			writeError(w, http.StatusPreconditionFailed, "version_conflict", err.Error())
		case errors.As(err, &ic):
			writeError(w, http.StatusConflict, "idempotency_conflict", err.Error())
		case errors.As(err, &qe):
			writeError(w, http.StatusInsufficientStorage, "quota_exceeded", err.Error())
		case errors.As(err, &pc):
			writeError(w, http.StatusPreconditionFailed, "pin_conflict", err.Error())
		case errors.As(err, &pv):
			writeError(w, http.StatusPreconditionFailed, "pin_version_conflict", err.Error())
		case errors.As(err, &rc):
			writeError(w, http.StatusConflict, "request_conflict", err.Error())
		default:
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}
