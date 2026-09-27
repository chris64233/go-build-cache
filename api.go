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
//	POST   /v1/namespaces                     创建/更新命名空间配额
//	GET    /v1/namespaces                     列出命名空间配额状态
//	GET    /v1/namespaces/{name}              查询命名空间配额
//	DELETE /v1/namespaces/{name}              删除空命名空间
//	POST   /v1/sessions                       创建会话（可带 namespace）
//	POST   /v1/sessions/{id}/chunks/{index}   上传分片（raw body）
//	POST   /v1/sessions/{id}/renew            续期
//	POST   /v1/sessions/{id}/complete         校验并原子发布
//	DELETE /v1/sessions/{id}                  取消会话
//	GET    /v1/entries/{key...}               读取已发布内容（?namespace= 或 X-Namespace）
//	POST   /v1/pins                           固定
//	POST   /v1/pins/renew                     续租
//	POST   /v1/pins/unpin                     解除
//	GET    /v1/pins                           列出固定（?namespace=&active=）
//	GET    /v1/pins/{namespace}/{key...}      查询单个固定
//	POST   /v1/evictions                      建立淘汰决策（plan）
//	POST   /v1/evictions/{id}/commit          提交淘汰决策
//	GET    /v1/evictions                      列出淘汰决策
//	GET    /v1/evictions/{id}                 查询淘汰决策
//	GET    /v1/blobs/{digest}/refs            内容块引用查询
//	POST   /v1/gc                             触发一次垃圾回收（含到期固定扫描）
//	GET    /v1/audit                          查看审计记录
type Handler struct {
	Cache *Cache
	Mux   *http.ServeMux
}

// NewHandler 创建路由完备的 HTTP 处理器。
func NewHandler(c *Cache) *Handler {
	h := &Handler{Cache: c, Mux: http.NewServeMux()}
	h.Mux.HandleFunc("/v1/sessions", h.createSession)
	h.Mux.HandleFunc("/v1/sessions/", h.sessionSubroute)
	h.Mux.HandleFunc("/v1/namespaces", h.listNamespaces)
	h.Mux.HandleFunc("/v1/namespaces/", h.namespaceSubroute)
	h.Mux.HandleFunc("/v1/entries/", h.readEntry)
	h.Mux.HandleFunc("/v1/pins", h.pinCollection)
	h.Mux.HandleFunc("/v1/pins/", h.pinSubroute)
	h.Mux.HandleFunc("/v1/evictions", h.evictionCollection)
	h.Mux.HandleFunc("/v1/evictions/", h.evictionSubroute)
	h.Mux.HandleFunc("/v1/blobs/", h.blobRefs)
	h.Mux.HandleFunc("/v1/gc", h.collectGC)
	h.Mux.HandleFunc("/v1/audit", h.audit)
	h.Mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return h
}

// requestNamespace 解析命名空间：查询参数 namespace、X-Namespace 头，缺省为 default。
func requestNamespace(r *http.Request) string {
	if ns := r.URL.Query().Get("namespace"); ns != "" {
		return ns
	}
	if ns := r.Header.Get("X-Namespace"); ns != "" {
		return ns
	}
	return DefaultNamespace
}

func (h *Handler) createSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req struct {
		Namespace      string      `json:"namespace"`
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
		Namespace: req.Namespace, Key: req.Key, FinalDigest: d, TotalSize: req.TotalSize,
		Chunks: req.Chunks, IdempotencyKey: req.IdempotencyKey, LeaseDuration: lease,
	})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sess)
}

func (h *Handler) sessionSubroute(w http.ResponseWriter, r *http.Request) {
	// 路径形态：/v1/sessions/{id}[/{action}] 或 /{id}/chunks/{index}
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
	namespace := requestNamespace(r)
	key := strings.TrimPrefix(r.URL.Path, "/v1/entries/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "key required")
		return
	}
	reader, err := h.Cache.Read(namespace, key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+reader.Entry().Digest.String()+`"`)
	w.Header().Set("X-Cache-Namespace", reader.Entry().Namespace)
	w.Header().Set("X-Cache-Version", strconv.FormatUint(reader.Entry().Version, 10))
	w.Header().Set("X-Cache-Digest", reader.Entry().Digest.String())
	if _, err := io.Copy(w, reader); err != nil {
		return // 头部已发出，只能中断连接。
	}
}

// ---- 命名空间与配额 ----

func (h *Handler) listNamespaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		st, err := h.Cache.ListNamespaceStatus()
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, st)
	case http.MethodPost:
		var req struct {
			Name     string `json:"name"`
			MaxBytes int64  `json:"max_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		ns, err := h.Cache.SetNamespaceQuota(req.Name, req.MaxBytes)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, ns)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
	}
}

func (h *Handler) namespaceSubroute(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/namespaces/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, http.StatusNotFound, "not_found", "namespace name required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		st, err := h.Cache.NamespaceStatus(name)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, st)
	case http.MethodDelete:
		if err := h.Cache.DeleteNamespace(name); err != nil {
			writeCacheError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or DELETE")
	}
}

// ---- 固定租约 ----

func (h *Handler) pinCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		namespace := r.URL.Query().Get("namespace")
		activeOnly := r.URL.Query().Get("active") == "true" || r.URL.Query().Get("active") == "1"
		pins, err := h.Cache.ListPins(namespace, activeOnly)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, pins)
	case http.MethodPost:
		// 按 body 的 action 区分 pin / renew / unpin，缺省为 pin。
		var req struct {
			Action string `json:"action"`
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		_ = json.Unmarshal(body, &req)
		switch req.Action {
		case "", PinActionPin:
			h.pin(w, body)
		case PinActionRenew:
			h.renewPin(w, body)
		case PinActionUnpin:
			h.unpin(w, body)
		default:
			writeError(w, http.StatusBadRequest, "bad_request", "unknown pin action "+req.Action)
		}
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
	}
}

func (h *Handler) pin(w http.ResponseWriter, body []byte) {
	var req struct {
		Namespace       string  `json:"namespace"`
		Key             string  `json:"key"`
		ExpectedDigest  string  `json:"expected_digest,omitempty"`
		Deadline        string  `json:"deadline,omitempty"`    // RFC3339 绝对时间
		TTLSeconds      int64   `json:"ttl_seconds,omitempty"` // 或相对秒数
		ExpectedVersion *uint64 `json:"expected_version,omitempty"`
		RequestID       string  `json:"request_id,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	opts := PinOptions{
		Namespace: req.Namespace, Key: req.Key,
		ExpectedVersion: req.ExpectedVersion, RequestID: req.RequestID,
	}
	if req.ExpectedDigest != "" {
		d, err := ParseDigest(req.ExpectedDigest)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_digest", err.Error())
			return
		}
		opts.ExpectedDigest = d
	}
	if req.Deadline != "" {
		d, err := time.Parse(time.RFC3339Nano, req.Deadline)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "deadline must be RFC3339 time")
			return
		}
		opts.Deadline = d
	}
	if req.TTLSeconds > 0 {
		opts.TTL = time.Duration(req.TTLSeconds) * time.Second
	}
	pin, err := h.Cache.Pin(opts)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, pin)
}

func (h *Handler) renewPin(w http.ResponseWriter, body []byte) {
	var req struct {
		Namespace       string  `json:"namespace"`
		Key             string  `json:"key"`
		ExtendSeconds   int64   `json:"extend_seconds"`
		ExpectedVersion *uint64 `json:"expected_version,omitempty"`
		RequestID       string  `json:"request_id,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	pin, err := h.Cache.RenewPin(RenewPinOptions{
		Namespace: req.Namespace, Key: req.Key,
		Extend:          time.Duration(req.ExtendSeconds) * time.Second,
		ExpectedVersion: req.ExpectedVersion, RequestID: req.RequestID,
	})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pin)
}

func (h *Handler) unpin(w http.ResponseWriter, body []byte) {
	var req struct {
		Namespace       string  `json:"namespace"`
		Key             string  `json:"key"`
		ExpectedVersion *uint64 `json:"expected_version,omitempty"`
		RequestID       string  `json:"request_id,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	pin, err := h.Cache.Unpin(UnpinOptions{
		Namespace: req.Namespace, Key: req.Key,
		ExpectedVersion: req.ExpectedVersion, RequestID: req.RequestID,
	})
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pin)
}

func (h *Handler) pinSubroute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	// 形态：/v1/pins/{namespace}/{key...}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/pins/")
	ns, key, ok := strings.Cut(rest, "/")
	if !ok || ns == "" || key == "" {
		writeError(w, http.StatusNotFound, "not_found", "path must be /v1/pins/{namespace}/{key}")
		return
	}
	pin, err := h.Cache.GetPin(ns, key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pin)
}

// ---- 淘汰决策 ----

func (h *Handler) evictionCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ds, err := h.Cache.ListEvictionDecisions(r.URL.Query().Get("namespace"))
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, ds)
	case http.MethodPost:
		var req struct {
			Namespace string `json:"namespace"`
			NeedBytes int64  `json:"need_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		d, err := h.Cache.PlanEviction(req.Namespace, req.NeedBytes)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, d)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
	}
}

func (h *Handler) evictionSubroute(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/evictions/")
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 1 && parts[0] != "" && r.Method == http.MethodGet:
		d, err := h.Cache.GetEvictionDecision(parts[0])
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, d)
	case len(parts) == 2 && parts[1] == "commit" && r.Method == http.MethodPost:
		d, err := h.Cache.CommitEviction(parts[0])
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, d)
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown eviction route")
	}
}

// ---- 内容引用查询 ----

func (h *Handler) blobRefs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	// 形态：/v1/blobs/{digest}/refs
	rest := strings.TrimPrefix(r.URL.Path, "/v1/blobs/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "refs" {
		writeError(w, http.StatusNotFound, "not_found", "path must be /v1/blobs/{digest}/refs")
		return
	}
	d, err := ParseDigest(parts[0])
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
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ErrLeaseExpired):
		writeError(w, http.StatusGone, "lease_expired", err.Error())
	case errors.Is(err, ErrPinNotActive):
		writeError(w, http.StatusConflict, "pin_not_active", err.Error())
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
		var pdc *PinDigestConflictError
		var pc *PinConflictError
		var pvc *PinVersionConflictError
		var prc *PinRequestConflictError
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
		case errors.As(err, &pdc):
			writeError(w, http.StatusPreconditionFailed, "pin_digest_conflict", err.Error())
		case errors.As(err, &pvc):
			writeError(w, http.StatusConflict, "pin_version_conflict", err.Error())
		case errors.As(err, &prc):
			writeError(w, http.StatusConflict, "pin_request_conflict", err.Error())
		case errors.As(err, &pc):
			switch pc.Reason {
			case PinReasonNoEntry:
				writeError(w, http.StatusNotFound, "entry_not_found", err.Error())
			default:
				writeError(w, http.StatusBadRequest, "pin_conflict", err.Error())
			}
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
