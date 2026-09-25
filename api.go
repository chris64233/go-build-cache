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
//	POST   /v1/sessions                     创建会话
//	POST   /v1/sessions/{id}/chunks/{index} 上传分片（raw body）
//	POST   /v1/sessions/{id}/renew          续期
//	POST   /v1/sessions/{id}/complete       校验并原子发布
//	DELETE /v1/sessions/{id}                取消会话
//	GET    /v1/entries/{key...}             读取已发布内容
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
		Key: req.Key, FinalDigest: d, TotalSize: req.TotalSize,
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
	reader, err := h.Cache.Read(key)
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
	case errors.Is(err, ErrLeaseExpired):
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
