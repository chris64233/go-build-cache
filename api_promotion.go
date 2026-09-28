package buildcache

import (
	"encoding/json"
	"net/http"
	"strings"
)

// promotionCollection 处理 POST（建立晋级请求，可选立即提交）与 GET（列出）。
func (h *Handler) promotionCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			SourceNamespace string `json:"source_namespace"`
			SourceKey       string `json:"source_key"`
			ExpectedDigest  string `json:"expected_digest,omitempty"`
			TargetNamespace string `json:"target_namespace"`
			TargetKey       string `json:"target_key,omitempty"`
			Mode            string `json:"mode"`
			RequestID       string `json:"request_id"`
			// Commit 为 true 时建立后立即原子提交（典型调用路径），缺省即立即提交；
			// 显式传 false 只建立请求（proposed），稍后调用 commit。
			Commit *bool `json:"commit,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		opts := PromoteOptions{
			SourceNamespace: req.SourceNamespace, SourceKey: req.SourceKey,
			TargetNamespace: req.TargetNamespace, TargetKey: req.TargetKey,
			Mode: req.Mode, RequestID: req.RequestID,
		}
		if req.ExpectedDigest != "" {
			d, err := ParseDigest(req.ExpectedDigest)
			if err != nil {
				writeError(w, http.StatusBadRequest, "bad_digest", err.Error())
				return
			}
			opts.ExpectedDigest = d
		}
		shouldCommit := true
		if req.Commit != nil {
			shouldCommit = *req.Commit
		}
		if !shouldCommit {
			// 仅建立请求（proposed），稍后由 /commit 提交。
			proposed, err := h.Cache.CreatePromotion(opts)
			if err != nil {
				writeCacheError(w, err)
				return
			}
			writeJSON(w, http.StatusCreated, proposed)
			return
		}
		// 建立与提交在同一临界区内原子完成（典型调用路径）。
		committed, err := h.Cache.Promote(opts)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, committed)
	case http.MethodGet:
		ps, err := h.Cache.ListPromotions(r.URL.Query().Get("namespace"))
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, ps)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
	}
}

func (h *Handler) promotionSubroute(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/promotions/")
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 1 && parts[0] != "" && r.Method == http.MethodGet:
		p, err := h.Cache.GetPromotion(parts[0])
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, p)
	case len(parts) == 2 && parts[1] == "commit" && r.Method == http.MethodPost:
		p, err := h.Cache.CommitPromotion(parts[0])
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, p)
	default:
		writeError(w, http.StatusNotFound, "not_found", "unknown promotion route")
	}
}

// promotionLinks 查询某个条目通过晋级与另一端条目建立的共享关系。
func (h *Handler) promotionLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET")
		return
	}
	namespace := requestNamespace(r)
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "key query parameter is required")
		return
	}
	links, err := h.Cache.PromotionLinks(namespace, key)
	if err != nil {
		writeCacheError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, links)
}
