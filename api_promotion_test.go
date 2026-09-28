package buildcache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// publishViaHTTP 在给定命名空间通过 HTTP 发布 data。
func publishViaHTTP(t *testing.T, base, namespace, key string, data []byte, chunkSize int) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{
		"namespace": namespace, "key": key,
		"final_digest": digestOf(data).String(), "total_size": len(data),
		"chunks": plan(t, data, chunkSize),
	})
	resp, err := http.Post(base+"/v1/sessions", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	var sess Session
	json.NewDecoder(resp.Body).Decode(&sess)
	resp.Body.Close()
	for _, sp := range plan(t, data, chunkSize) {
		uploadChunk(t, base, sess.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size])
	}
	r2, err := http.Post(base+"/v1/sessions/"+sess.ID+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
}

// 晋级完整 HTTP 生命周期：复制、详情查询、请求号幂等、错误码。
func TestAPIPromotionLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 1000})

	data := []byte("http promotion payload!") // 22
	publishViaHTTP(t, srv.URL, "src", "mod/x", data, 11)

	// 正常晋级（默认 commit）。
	status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "mod/x",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "mod/y",
		"mode": PromotionModeCopy, "request_id": "promo-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("promote = %d %v", status, body)
	}
	pid, _ := body["id"].(string)
	if body["status"] != PromotionCommitted {
		t.Fatalf("status = %v", body["status"])
	}

	// 目标可读且内容一致。
	if r, err := http.Get(srv.URL + "/v1/entries/mod/y?namespace=dst"); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("read target: %v %d", err, r.StatusCode)
	} else {
		r.Body.Close()
	}

	// 详情：两端关系 + 配额 + 内容来源。
	status, detail := getJSON(t, srv.URL, "/v1/promotions/"+pid)
	if status != http.StatusOK {
		t.Fatalf("detail = %d", status)
	}
	srcEnd := detail["source"].(map[string]any)
	tgtEnd := detail["target"].(map[string]any)
	if srcEnd["namespace"] != "src" || tgtEnd["namespace"] != "dst" {
		t.Fatalf("ends = %+v %+v", srcEnd, tgtEnd)
	}
	quota := detail["quota"].(map[string]any)
	if quota["used_after"].(float64) != float64(len(data)) {
		t.Fatalf("quota = %+v", quota)
	}
	chunks := detail["chunks"].([]any)
	if len(chunks) != len(plan(t, data, 11)) {
		t.Fatalf("chunks = %d", len(chunks))
	}

	// 同请求号重放 -> 同一晋级 ID。
	_, body2 := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "mod/x",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "mod/y",
		"mode": PromotionModeCopy, "request_id": "promo-1",
	})
	if body2["id"] != pid {
		t.Fatalf("idempotent replay changed id: %v", body2["id"])
	}

	// 列表过滤。
	if r, err := http.Get(srv.URL + "/v1/promotions?namespace=dst&status=committed"); err != nil {
		t.Fatal(err)
	} else {
		var ps []map[string]any
		json.NewDecoder(r.Body).Decode(&ps)
		r.Body.Close()
		if len(ps) != 1 {
			t.Fatalf("list = %+v", ps)
		}
	}
}

// 错误码：来源摘要不符 -> 412；来源不存在 -> 404；同号异内容 -> 409。
func TestAPIPromotionErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 1000})
	data := []byte("promo error data!!!")
	publishViaHTTP(t, srv.URL, "src", "k", data, 6)

	// 来源不存在。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "ghost",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "g", "mode": PromotionModeCopy,
	}); status != http.StatusNotFound || body["error"] != "entry_not_found" {
		t.Fatalf("missing source = %d %v", status, body)
	}

	// 摘要不符。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "k",
		"source_digest":    digestOf([]byte("other")).String(),
		"target_namespace": "dst", "target_key": "g", "mode": PromotionModeCopy,
	}); status != http.StatusPreconditionFailed || body["error"] != "promotion_digest_conflict" {
		t.Fatalf("digest conflict = %d %v", status, body)
	}

	// 正常一次。
	postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "k",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "g",
		"mode": PromotionModeCopy, "request_id": "r1",
	})

	// 同号不同目标 -> 409。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "k",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "different",
		"mode": PromotionModeCopy, "request_id": "r1",
	}); status != http.StatusConflict || body["error"] != "promotion_request_conflict" {
		t.Fatalf("request conflict = %d %v", status, body)
	}
}

// 两阶段：只建立（commit:false）得到 prepared，再 commit 提交。
func TestAPIPromotionTwoPhase(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 1000})
	data := []byte("two phase promo!!!")
	publishViaHTTP(t, srv.URL, "src", "k", data, 6)

	status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "k",
		"source_digest":    digestOf(data).String(),
		"target_namespace": "dst", "target_key": "k",
		"mode": PromotionModeCopy, "commit": false,
	})
	if status != http.StatusCreated || body["status"] != PromotionPrepared {
		t.Fatalf("prepare = %d %v", status, body)
	}
	pid := body["id"].(string)

	// prepared 期间目标尚不可读。
	if r, _ := http.Get(srv.URL + "/v1/entries/k?namespace=dst"); r.StatusCode != http.StatusNotFound {
		t.Fatalf("target visible before commit: %d", r.StatusCode)
	}

	r2, err := http.Post(srv.URL+"/v1/promotions/"+pid+"/commit", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	var committed map[string]any
	json.NewDecoder(r2.Body).Decode(&committed)
	r2.Body.Close()
	if committed["status"] != PromotionCommitted {
		t.Fatalf("commit = %+v", committed)
	}
	if r, _ := http.Get(srv.URL + "/v1/entries/k?namespace=dst"); r.StatusCode != http.StatusOK {
		r.Body.Close()
		t.Fatalf("target not readable after commit: %d", r.StatusCode)
	} else {
		r.Body.Close()
	}
}
