package buildcache

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// publishHTTP 通过 HTTP 在指定命名空间发布一个条目。
func publishHTTP(t *testing.T, base, namespace, key string, data []byte, chunkSize int) {
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
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("create %s/%s status %d: %s", namespace, key, resp.StatusCode, raw)
	}
	var sess Session
	json.NewDecoder(resp.Body).Decode(&sess)
	resp.Body.Close()
	for _, sp := range plan(t, data, chunkSize) {
		uploadChunk(t, base, sess.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size])
	}
	cresp, err := http.Post(base+"/v1/sessions/"+sess.ID+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	defer cresp.Body.Close()
	if cresp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(cresp.Body)
		t.Fatalf("complete status %d: %s", cresp.StatusCode, raw)
	}
}

// 完整 HTTP 生命周期：copy 晋级、查询、两端关系、内容共享。
func TestAPIPromotionCopy(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 0})

	data := []byte("http promotion content!!!") // 25
	publishHTTP(t, srv.URL, "src", "art", data, 8)

	status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"expected_digest":  digestOf(data).String(),
		"target_namespace": "dst", "target_key": "art-copy",
		"mode": PromoteCopy, "request_id": "http-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("promote = %d %v", status, body)
	}
	if body["status"] != PromotionCommitted {
		t.Fatalf("body = %v", body)
	}
	re := body["result_entry"].(map[string]any)
	if re["version"].(float64) != 1 || re["namespace"] != "dst" {
		t.Fatalf("result_entry = %v", re)
	}

	// 目标内容可读且与来源一致。
	resp, err := http.Get(srv.URL + "/v1/entries/art-copy?namespace=dst")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("read target = %d %q", resp.StatusCode, got)
	}

	// 查询单条记录。
	if status, b := getJSON(t, srv.URL, "/v1/promotions/http-1"); status != http.StatusOK ||
		b["request_id"] != "http-1" {
		t.Fatalf("get promotion = %d %v", status, b)
	}
	// 列表。
	var listed []map[string]any
	r2, _ := http.Get(srv.URL + "/v1/promotions")
	json.NewDecoder(r2.Body).Decode(&listed)
	r2.Body.Close()
	if len(listed) != 1 {
		t.Fatalf("listed = %d", len(listed))
	}

	// 两端关系查询。
	r3, err := http.Get(srv.URL + "/v1/promotion-links?namespace=src&key=art")
	if err != nil {
		t.Fatal(err)
	}
	var srcLinks []map[string]any
	json.NewDecoder(r3.Body).Decode(&srcLinks)
	r3.Body.Close()
	if len(srcLinks) != 1 || srcLinks[0]["role"] != PromotionRoleTarget ||
		srcLinks[0]["key"] != "art-copy" {
		t.Fatalf("source links = %+v", srcLinks)
	}
}

// 两阶段：commit=false 只建立请求，随后访问候选导致提交失败（412 promotion_stale）。
func TestAPIPromotionTwoPhaseStale(t *testing.T) {
	srv, clk := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 30})
	publishHTTP(t, srv.URL, "dst", "a", []byte("aaaaaaaaaa"), 10)
	publishHTTP(t, srv.URL, "dst", "b", []byte("bbbbbbbbbb"), 10)
	srcData := make([]byte, 20)
	publishHTTP(t, srv.URL, "src", "art", srcData, 10)

	// 只建立。
	status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"target_namespace": "dst", "target_key": "k",
		"mode": PromoteCopy, "request_id": "http-2", "commit": false,
	})
	if status != http.StatusCreated || body["status"] != PromotionProposed {
		t.Fatalf("create = %d %v", status, body)
	}

	// 提交前访问候选 a。
	clk.Advance(time.Minute)
	if r, err := http.Get(srv.URL + "/v1/entries/a?namespace=dst"); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("read candidate: %v", err)
	}

	// 提交 -> 412 promotion_stale。
	cresp, err := http.Post(srv.URL+"/v1/promotions/http-2/commit", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	defer cresp.Body.Close()
	if cresp.StatusCode != http.StatusPreconditionFailed {
		raw, _ := io.ReadAll(cresp.Body)
		t.Fatalf("commit status = %d: %s", cresp.StatusCode, raw)
	}
	var eb map[string]any
	json.NewDecoder(cresp.Body).Decode(&eb)
	if eb["error"] != "promotion_stale" {
		t.Fatalf("error body = %v", eb)
	}
}

// 错误码：来源不存在 404；摘要不符 412；copy 目标已存在 409；同号异内容 409。
func TestAPIPromotionErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 0})
	data := []byte("http error cases!!")
	publishHTTP(t, srv.URL, "src", "art", data, 7)
	publishHTTP(t, srv.URL, "dst", "taken", data, 7)

	// 来源不存在。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "ghost",
		"target_namespace": "dst", "target_key": "g",
		"mode": PromoteCopy, "request_id": "e1",
	}); status != http.StatusNotFound || body["error"] != "source_entry_not_found" {
		t.Fatalf("missing source = %d %v", status, body)
	}

	// 摘要不符。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"expected_digest":  digestOf([]byte("other")).String(),
		"target_namespace": "dst", "target_key": "g",
		"mode": PromoteCopy, "request_id": "e2",
	}); status != http.StatusPreconditionFailed || body["error"] != "promotion_digest_mismatch" {
		t.Fatalf("digest mismatch = %d %v", status, body)
	}

	// copy 目标已存在。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"target_namespace": "dst", "target_key": "taken",
		"mode": PromoteCopy, "request_id": "e3",
	}); status != http.StatusConflict || body["error"] != "promotion_target_exists" {
		t.Fatalf("target exists = %d %v", status, body)
	}

	// 正常晋级一次。
	postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"target_namespace": "dst", "target_key": "k",
		"mode": PromoteCopy, "request_id": "e4",
	})
	// 同号异内容（换目标键）-> 409。
	if status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"target_namespace": "dst", "target_key": "other",
		"mode": PromoteCopy, "request_id": "e4",
	}); status != http.StatusConflict || body["error"] != "promotion_request_conflict" {
		t.Fatalf("request conflict = %d %v", status, body)
	}

	// 未知请求号提交 -> 404。
	if r, err := http.Post(srv.URL+"/v1/promotions/nope/commit", "application/json",
		bytes.NewReader([]byte("{}"))); err != nil || r.StatusCode != http.StatusNotFound {
		t.Fatalf("commit unknown = %v %d", err, r.StatusCode)
	}
}

// 配额淘汰随晋级提交返回，块引用查询能看到两个命名空间的 entry 引用。
func TestAPIPromotionQuotaAndRefs(t *testing.T) {
	srv, _ := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "src", "max_bytes": 0})
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "dst", "max_bytes": 30})
	publishHTTP(t, srv.URL, "dst", "a", []byte("aaaaaaaaaa"), 10)
	publishHTTP(t, srv.URL, "dst", "b", []byte("bbbbbbbbbb"), 10)
	srcData := make([]byte, 20)
	publishHTTP(t, srv.URL, "src", "art", srcData, 10)

	status, body := postJSON(t, srv.URL, "/v1/promotions", map[string]any{
		"source_namespace": "src", "source_key": "art",
		"target_namespace": "dst", "target_key": "k",
		"mode": PromoteCopy, "request_id": "http-q",
	})
	if status != http.StatusCreated {
		t.Fatalf("promote = %d %v", status, body)
	}
	ev := body["eviction"].(map[string]any)
	cands := ev["candidates"].([]any)
	if len(cands) != 1 || cands[0].(map[string]any)["key"] != "a" {
		t.Fatalf("eviction = %v", ev)
	}
	if body["used_after"].(float64) != 30 {
		t.Fatalf("used_after = %v", body["used_after"])
	}

	// a 已淘汰。
	if r, _ := http.Get(srv.URL + "/v1/entries/a?namespace=dst"); r.StatusCode != http.StatusNotFound {
		t.Fatalf("a not evicted: %d", r.StatusCode)
	}

	// 共享块引用查询：取来源/目标共有的一个块，应看到两个 entry 引用。
	d := digestOf(srcData[0:10]).String()
	r2, err := http.Get(srv.URL + "/v1/blobs/" + d + "/refs")
	if err != nil {
		t.Fatal(err)
	}
	var refs []map[string]any
	json.NewDecoder(r2.Body).Decode(&refs)
	r2.Body.Close()
	entries := 0
	for _, ref := range refs {
		if ref["kind"] == RefEntry {
			entries++
		}
	}
	if entries != 2 {
		t.Fatalf("shared chunk entry refs = %d (%+v)", entries, refs)
	}
}
