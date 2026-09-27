package buildcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func postJSON(t *testing.T, base, path string, body any) (int, map[string]any) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func getJSON(t *testing.T, base, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// 命名空间配额：设置、查询、超限发布返回 507。
func TestAPINamespaceQuotaAndExceeded(t *testing.T) {
	srv, _ := newTestServer(t)

	if status, body := postJSON(t, srv.URL, "/v1/namespaces",
		map[string]any{"name": "ns", "max_bytes": 20}); status != http.StatusCreated || body["name"] != "ns" {
		t.Fatalf("create namespace = %d %v", status, body)
	}
	status, body := getJSON(t, srv.URL, "/v1/namespaces/ns")
	if status != http.StatusOK || body["max_bytes"].(float64) != 20 || body["used_bytes"].(float64) != 0 {
		t.Fatalf("namespace status = %d %v", status, body)
	}

	// 在 ns 内发布两个 10 字节条目恰好放得下；第三个触发 LRU 淘汰。
	dataA := []byte("aaaaaaaaaa")
	dataB := []byte("bbbbbbbbbb")
	createNS := func(key string, data []byte) string {
		b, _ := json.Marshal(map[string]any{
			"namespace": "ns", "key": key,
			"final_digest": digestOf(data).String(), "total_size": len(data),
			"chunks": plan(t, data, 10),
		})
		resp, err := http.Post(srv.URL+"/v1/sessions", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			raw, _ := io.ReadAll(resp.Body)
			t.Fatalf("create status %d: %s", resp.StatusCode, raw)
		}
		var sess Session
		json.NewDecoder(resp.Body).Decode(&sess)
		return sess.ID
	}
	for _, kv := range []struct {
		key  string
		data []byte
	}{{"a", dataA}, {"b", dataB}} {
		sid := createNS(kv.key, kv.data)
		uploadChunk(t, srv.URL, sid, 0, kv.data)
		resp, err := http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json",
			bytes.NewReader([]byte("{}")))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	// 第三个 10 字节条目：需要释放但前两个都未固定 -> LRU 淘汰 a，发布成功。
	dataC := []byte("cccccccccc")
	sid := createNS("c", dataC)
	uploadChunk(t, srv.URL, sid, 0, dataC)
	resp, err := http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish with LRU eviction = %d: %s", resp.StatusCode, raw)
	}
	var pr map[string]any
	_ = json.Unmarshal(raw, &pr)
	if pr["eviction"] == nil {
		t.Fatalf("response should include eviction decision: %v", pr)
	}

	// a 已被淘汰。
	if r, _ := http.Get(srv.URL + "/v1/entries/a?namespace=ns"); r.StatusCode != http.StatusNotFound {
		t.Fatalf("evicted a readable: %d", r.StatusCode)
	}
}

// 固定、续租、解除、查询的完整 HTTP 生命周期与错误码。
func TestAPIPinLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte("pin http api data!!") // 18
	sid := mustCreate(t, srv.URL, "k", "", data, 9)
	for _, sp := range plan(t, data, 9) {
		uploadChunk(t, srv.URL, sid, sp.Index, data[sp.Offset:sp.Offset+sp.Size])
	}
	resp, _ := http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	resp.Body.Close()

	// 固定不存在的键 -> 404 entry_not_found。
	if status, body := postJSON(t, srv.URL, "/v1/pins",
		map[string]any{"key": "ghost", "ttl_seconds": 60}); status != http.StatusNotFound ||
		body["error"] != "entry_not_found" {
		t.Fatalf("pin ghost = %d %v", status, body)
	}

	// 摘要不匹配 -> 412 pin_digest_conflict。
	if status, body := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"key": "k", "ttl_seconds": 60,
		"expected_digest": digestOf([]byte("nope")).String(),
	}); status != http.StatusPreconditionFailed || body["error"] != "pin_digest_conflict" {
		t.Fatalf("pin mismatch = %d %v", status, body)
	}

	// 正常固定 -> 201。
	status, body := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"key": "k", "ttl_seconds": 60,
		"expected_digest": digestOf(data).String(), "request_id": "pin-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("pin = %d %v", status, body)
	}
	if body["version"].(float64) != 1 || body["status"] != PinActive {
		t.Fatalf("pin body = %v", body)
	}

	// 同请求号重试 -> 原结果（仍为 v1）。
	_, body2 := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"key": "k", "ttl_seconds": 60,
		"expected_digest": digestOf(data).String(), "request_id": "pin-1",
	})
	if body2["version"].(float64) != 1 {
		t.Fatalf("idempotent pin replay = %v", body2)
	}

	// 同请求号不同内容 -> 409 pin_request_conflict。
	if status, body := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"key": "k", "ttl_seconds": 120, "request_id": "pin-1",
	}); status != http.StatusConflict || body["error"] != "pin_request_conflict" {
		t.Fatalf("pin request conflict = %d %v", status, body)
	}

	// 续租 -> v2。
	status, body = postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"action": "renew", "key": "k", "extend_seconds": 120,
	})
	if status != http.StatusOK || body["version"].(float64) != 2 {
		t.Fatalf("renew = %d %v", status, body)
	}

	// 旧版本续租 -> 409 pin_version_conflict。
	if status, body := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"action": "renew", "key": "k", "extend_seconds": 120, "expected_version": 1,
	}); status != http.StatusConflict || body["error"] != "pin_version_conflict" {
		t.Fatalf("stale renew = %d %v", status, body)
	}

	// 查询单个固定。
	status, body = getJSON(t, srv.URL, "/v1/pins/default/k")
	if status != http.StatusOK || body["version"].(float64) != 2 {
		t.Fatalf("get pin = %d %v", status, body)
	}
	// 列出活跃固定。
	resp2, err := http.Get(srv.URL + "/v1/pins?active=true")
	if err != nil {
		t.Fatal(err)
	}
	var pins []map[string]any
	json.NewDecoder(resp2.Body).Decode(&pins)
	resp2.Body.Close()
	if len(pins) != 1 || pins[0]["key"] != "k" {
		t.Fatalf("active pins = %+v", pins)
	}

	// 解除。
	status, body = postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"action": "unpin", "key": "k",
	})
	if status != http.StatusOK || body["status"] != PinReleased {
		t.Fatalf("unpin = %d %v", status, body)
	}
	// 再次续租 -> 409 pin_not_active。
	if status, body := postJSON(t, srv.URL, "/v1/pins", map[string]any{
		"action": "renew", "key": "k", "extend_seconds": 60,
	}); status != http.StatusConflict || body["error"] != "pin_not_active" {
		t.Fatalf("renew after unpin = %d %v", status, body)
	}
}

// 淘汰决策的建立 / 查询 / 提交，以及访问使候选失效。
func TestAPIEvictionPlanCommit(t *testing.T) {
	srv, clk := newTestServer(t)
	postJSON(t, srv.URL, "/v1/namespaces", map[string]any{"name": "ns", "max_bytes": 1000})
	data := []byte("eviction api data")
	b, _ := json.Marshal(map[string]any{
		"namespace": "ns", "key": "a",
		"final_digest": digestOf(data).String(), "total_size": len(data),
		"chunks": plan(t, data, 6),
	})
	resp, _ := http.Post(srv.URL+"/v1/sessions", "application/json", bytes.NewReader(b))
	var sess Session
	json.NewDecoder(resp.Body).Decode(&sess)
	resp.Body.Close()
	for _, sp := range plan(t, data, 6) {
		uploadChunk(t, srv.URL, sess.ID, sp.Index, data[sp.Offset:sp.Offset+sp.Size])
	}
	resp, _ = http.Post(srv.URL+"/v1/sessions/"+sess.ID+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	resp.Body.Close()

	// plan。
	status, body := postJSON(t, srv.URL, "/v1/evictions",
		map[string]any{"namespace": "ns", "need_bytes": 1})
	if status != http.StatusCreated {
		t.Fatalf("plan = %d %v", status, body)
	}
	id, _ := body["id"].(string)
	cands := body["candidates"].([]any)
	if len(cands) != 1 {
		t.Fatalf("candidates = %v", cands)
	}

	// 访问候选使其失效。
	clk.Advance(time.Minute)
	if r, err := http.Get(srv.URL + "/v1/entries/a?namespace=ns"); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("read candidate: %v %d", err, r.StatusCode)
	}

	// commit。
	resp2, err := http.Post(fmt.Sprintf("%s/v1/evictions/%s/commit", srv.URL, id),
		"application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	var decision map[string]any
	json.NewDecoder(resp2.Body).Decode(&decision)
	resp2.Body.Close()
	c0 := decision["candidates"].([]any)[0].(map[string]any)
	if c0["outcome"] != EvictSkippedAccessed {
		t.Fatalf("outcome = %v", c0["outcome"])
	}

	// 决策可查询、可列表。
	if status, _ := getJSON(t, srv.URL, "/v1/evictions/"+id); status != http.StatusOK {
		t.Fatalf("get decision = %d", status)
	}
	if r, err := http.Get(srv.URL + "/v1/evictions?namespace=ns"); err != nil {
		t.Fatal(err)
	} else {
		var ds []map[string]any
		json.NewDecoder(r.Body).Decode(&ds)
		r.Body.Close()
		if len(ds) != 1 {
			t.Fatalf("decisions = %d", len(ds))
		}
	}
}

// 内容块引用查询：已发布 + 固定两类引用都应出现。
func TestAPIBlobReferences(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte("refs api test data!!") // 单片即可
	sid := mustCreate(t, srv.URL, "k", "", data, len(data))
	uploadChunk(t, srv.URL, sid, 0, data)
	resp, _ := http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	resp.Body.Close()
	postJSON(t, srv.URL, "/v1/pins", map[string]any{"key": "k", "ttl_seconds": 60})

	d := digestOf(data).String()
	r2, err := http.Get(srv.URL + "/v1/blobs/" + d + "/refs")
	if err != nil {
		t.Fatal(err)
	}
	var refs []map[string]any
	json.NewDecoder(r2.Body).Decode(&refs)
	r2.Body.Close()
	kinds := map[string]bool{}
	for _, ref := range refs {
		kinds[ref["kind"].(string)] = true
	}
	if !kinds[RefEntry] || !kinds[RefPin] {
		t.Fatalf("refs = %+v", refs)
	}

	// 形态错误的路径。
	if r, _ := http.Get(srv.URL + "/v1/blobs/bogus/refs"); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad digest refs = %d", r.StatusCode)
	}
}
