package buildcache

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func postJSON(t *testing.T, url string, body any) (int, []byte) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func mustCreateNS(t *testing.T, base, namespace, key string, data []byte, chunkSize int) string {
	t.Helper()
	body := map[string]any{
		"namespace":    namespace,
		"key":          key,
		"final_digest": digestOf(data).String(),
		"total_size":   len(data),
		"chunks":       plan(t, data, chunkSize),
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+"/v1/sessions", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status %d: %s", resp.StatusCode, raw)
	}
	var sess Session
	if err := json.NewDecoder(resp.Body).Decode(&sess); err != nil {
		t.Fatal(err)
	}
	return sess.ID
}

func publishNSOverHTTP(t *testing.T, base, namespace, key string, data []byte) {
	t.Helper()
	sid := mustCreateNS(t, base, namespace, key, data, len(data))
	uploadChunk(t, base, sid, 0, data)
	resp, err := http.Post(base+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete %d: %s", resp.StatusCode, raw)
	}
}

// 端到端：注册配额命名空间 -> 发布 -> 固定 -> 续租 -> 查询 -> 解除。
func TestAPIPinLeaseLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	base := srv.URL

	// 注册命名空间。
	if status, raw := postJSON(t, base+"/v1/namespaces", map[string]any{"name": "ns", "max_bytes": 100}); status != http.StatusCreated {
		t.Fatalf("register namespace %d: %s", status, raw)
	}

	// 列表包含 default 与 ns。
	resp, err := http.Get(base + "/v1/namespaces")
	if err != nil {
		t.Fatal(err)
	}
	var nss []Namespace
	json.NewDecoder(resp.Body).Decode(&nss)
	resp.Body.Close()
	if len(nss) != 2 {
		t.Fatalf("namespaces = %+v", nss)
	}

	data := []byte("http pin lifecycle data!!")
	publishNSOverHTTP(t, base, "ns", "mod/a", data)

	// 固定错误摘要 -> 412 pin_conflict。
	if status, raw := postJSON(t, base+"/v1/namespaces/ns/pins/mod/a", map[string]any{
		"expected_digest": digestOf([]byte("wrong")).String(), "ttl_seconds": 3600,
	}); status != http.StatusPreconditionFailed {
		t.Fatalf("pin wrong digest status=%d body=%s", status, raw)
	}

	// 固定正确摘要 -> 201，租约 v1。
	status, raw := postJSON(t, base+"/v1/namespaces/ns/pins/mod/a", map[string]any{
		"expected_digest": digestOf(data).String(), "ttl_seconds": 3600, "request_id": "pin-1",
	})
	if status != http.StatusCreated {
		t.Fatalf("pin status=%d body=%s", status, raw)
	}
	var pinRes PinResult
	if err := json.Unmarshal(raw, &pinRes); err != nil {
		t.Fatal(err)
	}
	if pinRes.Lease.Version != 1 {
		t.Fatalf("pin lease = %+v", pinRes)
	}

	// 请求号重放：结果相同且标记 replayed。
	_, raw = postJSON(t, base+"/v1/namespaces/ns/pins/mod/a", map[string]any{
		"expected_digest": digestOf(data).String(), "ttl_seconds": 3600, "request_id": "pin-1",
	})
	json.Unmarshal(raw, &pinRes)
	if !pinRes.Replayed {
		t.Fatalf("same request id must replay: %s", raw)
	}

	// 请求号相同、参数不同 -> 409 request_conflict。
	if status, _ := postJSON(t, base+"/v1/namespaces/ns/pins/mod/a", map[string]any{
		"expected_digest": digestOf(data).String(), "ttl_seconds": 7200, "request_id": "pin-1",
	}); status != http.StatusConflict {
		t.Fatalf("request conflict status=%d, want 409", status)
	}

	// 错误版本续租 -> 412。
	if status, _ := postJSON(t, base+"/v1/namespaces/ns/pins/mod/a/renew", map[string]any{
		"expected_version": 99, "ttl_seconds": 7200,
	}); status != http.StatusPreconditionFailed {
		t.Fatalf("stale renew status=%d, want 412", status)
	}

	// 正确版本续租 -> 200，v2。
	status, raw = postJSON(t, base+"/v1/namespaces/ns/pins/mod/a/renew", map[string]any{
		"expected_version": 1, "ttl_seconds": 7200,
	})
	if status != http.StatusOK {
		t.Fatalf("renew status=%d body=%s", status, raw)
	}
	json.Unmarshal(raw, &pinRes)
	if pinRes.Lease.Version != 2 {
		t.Fatalf("renewed lease v=%d, want 2", pinRes.Lease.Version)
	}

	// 查询单个租约（键含 "/"）。
	resp, err = http.Get(base + "/v1/namespaces/ns/pins/mod/a")
	if err != nil {
		t.Fatal(err)
	}
	var lease PinLease
	json.NewDecoder(resp.Body).Decode(&lease)
	resp.Body.Close()
	if lease.Version != 2 {
		t.Fatalf("get lease = %+v", lease)
	}

	// 列出有效租约。
	resp, _ = http.Get(base + "/v1/namespaces/ns/pins")
	var pins []PinLease
	json.NewDecoder(resp.Body).Decode(&pins)
	resp.Body.Close()
	if len(pins) != 1 || pins[0].Key != "mod/a" {
		t.Fatalf("pins = %+v", pins)
	}

	// 解除。
	if status, raw = postJSON(t, base+"/v1/namespaces/ns/pins/mod/a/release", map[string]any{
		"expected_version": 2,
	}); status != http.StatusOK {
		t.Fatalf("release status=%d body=%s", status, raw)
	}
	json.Unmarshal(raw, &pinRes)
	if !pinRes.Lease.Released || pinRes.Lease.Version != 3 {
		t.Fatalf("released = %+v", pinRes)
	}

	// 扫描清除。
	if status, _ := postJSON(t, base+"/v1/pins/sweep", nil); status != http.StatusOK {
		t.Fatalf("pin sweep status=%d", status)
	}
}

// 端到端：配额查询与两阶段淘汰，决策后访问使候选失效。
func TestAPIQuotaAndTwoPhaseEviction(t *testing.T) {
	srv, clk := newTestServer(t)
	base := srv.URL

	postJSON(t, base+"/v1/namespaces", map[string]any{"name": "ns", "max_bytes": 10})
	publishNSOverHTTP(t, base, "ns", "old", []byte("aaa"))
	clk.Advance(1) // 保证 old 比 new 更久未访问
	publishNSOverHTTP(t, base, "ns", "new", []byte("bbb"))

	// 配额用量。
	resp, err := http.Get(base + "/v1/namespaces/ns/quota")
	if err != nil {
		t.Fatal(err)
	}
	var info QuotaInfo
	json.NewDecoder(resp.Body).Decode(&info)
	resp.Body.Close()
	if info.MaxBytes != 10 || info.UsedBytes != 6 || info.EntryCount != 2 {
		t.Fatalf("quota = %+v", info)
	}

	// LRU 候选查询：old 在前。
	resp, _ = http.Get(base + "/v1/namespaces/ns/eviction-order")
	var order []EvictionCandidate
	json.NewDecoder(resp.Body).Decode(&order)
	resp.Body.Close()
	if len(order) != 2 || order[0].Key != "old" {
		t.Fatalf("eviction order = %+v", order)
	}

	// 第一阶段：选定淘汰候选。
	status, raw := postJSON(t, base+"/v1/namespaces/ns/evictions", map[string]any{"need_bytes": 3})
	if status != http.StatusCreated {
		t.Fatalf("plan status=%d body=%s", status, raw)
	}
	var decision EvictionDecision
	if err := json.Unmarshal(raw, &decision); err != nil {
		t.Fatal(err)
	}
	if len(decision.Candidates) != 1 || decision.Candidates[0].Key != "old" {
		t.Fatalf("decision = %+v", decision)
	}

	// 决策之后读取 old。
	resp, _ = http.Get(base + "/v1/namespaces/ns/entries/old")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// 第二阶段确认：候选因被访问而失效，不删除。
	status, raw = postJSON(t, base+"/v1/namespaces/ns/evictions?commit=1", decision)
	if status != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", status, raw)
	}
	var report EvictionReport
	json.Unmarshal(raw, &report)
	if len(report.Deleted) != 0 || len(report.Stale) != 1 ||
		report.Stale[0].Reason != EvictStaleAccessed {
		t.Fatalf("commit report = %+v", report)
	}

	// 重新决策并确认：这次真正删除。
	_, raw = postJSON(t, base+"/v1/namespaces/ns/evictions", map[string]any{"need_bytes": 3})
	json.Unmarshal(raw, &decision)
	_, raw = postJSON(t, base+"/v1/namespaces/ns/evictions?commit=1", decision)
	json.Unmarshal(raw, &report)
	if len(report.Deleted) != 1 || report.ReclaimedBytes != 3 {
		t.Fatalf("second commit report = %+v", report)
	}

	// 引用查询：存活条目 old 的块有 entry 引用（它在第一次决策后被读取，
	// 因而第二次决策淘汰的是 new）。
	d := digestOf([]byte("aaa"))
	resp, _ = http.Get(base + "/v1/blobs/" + d.String() + "/references")
	var refs BlobReferences
	json.NewDecoder(resp.Body).Decode(&refs)
	resp.Body.Close()
	if len(refs.Entries) != 1 || refs.Entries[0].Key != "old" {
		t.Fatalf("blob refs = %+v", refs)
	}
}

// 端到端：固定使条目免于配额淘汰。
func TestAPIPinnedEntrySurvivesQuota(t *testing.T) {
	srv, _ := newTestServer(t)
	base := srv.URL

	postJSON(t, base+"/v1/namespaces", map[string]any{"name": "ns", "max_bytes": 6})
	a := []byte("aaa")
	publishNSOverHTTP(t, base, "ns", "a", a)
	publishNSOverHTTP(t, base, "ns", "b", []byte("bbb"))

	// 固定 a。
	if status, raw := postJSON(t, base+"/v1/namespaces/ns/pins/a", map[string]any{
		"expected_digest": digestOf(a).String(), "ttl_seconds": 3600,
	}); status != http.StatusCreated {
		t.Fatalf("pin = %d %s", status, raw)
	}

	// 超大发布 -> 507 quota_exceeded（固定的 a 不能被淘汰）。
	big := []byte("XXXXXXXXXX")
	sid := mustCreateNS(t, base, "ns", "big", big, len(big))
	uploadChunk(t, base, sid, 0, big)
	resp, err := http.Post(base+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInsufficientStorage {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("quota exceeded want 507, got %d: %s", resp.StatusCode, raw)
	}

	// a、b 均仍在。
	for _, k := range []string{"a", "b"} {
		resp, _ := http.Get(base + "/v1/namespaces/ns/entries/" + k)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("entry %s should survive, status=%d", k, resp.StatusCode)
		}
	}
}
