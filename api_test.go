package buildcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*httptest.Server, *FakeClock) {
	t.Helper()
	clk := NewFakeClock(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	c, err := New(NewMemoryStore(), clk, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(c)
	srv := httptest.NewServer(h.Mux)
	t.Cleanup(srv.Close)
	return srv, clk
}

func mustCreate(t *testing.T, base, key, idem string, data []byte, chunkSize int) string {
	t.Helper()
	body := map[string]any{
		"key":          key,
		"final_digest": digestOf(data).String(),
		"total_size":   len(data),
		"chunks":       plan(t, data, chunkSize),
	}
	if idem != "" {
		body["idempotency_key"] = idem
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

func uploadChunk(t *testing.T, base, sid string, index int, data []byte) {
	t.Helper()
	url := fmt.Sprintf("%s/v1/sessions/%s/chunks/%d", base, sid, index)
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload %d status %d: %s", index, resp.StatusCode, raw)
	}
}

func TestAPIFullLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte("http end-to-end artifact payload")
	key := "art/42"
	sid := mustCreate(t, srv.URL, key, "", data, 9)

	// 完成前读取 -> 404 entry_not_found。
	resp, err := http.Get(srv.URL + "/v1/entries/" + key)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}

	// 乱序上传 + 一次重传。
	specs := plan(t, data, 9)
	order := make([]int, len(specs))
	for i := range order {
		order[i] = len(specs) - 1 - i
	}
	for _, i := range order {
		sp := specs[i]
		uploadChunk(t, srv.URL, sid, i, data[sp.Offset:sp.Offset+sp.Size])
	}
	uploadChunk(t, srv.URL, sid, 0, data[specs[0].Offset:specs[0].Offset+specs[0].Size])

	// 完成。
	resp, err = http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete %d: %s", resp.StatusCode, raw)
	}
	var pr PublishResult
	json.NewDecoder(resp.Body).Decode(&pr)
	resp.Body.Close()
	if pr.Entry.Version != 1 {
		t.Fatalf("version = %d", pr.Entry.Version)
	}

	// 读取并校验内容与响应头。
	resp, err = http.Get(srv.URL + "/v1/entries/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Fatalf("body mismatch")
	}
	if resp.Header.Get("X-Cache-Digest") != digestOf(data).String() {
		t.Fatalf("digest header missing")
	}
	if resp.Header.Get("X-Cache-Version") != "1" {
		t.Fatalf("version header = %q", resp.Header.Get("X-Cache-Version"))
	}

	// 同摘要再次发布 -> 复用。
	sid2 := mustCreate(t, srv.URL, key, "", data, 9)
	for i, sp := range specs {
		uploadChunk(t, srv.URL, sid2, i, data[sp.Offset:sp.Offset+sp.Size])
	}
	resp, _ = http.Post(srv.URL+"/v1/sessions/"+sid2+"/complete", "application/json",
		bytes.NewReader([]byte("{}")))
	json.NewDecoder(resp.Body).Decode(&pr)
	resp.Body.Close()
	if !pr.Reused || pr.Entry.Version != 1 {
		t.Fatalf("same digest must reuse, got %+v", pr)
	}

	// GC 后内容仍在。
	resp, err = http.Post(srv.URL+"/v1/gc", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gc = %d", resp.StatusCode)
	}
	resp, _ = http.Get(srv.URL + "/v1/entries/" + key)
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("content lost after GC")
	}

	// 审计接口可查。
	resp, _ = http.Get(srv.URL + "/v1/audit")
	var audit []GCRecord
	json.NewDecoder(resp.Body).Decode(&audit)
	resp.Body.Close()
	if len(audit) == 0 {
		t.Fatalf("audit empty")
	}
}

func TestAPIVersionConflictAndCAS(t *testing.T) {
	srv, _ := newTestServer(t)
	v1 := []byte("version one payload!!!!!!!!!")
	v2 := []byte("version two payload!!!!!!!!!")
	key := "k"

	publish := func(data []byte, cond string) (int, map[string]any) {
		sid := mustCreate(t, srv.URL, key, "", data, 8)
		for _, sp := range plan(t, data, 8) {
			uploadChunk(t, srv.URL, sid, sp.Index, data[sp.Offset:sp.Offset+sp.Size])
		}
		body := bytes.NewReader([]byte(cond))
		resp, err := http.Post(srv.URL+"/v1/sessions/"+sid+"/complete", "application/json", body)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		raw, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}

	if status, _ := publish(v1, "{}"); status != http.StatusOK {
		t.Fatalf("v1 publish = %d", status)
	}
	// 不同摘要无条件覆盖 -> 412。
	status, body := publish(v2, "{}")
	if status != http.StatusPreconditionFailed || body["error"] != "version_conflict" {
		t.Fatalf("want 412 version_conflict, got %d %v", status, body)
	}
	// 旧版本条件 -> 412。
	status, _ = publish(v2, `{"expected_version":99}`)
	if status != http.StatusPreconditionFailed {
		t.Fatalf("stale CAS = %d", status)
	}
	// 正确版本条件 -> 200。注意上面 publish 每次新建会话，会话在冲突后仍可重试。
	status, _ = publish(v2, `{"expected_version":1}`)
	if status != http.StatusOK {
		t.Fatalf("CAS overwrite = %d", status)
	}
}

func TestAPICancelAndLease(t *testing.T) {
	srv, clk := newTestServer(t)
	data := []byte("cancel/lease api data")
	sid := mustCreate(t, srv.URL, "k", "", data, 8)

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/sessions/"+sid, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("cancel = %d", resp.StatusCode)
	}

	resp, _ = http.Post(fmt.Sprintf("%s/v1/sessions/%s/chunks/0", srv.URL, sid),
		"application/octet-stream", bytes.NewReader(data[:8]))
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("upload after cancel = %d, want 409", resp.StatusCode)
	}

	// 过期场景：新会话，推进时钟后上传 -> 410。
	sid2 := mustCreate(t, srv.URL, "k2", "", data, 8)
	clk.Advance(2 * time.Minute)
	resp, err = http.Post(fmt.Sprintf("%s/v1/sessions/%s/chunks/0", srv.URL, sid2),
		"application/octet-stream", bytes.NewReader(data[:8]))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("expired upload = %d, want 410", resp.StatusCode)
	}
	var eb map[string]string
	json.NewDecoder(resp.Body).Decode(&eb)
	if eb["error"] != "lease_expired" {
		t.Fatalf("error code = %v", eb)
	}
}

func TestAPIIdempotencyConflict(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte("idem api data")
	id := mustCreate(t, srv.URL, "k", "req-9", data, 8)

	other := []byte("different digest here!!!")
	body, _ := json.Marshal(map[string]any{
		"key": "k", "idempotency_key": "req-9",
		"final_digest": digestOf(other).String(),
		"total_size":   len(other),
		"chunks":       plan(t, other, 8),
	})
	resp, err := http.Post(srv.URL+"/v1/sessions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409, got %d", resp.StatusCode)
	}
	var eb map[string]string
	json.NewDecoder(resp.Body).Decode(&eb)
	if eb["error"] != "idempotency_conflict" {
		t.Fatalf("error = %v", eb)
	}
	// 同参数重试仍命中原会话。
	sid := mustCreate(t, srv.URL, "k", "req-9", data, 8)
	if sid != id {
		t.Fatalf("idempotent retry returned different session")
	}
}

func TestAPIChunkConflict(t *testing.T) {
	srv, _ := newTestServer(t)
	data := []byte("chunk conflict test data")
	sid := mustCreate(t, srv.URL, "k", "", data, 8)
	uploadChunk(t, srv.URL, sid, 0, data[:8])
	// 同号不同内容 -> 422 chunk_conflict。
	resp, err := http.Post(fmt.Sprintf("%s/v1/sessions/%s/chunks/0", srv.URL, sid),
		"application/octet-stream", bytes.NewReader([]byte("XXXXXXXX")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", resp.StatusCode)
	}
	var eb map[string]string
	json.NewDecoder(resp.Body).Decode(&eb)
	if eb["error"] != "chunk_conflict" {
		t.Fatalf("error = %v", eb)
	}
}

// 健康检查。
func TestAPIHealthz(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, err := http.Get(srv.URL + "/v1/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
}

// 续期接口：续期后，即使默认 TTL 已过，会话依然可用。
func TestAPIRenew(t *testing.T) {
	srv, clk := newTestServer(t)
	data := []byte("renew me via http api")
	sid := mustCreate(t, srv.URL, "k", "", data, 8)

	resp, err := http.Post(srv.URL+"/v1/sessions/"+sid+"/renew", "application/json",
		bytes.NewReader([]byte(`{"extend_seconds":300}`)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("renew = %d: %s", resp.StatusCode, raw)
	}
	var out map[string]string
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out["expires_at"] == "" {
		t.Fatalf("renew response missing expires_at: %v", out)
	}

	clk.Advance(90 * time.Second) // 超过默认 1 分钟 TTL，但在 5 分钟续期窗口内
	uploadChunk(t, srv.URL, sid, 0, data[:8])

	// GET 会话状态。
	resp, err = http.Get(srv.URL + "/v1/sessions/" + sid)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get session = %d", resp.StatusCode)
	}
}

// 非法方法与未知路由。
func TestAPIMethodAndRouting(t *testing.T) {
	srv, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/v1/sessions")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /sessions = %d, want 405", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/v1/gc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /gc = %d, want 405", resp.StatusCode)
	}

	resp, _ = http.Post(srv.URL+"/v1/sessions", "application/json", bytes.NewReader([]byte("{bad")))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed json = %d, want 400", resp.StatusCode)
	}

	resp, _ = http.Get(srv.URL + "/v1/sessions/does-not-exist")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing session = %d, want 404", resp.StatusCode)
	}

	resp, _ = http.Post(srv.URL+"/v1/sessions", "application/json",
		bytes.NewReader([]byte(`{"key":"k","final_digest":"bogus","total_size":1,"chunks":[]}`)))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad digest = %d, want 400", resp.StatusCode)
	}
}
