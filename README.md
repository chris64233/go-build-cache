# go-build-cache

内容寻址（content-addressed）的构建制品缓存：支持分片上传、租约管理、原子发布、
乐观并发控制、命名空间配额与可续租缓存固定（pin）、两阶段 LRU 淘汰以及标记-清除
垃圾回收。所有元数据持久化，进程重启后可完整恢复。

开发环境：Go 1.23.0。

## 核心模型与不变量

- **内容寻址块（blob）**：每个分片按其内容摘要（默认 `sha256:<hex>`）存储，
  `PutBlob` 天然幂等——相同内容重传等于无操作。
- **上传会话（session）**：创建时声明命名空间、缓存键、最终制品预期摘要、
  总大小与完整分片集合（分片号 / 偏移 / 大小 / 单片摘要）。分片可以**乱序到达**。
- **已发布条目（entry）**：只有 `complete` 校验全部通过后才写入。
  **校验完成之前，读者通过缓存键看不到任何数据**（`Read` 返回 `entry_not_found`）。
  每个条目记录 `published_at` 与每次读取刷新的 `last_accessed_at`（另有严格递增的
  `access_seq`，作为"淘汰决策之后是否被访问"的可靠判据）。
- **命名空间（namespace）与配额**：条目按命名空间隔离，每个命名空间有
  `max_bytes` 上限（0 表示不限）。未显式指定命名空间时使用 `default`
  （自动注册、配额不限）。
- **固定租约（pin lease）**：可以把某个已发布条目保护到截止时间，租约带
  **严格递增版本**。租约在固定时刻**快照条目对内容块的引用**。
- **引用关系**：`entry → blob`、`活跃 open 会话 → 已上传 blob`、
  `有效固定租约 → 快照 blob`。垃圾回收只删除**三类引用全部为空**的块。

### 上传与冲突语义

- 分片乱序、相同内容重传：幂等成功。
- 同一分片号上传**不同内容**：`ChunkConflictError(content_mismatch)`。
- 分片大小不符：`ChunkConflictError(size_mismatch)`；单片摘要不符：`DigestMismatchError`。
- `complete` 依次核对：①分片齐全（否则 `chunks_incomplete`，返回缺失分片号）
  ②拼接总大小 ③按分片号顺序流式重算最终摘要。任一不符则**不发布**。

### 并发发布与版本条件

多个会话可同时尝试发布同一个命名空间内的同一个键：

- 最终摘要**相同** → 复用现有条目（不产生新版本，同时刷新最近访问时间）；
- 最终摘要**不同**且未带版本条件 → `VersionConflictError`（HTTP 412），拒绝覆盖；
- 带 `expected_version`：仅当当前版本恰好匹配才覆盖为 `version+1`。

版本条件（CAS）在**存储层**实现（`Store.PutEntry`），即使绕过本进程锁也不会出现
旧版本覆盖新版本。

### 租约与统一时钟

- 所有"当前时间"一律取自 `Clock` 接口（生产用 `SystemClock`，测试用 `FakeClock`），
  代码中不直接调用 `time.Now()` 做过期判断。
- 会话有 `expires_at`；过期后上传 / 续期 / 完成一律返回 `ErrLeaseExpired`，
  **过期会话不会被任何操作复活**（记录仍保留，直到清理器统一删除）。
- 过期清理、取消、上传、完成发布、固定 / 续租 / 解除、淘汰与 GC 全部在同一把
  服务锁下串行，因此任何"决策 → 确认"窗口之间的状态变化都不会被遗漏。

## 命名空间配额与 LRU 淘汰

- 注册命名空间：`RegisterNamespace(name, maxBytes)`；`SetNamespaceQuota` 可调整。
  收缩配额不会立即淘汰——超限部分在**下一次发布**时回收。
- 发布会使命名空间超过配额时，按以下顺序选择淘汰候选，直到腾出足够字节：

  1. **未固定**（持有有效 pin 的条目永不参与淘汰）；
  2. **最近最少使用**（`last_accessed_at` 升序）；
  3. **键名升序**（访问时刻相同时结果确定）。

- 未固定条目全部腾出仍不足：发布返回 `QuotaExceededError`（HTTP 507），
  **不删除任何条目、新条目不可见**。

### 两阶段淘汰：候选确认到真正删除之间必须可失效

淘汰分为显式的两个阶段，决策对象是自包含的、可保存 / 重放的：

1. `PlanEviction(ns, needBytes)`：选定候选并写审计（`eviction_selected`），**不删除**；
2. `CommitEviction(decision)`：逐个候选对照最新状态确认后才真正删除
   （`entry_evicted`）。

确认时候选发生以下任一变化，旧淘汰决定即失效（`eviction_stale`，条目保留）：

| 变化 | 失效原因 |
| --- | --- |
| 决策后被读取（含同摘要复用发布） | `accessed`（`access_seq` 不一致） |
| 决策后被重新发布（版本或摘要变化） | `republished` |
| 决策后被有效固定 | `pinned` |
| 条目已经不存在 | `entry_gone` |

发布路径内部的配额回收等价于同一临界区内立即执行的 plan→commit，
因此不存在超配额发布；外部调用方则可以把决策持久化、延迟到任意时刻确认。

## 固定租约（可续租的缓存固定）

`Pin` / `RenewPin` / `ReleasePin` 三个操作作用于 `(namespace, key)`：

- **固定**：必须给出 `ExpectedDigest`。目标条目**不存在**返回
  `PinConflictError(missing_entry)`；**当前摘要不匹配**返回
  `PinConflictError(digest_mismatch)`——固定绝不作用于不存在或摘要不匹配的条目。
- **严格递增版本**：续租、解除都使租约 `version+1`。续租 / 解除必须携带
  `ExpectedVersion`，版本不匹配返回 `PinVersionConflictError`——
  **旧版本操作不可能缩短或复活新租约**。
- **续租只能延长**：新截止时间必须晚于当前截止时间；已过期 / 已解除的租约
  续租返回 `ErrPinExpired`，不会复活。想继续固定请对当前条目重新 `Pin`
  （在原记录上得到 `version+1` 的新租约）。
- **解除**：旧版本请求不能解除新版本租约；重复解除同一版本按幂等成功返回。
- **过期扫描**：`SweepExpiredPins()`（GC 也会自动执行）物理清除已到期与已解除
  的租约记录。
- **块保护独立于条目**：租约快照固定时刻的块引用。条目随后被覆盖发布时，
  旧版本独有的块仍受租约保护，直到租约解除 / 过期并被扫描清除后，才由 GC 回收。

### 请求号幂等

固定、续租、解除都可带 `RequestID`：

- **请求号相同且内容相同** → 返回首次结果（响应带 `"replayed": true`），不重复推进版本；
- **请求号相同但内容不同**（不同操作、不同键、不同截止时间 / 摘要 / 版本条件）
  → `RequestConflictError`（HTTP 409）。

## 垃圾回收（mark → sweep）

1. 清理已取消 / 已过期的会话（它们不再保护任何块）；
2. 清除已到期 / 已解除的固定租约记录（不再是有效固定）；
3. **Mark**：汇总三类存活引用——已发布条目、未过期活跃会话、有效固定租约；
4. **Sweep**：逐块删除未标记内容；删除每个块之前基于最新引用集**二次复核**，
   若标记后出现了新引用则保留并写审计（`blob_skip_in_use`）。

由于 mark 与 sweep 与"发布 / 固定 / 解除 / 淘汰"在同一临界区，一个引用要么完整
建立在快照之前（块被保留），要么只能发生在 GC 之后；不存在误删窗口。

每次决策（开始、会话与租约清理、块删除及原因、保留、淘汰选定 / 删除 / 失效、
固定生命周期、结束、代次、时间戳）都写入**追加式审计日志**，可通过 `AuditLog()`
或 `GET /v1/audit` 查询。

## 查询接口

- **配额**：`QuotaUsage(ns)` 返回 `max_bytes / used_bytes`、条目数、
  固定字节与可淘汰字节。
- **淘汰决定**：`EvictionOrder(ns)` 返回此刻按 LRU + 键名排序的可淘汰候选（只读）；
  `PlanEviction` / `CommitEviction` 执行两阶段淘汰。
- **固定租约**：`GetPin(ns, key)`、`ListPins(ns, includeInactive)`。
- **内容引用**：`BlobReferences(digest)` 返回某块被哪些已发布条目 /
  活跃上传会话 / 有效固定租约引用；三类皆空即下一次 GC 的回收对象。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `cache.go` | `Cache` 服务：会话 / 发布 / 读取（访问时间）/ 清理 / GC 与全部并发控制 |
| `quota.go` | 命名空间配额、两阶段 LRU 淘汰、固定租约（版本 + 请求号幂等）、引用查询 |
| `model.go` | `Session` / `Entry` / `Namespace` / `PinLease` / `GCRecord` 等持久化元数据 |
| `digest.go` | 摘要类型与校验（`sha256:hex`） |
| `clock.go` | `Clock` / `SystemClock` / `FakeClock` 统一时间来源 |
| `errors.go` | 按类别区分的错误：摘要、分片、租约、版本、幂等、配额、固定冲突 |
| `store.go` | `Store` 存储抽象（blob / 会话 / 带 CAS 的条目 / 命名空间 / 固定 / 审计） |
| `memory_store.go` | 进程内实现（测试用） |
| `file_store.go` | 文件持久化实现：临时文件 + `rename` 原子写，重启恢复 |
| `reader.go` | 按分片顺序拼接的已发布条目读取器 |
| `api.go` | HTTP 接口与错误码映射 |
| `cmd/buildcached/main.go` | 可运行服务，带可选后台 GC |

## HTTP 接口

上传与读取（默认命名空间）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/sessions` | 创建会话（body 可带 `namespace`、`idempotency_key`、`lease_seconds`） |
| GET | `/v1/sessions/{id}` | 查询会话状态 |
| POST | `/v1/sessions/{id}/chunks/{index}` | 上传分片（body 为原始字节） |
| POST | `/v1/sessions/{id}/renew` | 续期（`{"extend_seconds": 900}`） |
| POST | `/v1/sessions/{id}/complete` | 校验并原子发布（可带 `expected_version`） |
| DELETE | `/v1/sessions/{id}` | 取消会话 |
| GET | `/v1/entries/{key...}` | 读取默认命名空间已发布内容 |
| POST | `/v1/gc` | 同步执行一次垃圾回收，返回决策报告 |
| POST | `/v1/pins/sweep` | 清除过期 / 已解除固定租约 |
| GET | `/v1/audit` | 查看审计记录 |

命名空间、配额、淘汰与固定（键可以包含 `/`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST/GET | `/v1/namespaces` | 注册配额（`{"name","max_bytes"}`）/ 列出命名空间 |
| GET | `/v1/namespaces/{ns}/quota` | 配额与用量明细 |
| GET | `/v1/namespaces/{ns}/eviction-order` | 当前 LRU 淘汰候选（只读） |
| POST | `/v1/namespaces/{ns}/evictions` | 两阶段淘汰①：选定候选（`{"need_bytes": N}`） |
| POST | `/v1/namespaces/{ns}/evictions?commit=1` | 两阶段淘汰②：确认并删除（body 为①的决策） |
| GET | `/v1/namespaces/{ns}/entries/{key...}` | 读取命名空间内已发布内容 |
| POST | `/v1/namespaces/{ns}/pins/{key...}` | 固定（`expected_digest` + `expires_at`/`ttl_seconds` + 可选 `request_id`） |
| POST | `/v1/namespaces/{ns}/pins/{key...}/renew` | 续租（`expected_version`，只能延长） |
| POST | `/v1/namespaces/{ns}/pins/{key...}/release` | 解除（`expected_version`） |
| GET | `/v1/namespaces/{ns}/pins[/{key...}]` | 列出有效租约（`?all=1` 含失效）/ 查询单个租约 |
| GET | `/v1/blobs/{digest}/references` | 查询内容块的三类引用 |

错误响应统一为 `{"error": "<code>", "message": "..."}`，典型状态码：

| 场景 | HTTP | error code |
| --- | --- | --- |
| 会话 / 条目 / 命名空间 / 租约不存在 | 404 | `session_not_found` / `entry_not_found` / `namespace_not_found` / `pin_not_found` |
| 租约（会话或固定）过期 | 410 | `lease_expired` |
| 会话已完成/取消、幂等键冲突、请求号冲突 | 409 | `session_not_active` / `idempotency_conflict` / `request_conflict` |
| 摘要/分片内容冲突、缺片 | 422 / 409 | `digest_mismatch` / `chunk_conflict` / `chunks_incomplete` |
| 版本条件、固定目标、固定版本不满足 | 412 | `version_conflict` / `pin_conflict` / `pin_version_conflict` |
| 配额不足且固定条目无法腾出空间 | 507 | `quota_exceeded` |

## 使用示例（Go API）

```go
store, _ := buildcache.NewFileStore("./data")
cache, _ := buildcache.New(store, buildcache.SystemClock{}, 15*time.Minute)

// 1) 注册配额命名空间。
cache.RegisterNamespace("team-a", 10<<30) // 10 GiB

sess, err := cache.CreateSession(buildcache.CreateSessionOptions{
    Namespace:   "team-a",
    Key:         "mod/github.com/x/y@v1.2.3",
    FinalDigest: finalDigest,                 // sha256:...
    TotalSize:   int64(len(artifact)),
    Chunks:      chunkSpecs,                  // 分片号/偏移/大小/单片摘要
})
// 乱序、可重试：
cache.UploadChunk(sess.ID, 2, part2)
cache.UploadChunk(sess.ID, 0, part0)
cache.UploadChunk(sess.ID, 1, part1)

res, _ := cache.Complete(sess.ID, buildcache.CompleteOptions{})

// 2) 读取（会刷新 LRU 最近访问时间）。
r, _ := cache.Read("team-a", "mod/github.com/x/y@v1.2.3")
defer r.Close()
io.Copy(os.Stdout, r)

// 3) 把该条目固定 24 小时；之后可凭返回的版本续租或解除。
pin, _ := cache.Pin(buildcache.PinOptions{
    Namespace: "team-a", Key: res.Entry.Key,
    ExpectedDigest: res.Entry.Digest, TTL: 24 * time.Hour,
    RequestID: "client-request-001", // 同号同参重放返回原结果
})
v := pin.Lease.Version
cache.RenewPin(buildcache.PinOptions{ // 只能延长；旧版本号会被拒绝
    Namespace: "team-a", Key: res.Entry.Key,
    ExpectedVersion: &v, TTL: 48 * time.Hour,
})

// 4) 查询：配额、淘汰候选、内容块被谁引用。
usage, _ := cache.QuotaUsage("team-a")
order, _ := cache.EvictionOrder("team-a")
refs, _ := cache.BlobReferences(someChunkDigest)

// 5) 显式两阶段淘汰（发布超限会自动做同样的事）。
decision, _ := cache.PlanEviction("team-a", 1<<20)
report, _ := cache.CommitEviction(decision) // 决策后被访问/固定/重发的候选自动失效

// 6) GC 只回收三类引用皆空的块；固定解除并扫描后旧块才会被回收。
cache.CollectGarbage()
```

## 运行服务

```sh
go run ./cmd/buildcached -addr :8080 -data ./data -ttl 15m -gc-interval 10m
```

## 测试

```sh
go test ./...              # 全部单元 / 持久化 / HTTP 端到端测试
go test -race ./...        # 含竞态检测（含 GC、并发发布与固定租约对打测试）
```

测试覆盖：乱序上传与重传幂等、分片冲突分类、完成三校验、不可见性、
同摘要复用与版本 CAS、租约过期不复活、过期清理、取消、幂等键冲突、
GC 引用保护（已发布 / 活跃会话 / 过期会话 / 无主块）、覆盖发布后旧块回收、
8×25 并发发布与 GC 对打下所有已发布内容保持完整、文件存储重启恢复、
以及完整 HTTP 生命周期；

配额与固定部分覆盖：配额记账与覆盖发布、发布超限的 LRU 淘汰与键名 tie-break、
读取刷新 LRU、全部固定时配额拒绝且不删除、两阶段淘汰在访问 / 固定 / 重新发布 /
条目消失四种情况下的决策失效、固定必须存在且摘要匹配、租约续租只能延长、
旧版本续租 / 解除不能缩短或复活新租约、过期不复活与扫描、固定快照块在条目覆盖后
仍受 GC 保护而解除后被回收、请求号同参重放与异参冲突、块引用三类来源查询、
多命名空间隔离、固定 / 解除 / 续租并发下版本严格单调、以及配额 / 淘汰 / 读取 / GC
高并发对打下内容完整性与最终不超配额，并通过 HTTP 端到端验证全部新接口。
