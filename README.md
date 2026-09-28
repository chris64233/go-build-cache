# go-build-cache

内容寻址（content-addressed）的构建制品缓存：支持分片上传、租约管理、原子发布、
乐观并发控制、**命名空间配额与 LRU 淘汰**、**可续租的缓存固定（pin）**、
**跨命名空间条目晋级（promotion，共享内容块）**以及标记-清除垃圾回收。
所有元数据持久化，进程重启后可完整恢复。

开发环境：Go 1.23.0。

## 核心模型与不变量

- **内容寻址块（blob）**：每个分片按其内容摘要（默认 `sha256:<hex>`）存储，
  `PutBlob` 天然幂等——相同内容重传等于无操作。
- **命名空间（namespace）**：配额与固定的边界。每个命名空间有最大可用字节数
  `max_bytes`（`<=0` 表示不限）；条目身份为 `(namespace, key)`。
  未指定命名空间的会话/条目归入自动创建的 `default` 命名空间。
- **上传会话（session）**：创建时声明命名空间、缓存键、最终制品预期摘要、总大小与
  完整分片集合（分片号 / 偏移 / 大小 / 单片摘要）。分片可以**乱序到达**。
- **已发布条目（entry）**：只有 `complete` 校验全部通过后才写入，并记录
  `last_accessed_at`（读取命中、同摘要重新发布都会刷新）。
  **校验完成之前，读者通过缓存键看不到任何数据**（`Read` 返回 `entry_not_found`）。
- **引用关系**：`entry → 多个 blob`、`活跃 open 会话 → 已上传 blob`、
  `有效固定租约 → 固定瞬间快照的 blob`。垃圾回收只删除**三类引用都不再持有**的块。

### 上传与冲突语义

- 分片乱序、相同内容重传：幂等成功。
- 同一分片号上传**不同内容**：`ChunkConflictError(content_mismatch)`。
- 分片大小不符：`ChunkConflictError(size_mismatch)`；单片摘要不符：`DigestMismatchError`。
- `complete` 依次核对：①分片齐全（否则 `chunks_incomplete`，返回缺失分片号）
  ②拼接总大小 ③按分片号顺序流式重算最终摘要。任一不符则**不发布**。

### 并发发布与版本条件

多个会话可同时尝试发布同一个键：

- 最终摘要**相同** → 复用现有条目，不产生新版本（并刷新最近访问时间），所有会话都成功；
- 最终摘要**不同**且未带版本条件 → `VersionConflictError`（HTTP 412），拒绝覆盖；
- 带 `expected_version`：仅当当前版本恰好匹配才覆盖为 `version+1`。

版本条件（CAS）在**存储层**实现（`Store.PutEntry`），即使绕过本进程锁也不会出现
旧版本覆盖新版本。

### 命名空间配额与 LRU 淘汰

- 命名空间用量 = 该空间内全部已发布条目 `total_size` 之和。
- **发布**若使投影用量超过配额（覆盖发布按 `已用 - 旧版本 + 新版本` 计），
  服务先做一次内部淘汰再落盘；淘汰后仍不足则发布失败并返回
  `QuotaExceededError`（HTTP 507 `quota_exceeded`），且**不删除任何东西**。
- 淘汰候选排序：**仅未固定条目**，按最近访问时间升序（LRU），访问时间相同时按
  **键名升序**稳定排序，累计体量直到满足需要释放的字节数。
- 淘汰是**两阶段决策**（plan → commit），决策建立时快照每个候选的
  `版本 / 摘要 / 最近访问时间`；提交删除前逐个用最新状态复核：
  - 快照之后候选被**访问**（访问时间前移）→ `skipped_accessed`，保留；
  - 候选被**固定**（存在有效租约）→ `skipped_pinned`，保留；
  - 候选被**重新发布**（版本或摘要变化、或已消失）→
    `skipped_republished` / `skipped_missing`，保留；
  - 其余候选才真正删除（`deleted`）。

  所有状态变更都在同一把服务锁下串行，因此"旧淘汰决定"绝不可能删掉决定之后又被
  使用或保护的条目。每次决策（候选、结果、释放字节数）都持久化并写入审计日志。
- 也可以用 `PlanEviction` / `CommitEviction`（HTTP `POST /v1/evictions`、
  `POST /v1/evictions/{id}/commit`）手动执行两阶段淘汰；提交是幂等的。

### 固定租约（pin）：可续租、严格递增版本

固定把一个已发布键保护到某个截止时间：

- **前置条件**：键必须存在已发布条目，否则 `PinConflictError(no_entry)`；
  若请求带 `expected_digest`，还必须与当前摘要一致，否则
  `PinDigestConflictError`（HTTP 412）。**固定绝不作用于不存在或摘要不匹配的条目。**
- **租约 + 严格递增版本**：每个 `(namespace,key)` 的固定有自己的版本号，首次为 1，
  此后每次**续租 / 解除 / 过期失效 / 重新固定**都 +1。操作可带 `expected_version`
  条件；携带旧版本号的续租 / 解除一律被拒绝（`PinVersionConflictError`），
  因此与续租、解除、过期扫描并发时，**旧版本操作既不能缩短也不能复活新租约**。
- **续租单调延长**：新截止时间取 `max(现有截止时间, now+extend)`，永不缩短；
  只有当前未到期且 `active` 的租约能续租。
- **解除 / 过期是终态**：解除后状态为 `released`；到期由 `SweepExpiredPins` 或 GC
  扫描为 `expired`（同样 +1 版本并写审计）。终态租约不能续租、不能重复解除、
  不能被任何旧操作复活；需要保护时重新固定，得到更高版本。
- **请求号幂等**：pin / renew / unpin 均可带 `request_id`。相同请求号 + 相同请求
  内容返回**原结果**（即使时钟已推进，截止时间也不重算）；相同请求号 + 不同内容
  返回 `PinRequestConflictError`。
- **块保护快照**：固定时把当前条目的块引用复制进租约。即使条目随后被覆盖发布而
  消失，只要租约仍有效（`active` 且未到期），这些旧版本独有块依旧被 GC 保留；
  解除或过期后下一次 GC 才回收。固定**不阻止**条目本身的覆盖发布，只影响配额淘汰
  与块回收。

### 跨命名空间晋级（promotion）：内容块共享、配额淘汰、旧请求不覆盖新状态

晋级把一个**已发布来源条目**提升到另一个命名空间，目标可以**复制为新键**
（`copy`，目标键必须不存在）或**替换同名旧条目**（`replace`，版本递增；
旧条目不存在时退化为新建）。

晋级是**两阶段请求**（`CreatePromotion` → `CommitPromotion`），也可用
`Promote` 在一次调用内原子完成（典型路径）：

- **建立（create）时冻结**来源条目的当前摘要与内容块引用快照（`PromotionSource`）、
  目标配额版本（命名空间 `updated_at`）、目标同名条目版本，并**按目标配额算定
  淘汰候选**。冻结后来源条目即使被覆盖发布或删除，本次晋级依据也不变。
- **拒绝建立**的情形：来源条目不存在（`source_missing`）、来源摘要与
  `expected_digest` 不符（`source_digest_mismatch`）、来源键存在未过期的
  open 上传会话（`source_uploading`）、`copy` 模式目标键已存在
  （`target_exists`）、来源与目标指向同一条目（`same_entry`）。
- **提交（commit）前逐项复核**，任一状态漂移都让旧请求**明确失败**
  （`PromotionStaleError`，记录进入 `failed` 终态），绝不覆盖新状态：
  - 来源摘要变化（或来源消失）→ `source_changed`；
  - 目标命名空间配额版本 / 上限变化 → `quota_version_changed`；
  - `replace` 目标条目被推进（或 `copy` 目标键出现）→ `target_changed`；
  - 冻结的淘汰决定在提交时已腾不出足够空间 → `eviction_decision_stale`。
- **淘汰候选保护**：提交时逐候选用最新状态复核——提交期间被**重新访问**
  （LRU 时间前移）、**固定**或**重新发布**的候选一律跳过（沿用淘汰决定的
  `skipped_accessed` / `skipped_pinned` / `skipped_republished` 语义）；
  有效固定项永不淘汰。整个旧决定因此失效、无法满足配额时，晋级失败且
  **不删除任何条目**，这些候选记为 `skipped_stale`。
- **本次晋级依赖的内容不被淘汰**：目标写入键被排除；同命名空间内复制时，
  来源键也被排除，避免"删掉自己依赖的内容"。
- **原子提交**：目标条目（存储层 CAS 条件兜底）→ 删除淘汰条目 → 落定淘汰决定
  → 落定晋级记录 → 审计，全部在同一把服务锁内；任一步持久化失败都会**完整
  回滚**（恢复被覆盖旧目标与被删候选、记录退回 `proposed`），保持两端原状，
  解除故障后可用同一请求号重试。
- **内容块跨命名空间共享**：目标条目直接引用来源的同一组 blob，引用由 GC 统一
  按 `entry / session / pin` 计数。来源条目之后被删除或覆盖，也**不会回收**
  仍由目标引用的块；两端都消失后下一次 GC 才回收。
- **请求号幂等**：相同 `request_id` + 相同请求指纹返回**首次结果**（成功返回
  同一目标条目；失败返回同一 stale 错误）；相同请求号 + 不同内容（指纹不同）
  返回 `PromotionRequestConflictError`。配额版本变化属于"状态漂移"而非指纹
  变化，只影响尚未提交的请求。

晋级结果可通过 `GetPromotion(requestID)` / `ListPromotions(namespace)` 查询，
其中含目标条目身份、被替换版本、两端共享块数、配额前后用量（`used_before` /
`used_after`）与随附的淘汰决定；某个条目与另一端条目的共享关系用
`PromotionLinks(namespace, key)` 查询；内容块的跨命名空间引用仍由
`BlobReferences(digest)` 给出（同一块会出现两条 `entry` 引用）。

### 租约与统一时钟

- 所有"当前时间"一律取自 `Clock` 接口（生产用 `SystemClock`，测试用 `FakeClock`），
  代码中不直接调用 `time.Now()` 做过期判断。
- 会话有 `expires_at`；过期后上传 / 续期 / 完成一律返回 `ErrLeaseExpired`，
  **过期会话不会被任何操作复活**（记录仍保留，直到清理器统一删除）。
- 过期清理、取消、上传、完成发布、固定续租解除、过期固定扫描、配额淘汰、GC
  全部在同一把服务锁下串行，因此"失效"与"已发布/已固定内容"的判定不会相互踩踏。

### 垃圾回收（mark → sweep）

1. 清理已取消 / 已过期的会话（它们不再保护任何块）；
2. 扫描到期固定租约，将其翻转为 `expired`（版本 +1，写审计）；
3. **Mark**：汇总三类存活引用——已发布条目、未过期活跃会话、**未到期 active 固定
   快照**——引用的 blob；
4. **Sweep**：逐块删除未标记内容；删除每个块之前基于最新引用集**二次复核**，
   若标记后出现了新引用则保留并写审计（`blob_skip_in_use`）。

由于 mark 与 sweep 与"发布 / 固定 / 淘汰"在同一临界区，一个引用建立要么完整发生在
GC 快照之前（块被标记保留），要么只能发生在 GC 之后；不存在标记到删除之间被抢先
建立引用而误删的窗口。

每次 GC 的决策（开始、会话/固定清理、块删除及原因、保留、结束、代次、时间戳）与
每次配额淘汰都写入**追加式审计日志**，可通过 `AuditLog()` 或 `GET /v1/audit` 查询。

### 查询接口

- **配额**：`NamespaceStatus()` / `ListNamespaceStatus()` 返回上限、已用、剩余
  （不限时为 -1）、条目数、活跃固定数及其快照引用体量。
- **固定租约**：`GetPin(ns,key)` 取单个租约；`ListPins(namespace, activeOnly)` 列出。
- **淘汰决定**：`GetEvictionDecision(id)` / `ListEvictionDecisions(namespace)`
  返回每次决策及逐候选结果。
- **晋级**：`GetPromotion(requestID)` / `ListPromotions(namespace)` 返回晋级结果、
  被替换版本、共享块数、配额前后用量与随附淘汰决定；
  `PromotionLinks(namespace, key)` 返回一个条目与另一端条目的晋级共享关系。
- **内容引用**：`BlobReferences(digest)` 返回某块当前的全部引用（`entry` /
  `session` / `pin`），并标注引用是否仍有效（会话是否 open 未过期、固定是否未到期）。
  晋级后同一块会被来源与目标两个 `entry` 引用。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `cache.go` | `Cache` 服务：会话/发布/配额淘汰/固定/清理/GC/查询，全部并发控制 |
| `promotion.go` | 跨命名空间晋级：两阶段建立/提交、冻结快照、配额淘汰复核、原子落定与回滚、查询 |
| `model.go` | `Namespace` / `Pin` / `PinRequest` / `EvictionDecision` / `Promotion` / `Entry` 等持久化元数据 |
| `digest.go` | 摘要类型与校验（`sha256:hex`） |
| `clock.go` | `Clock` / `SystemClock` / `FakeClock` 统一时间来源 |
| `errors.go` | 按类别区分的错误：摘要、分片、租约、版本、配额、固定冲突 |
| `store.go` | `Store` 存储抽象（blob / 会话 / 命名空间 / 带 CAS 条目 / 固定 / 请求号 / 决策 / 审计） |
| `memory_store.go` | 进程内实现（测试用） |
| `file_store.go` | 文件持久化实现：临时文件 + `rename` 原子写，重启恢复，旧版条目迁移到默认命名空间 |
| `reader.go` | 按分片顺序拼接的已发布条目读取器 |
| `api.go` / `api_promotion.go` | HTTP 接口与错误码映射 / 晋级与两端关系端点 |
| `cmd/buildcached/main.go` | 可运行服务，带可选后台 GC 与默认配额参数 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST/GET | `/v1/namespaces` | 创建/更新配额（`{"name","max_bytes"}`）/ 列出全部配额状态 |
| GET/DELETE | `/v1/namespaces/{name}` | 查询配额 / 删除空命名空间 |
| POST | `/v1/sessions` | 创建会话（可带 `namespace`、`idempotency_key`、`lease_seconds`） |
| GET | `/v1/sessions/{id}` | 查询会话状态 |
| POST | `/v1/sessions/{id}/chunks/{index}` | 上传分片（body 为原始字节） |
| POST | `/v1/sessions/{id}/renew` | 续期（`{"extend_seconds": 900}`） |
| POST | `/v1/sessions/{id}/complete` | 校验并原子发布（可带 `expected_version`，超配额时返回淘汰决策） |
| DELETE | `/v1/sessions/{id}` | 取消会话 |
| GET | `/v1/entries/{key...}` | 读取内容（`?namespace=` 或 `X-Namespace`，默认 default；刷新 LRU） |
| POST/GET | `/v1/pins` | 按 body `action` 固定（默认）/ `renew` / `unpin`；GET 列出（`?namespace=&active=`） |
| GET | `/v1/pins/{namespace}/{key...}` | 查询单个固定租约 |
| POST/GET | `/v1/evictions` | 建立淘汰决策（`{"namespace","need_bytes"}`）/ 列出决策 |
| POST/GET | `/v1/evictions/{id}/commit`、`/v1/evictions/{id}` | 提交决策 / 查询决策 |
| POST/GET | `/v1/promotions` | 建立晋级（可 `commit:false` 只建立；缺省建立并原子提交）/ 列出晋级（`?namespace=`） |
| POST/GET | `/v1/promotions/{id}/commit`、`/v1/promotions/{id}` | 提交晋级请求 / 查询晋级结果 |
| GET | `/v1/promotion-links?namespace=&key=` | 查询条目与另一端条目的晋级共享关系 |
| GET | `/v1/blobs/{digest}/refs` | 查询内容块的全部引用 |
| POST | `/v1/gc` | 同步执行一次垃圾回收（含到期固定扫描），返回决策报告 |
| GET | `/v1/audit` | 查看审计记录 |
| GET | `/v1/healthz` | 健康检查 |

固定请求 body 示例：

```json
{
  "action": "pin",
  "namespace": "team-a",
  "key": "mod/github.com/x/y@v1.2.3",
  "expected_digest": "sha256:...",
  "ttl_seconds": 3600,
  "expected_version": 3,
  "request_id": "pin-req-0001"
}
```

续租用 `{"action":"renew","extend_seconds":900,...}`，解除用
`{"action":"unpin",...}`；截止时间也可用 RFC3339 的 `deadline` 绝对时间指定。

晋级请求 body 示例（缺省建立并原子提交；传 `"commit": false` 只建立，
之后调用 `/v1/promotions/{id}/commit`）：

```json
{
  "source_namespace": "staging",
  "source_key": "mod/github.com/x/y@v1.2.3",
  "expected_digest": "sha256:...",
  "target_namespace": "release",
  "target_key": "mod/github.com/x/y@v1.2.3",
  "mode": "copy",
  "request_id": "promo-req-0001"
}
```

`mode` 为 `copy`（复制为新键，目标键必须不存在）或 `replace`（替换同名旧条目）。
目标键省略时与来源键同名。

错误响应统一为 `{"error": "<code>", "message": "..."}`，典型状态码：

| 场景 | HTTP | error code |
| --- | --- | --- |
| 会话 / 条目 / 命名空间 / 固定 / 晋级不存在 | 404 | `session_not_found` / `entry_not_found` / `namespace_not_found` / `pin_not_found` / `promotion_not_found` / `source_entry_not_found` |
| 租约过期 | 410 | `lease_expired` |
| 会话已完成/取消、幂等键冲突、固定不活跃、固定版本冲突、请求号冲突 | 409 | `session_not_active` / `idempotency_conflict` / `pin_not_active` / `pin_version_conflict` / `pin_request_conflict` |
| 摘要/分片内容冲突、缺片 | 422 / 409 | `digest_mismatch` / `chunk_conflict` / `chunks_incomplete` |
| 版本条件 / 固定摘要不匹配 | 412 | `version_conflict` / `pin_digest_conflict` |
| 晋级来源摘要漂移、配额版本变化、目标被推进、旧淘汰决定失效 | 412 | `promotion_stale` |
| 晋级来源摘要与请求不符 | 412 | `promotion_digest_mismatch` |
| 晋级 copy 目标已存在、晋级请求号异内容冲突、来源正在上传 | 409 | `promotion_target_exists` / `promotion_request_conflict` / `promotion_conflict` |
| 命名空间配额不足（LRU 也腾不出空间） | 507 | `quota_exceeded` |

## 使用示例（Go API）

```go
store, _ := buildcache.NewFileStore("./data")
cache, _ := buildcache.New(store, buildcache.SystemClock{}, 15*time.Minute)

// 注册一个 1 GiB 的命名空间。
cache.SetNamespaceQuota("team-a", 1<<30)

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

res, err := cache.Complete(sess.ID, buildcache.CompleteOptions{})
// res.Eviction 非 nil 时说明本次发布触发了未固定 LRU 淘汰（含逐候选结果）。

// 把发布结果固定一小时（可带 expected_digest 防止固定到错误版本）。
pin, _ := cache.Pin(buildcache.PinOptions{
    Namespace: "team-a", Key: "mod/github.com/x/y@v1.2.3",
    ExpectedDigest: res.Entry.Digest, TTL: time.Hour, RequestID: "req-1",
})
// 续租（单调延长、版本 +1）：
cache.RenewPin(buildcache.RenewPinOptions{
    Namespace: "team-a", Key: pin.Key, Extend: time.Hour,
    ExpectedVersion: &pin.Version,
})
// 解除：
// cache.Unpin(buildcache.UnpinOptions{Namespace: "team-a", Key: pin.Key})

r, _ := cache.Read("team-a", "mod/github.com/x/y@v1.2.3")
defer r.Close()
io.Copy(os.Stdout, r)

// 把已发布条目从 staging 晋级到 release（两命名空间共享同一组内容块）。
prom, err := cache.Promote(buildcache.PromoteOptions{
    SourceNamespace: "staging", SourceKey: "mod/github.com/x/y@v1.2.3",
    ExpectedDigest:  res.Entry.Digest,
    TargetNamespace: "release", TargetKey: "mod/github.com/x/y@v1.2.3",
    Mode:            buildcache.PromoteCopy, RequestID: "promo-req-1",
})
// prom.ResultEntry / prom.UsedAfter / prom.Eviction 给出目标条目、配额变化与淘汰结果。
_ = prom
links, _ := cache.PromotionLinks("staging", "mod/github.com/x/y@v1.2.3") // 两端关系

st, _ := cache.NamespaceStatus("team-a")                 // 配额占用
refs, _ := cache.BlobReferences(res.Entry.Chunks[0].Digest) // 内容块被谁引用
report, _ := cache.CollectGarbage()                       // 只回收无三类引用的块
```

## 运行服务

```sh
go run ./cmd/buildcached -addr :8080 -data ./data -ttl 15m -gc-interval 10m \
  -default-quota 10737418240
```

## 测试

```sh
go test ./...              # 全部单元 / 持久化 / HTTP 端到端测试
go test -race ./...        # 含竞态检测（含 GC、并发发布、并发固定对打测试）
```

测试覆盖：

- 既有：乱序上传与重传幂等、分片冲突分类、完成三校验、不可见性、同摘要复用与版本
  CAS、租约过期不复活、过期清理、取消、幂等键冲突、GC 引用保护、覆盖发布后旧块回收、
  8×25 并发发布与 GC 对打、文件存储重启恢复、完整 HTTP 生命周期；
- 新增：
  - 配额：超配额按未固定 LRU + 键名稳定序淘汰、固定条目豁免、全部固定时 507 且不删
    数据、并发发布下配额不被突破且存活条目内容完整；
  - 两阶段淘汰：决策后访问 / 固定 / 重新发布分别使候选失效、提交幂等；
  - 固定：拒绝不存在条目与摘要不匹配、续租单调延长与版本严格递增、旧版本续租/解除被
    拒、过期扫描不复活、重新固定得到更高版本、请求号同内容回放/异内容冲突；
  - 块回收：固定快照在覆盖发布后保留旧块、解除后回收、过期固定失效；
  - 查询：配额状态、固定租约列表、淘汰决策、内容块三类引用查询；
  - HTTP：命名空间配额与 507、固定全生命周期与错误码、淘汰 plan/commit、块引用查询；
- 晋级（本轮新增）：
  - copy / replace（含目标缺省新建）基础晋级、两端共享块、来源删除后目标内容与块
    存活、两端都删除后块才回收；
  - 前置校验：来源缺失 / 摘要不符 / 正在上传 / copy 目标已存在 / 同源同键 /
    目标命名空间不存在；
  - 目标配额淘汰：未固定 LRU 生效、固定项豁免、全固定时失败且不删数据、
    同命名空间复制时来源键不被淘汰、`used_before`/`used_after` 正确；
  - 两阶段失效：提交期间候选被访问 / 固定 / 重新发布使旧淘汰决定失效，来源摘要
    变化、目标配额版本变化、replace 目标版本推进分别返回对应 stale 原因；
  - 请求号幂等：同号同内容返回首次结果、提交幂等、同号异内容冲突；
  - 原子性：在删除候选 / 落定淘汰决定 / 落定晋级记录 / 写审计各点注入持久化失败，
    验证两端原状恢复、记录退回 proposed 且解除故障后重试成功；
  - 持久化：FileStore 重启后晋级记录、两端关系、共享块与 proposed 请求可恢复，
    重启后同号重放返回首次结果；
  - 并发：多 goroutine 晋级 + 发布对打，目标配额不被突破、存活条目内容完整；
  - 查询与 HTTP：晋级结果 / 列表 / 两端关系 / 块跨命名空间引用查询，
    两阶段 412 `promotion_stale`、各类 404/409 错误码、随晋级返回的淘汰决定。
