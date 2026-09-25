# go-build-cache

内容寻址（content-addressed）的构建制品缓存：支持分片上传、租约管理、原子发布、
乐观并发控制与标记-清除垃圾回收。所有元数据持久化，进程重启后可完整恢复。

开发环境：Go 1.23.0。

## 核心模型与不变量

- **内容寻址块（blob）**：每个分片按其内容摘要（默认 `sha256:<hex>`）存储，
  `PutBlob` 天然幂等——相同内容重传等于无操作。
- **上传会话（session）**：创建时声明缓存键、最终制品预期摘要、总大小与完整分片集合
  （分片号 / 偏移 / 大小 / 单片摘要）。分片可以**乱序到达**。
- **已发布条目（entry）**：只有 `complete` 校验全部通过后才写入。
  **校验完成之前，读者通过缓存键看不到任何数据**（`Read` 返回 `entry_not_found`）。
- **引用关系**：`entry → 多个 blob`；`活跃 open 会话 → 已上传 blob`。
  垃圾回收只删除**既不被任何已发布键、也不被任何活跃会话引用**的块。

### 上传与冲突语义

- 分片乱序、相同内容重传：幂等成功。
- 同一分片号上传**不同内容**：`ChunkConflictError(content_mismatch)`。
- 分片大小不符：`ChunkConflictError(size_mismatch)`；单片摘要不符：`DigestMismatchError`。
- `complete` 依次核对：①分片齐全（否则 `chunks_incomplete`，返回缺失分片号）
  ②拼接总大小 ③按分片号顺序流式重算最终摘要。任一不符则**不发布**。

### 并发发布与版本条件

多个会话可同时尝试发布同一个键：

- 最终摘要**相同** → 复用现有条目，不产生新版本，所有会话都成功；
- 最终摘要**不同**且未带版本条件 → `VersionConflictError`（HTTP 412），拒绝覆盖；
- 带 `expected_version`：仅当当前版本恰好匹配才覆盖为 `version+1`。

版本条件（CAS）在**存储层**实现（`Store.PutEntry`），即使绕过本进程锁也不会出现
旧版本覆盖新版本。

### 租约与统一时钟

- 所有"当前时间"一律取自 `Clock` 接口（生产用 `SystemClock`，测试用 `FakeClock`），
  代码中不直接调用 `time.Now()` 做过期判断。
- 会话有 `expires_at`；过期后上传 / 续期 / 完成一律返回 `ErrLeaseExpired`，
  **过期会话不会被任何操作复活**（记录仍保留，直到清理器统一删除）。
- 过期清理、取消、上传、完成发布、GC 全部在同一把服务锁下串行，
  因此"失效会话"与"已发布内容"的判定不会相互踩踏：已发布内容永远不会被当作会话垃圾删除。

### 垃圾回收（mark → sweep）

1. 清理已取消 / 已过期的会话（它们不再保护任何块）；
2. **Mark**：汇总所有已发布条目与未过期活跃会话引用的 blob；
3. **Sweep**：逐块删除未标记内容；删除每个块之前基于最新引用集**二次复核**，
   若标记后出现了新引用则保留并写审计（`blob_skip_in_use`）。

由于 mark 与 sweep 与"发布条目"在同一临界区，一个发布要么完整发生在 GC 快照之前
（块被标记保留），要么只能发生在 GC 之后；不存在标记到删除之间被并发发布抢先建立
引用而误删的窗口。

每次 GC 的决策（开始、会话清理、块删除及原因、保留、结束、代次、时间戳）都写入
**追加式审计日志**，可通过 `AuditLog()` 或 `GET /v1/audit` 查询。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `cache.go` | `Cache` 服务：会话创建/上传/续期/完成/取消/清理/GC，全部并发控制 |
| `model.go` | `Session` / `Entry` / `ChunkSpec` / `GCRecord` 等持久化元数据 |
| `digest.go` | 摘要类型与校验（`sha256:hex`） |
| `clock.go` | `Clock` / `SystemClock` / `FakeClock` 统一时间来源 |
| `errors.go` | 按类别区分的错误：摘要、分片、租约、版本、幂等冲突 |
| `store.go` | `Store` 存储抽象（blob / 会话 / 带 CAS 的条目 / 审计） |
| `memory_store.go` | 进程内实现（测试用） |
| `file_store.go` | 文件持久化实现：临时文件 + `rename` 原子写，重启恢复 |
| `reader.go` | 按分片顺序拼接的已发布条目读取器 |
| `api.go` | HTTP 接口与错误码映射 |
| `cmd/buildcached/main.go` | 可运行服务，带可选后台 GC |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/sessions` | 创建会话（支持 `idempotency_key`、`lease_seconds`） |
| GET | `/v1/sessions/{id}` | 查询会话状态 |
| POST | `/v1/sessions/{id}/chunks/{index}` | 上传分片（body 为原始字节） |
| POST | `/v1/sessions/{id}/renew` | 续期（`{"extend_seconds": 900}`） |
| POST | `/v1/sessions/{id}/complete` | 校验并原子发布（可带 `expected_version`） |
| DELETE | `/v1/sessions/{id}` | 取消会话 |
| GET | `/v1/entries/{key...}` | 读取已发布内容（带 `ETag`、`X-Cache-Digest`、`X-Cache-Version`） |
| POST | `/v1/gc` | 同步执行一次垃圾回收，返回决策报告 |
| GET | `/v1/audit` | 查看审计记录 |
| GET | `/v1/healthz` | 健康检查 |

错误响应统一为 `{"error": "<code>", "message": "..."}`，典型状态码：

| 场景 | HTTP | error code |
| --- | --- | --- |
| 会话 / 条目不存在 | 404 | `session_not_found` / `entry_not_found` |
| 租约过期 | 410 | `lease_expired` |
| 会话已完成/取消、幂等键冲突 | 409 | `session_not_active` / `idempotency_conflict` |
| 摘要/分片内容冲突、缺片 | 422 / 409 | `digest_mismatch` / `chunk_conflict` / `chunks_incomplete` |
| 版本条件不满足 | 412 | `version_conflict` |

## 使用示例（Go API）

```go
store, _ := buildcache.NewFileStore("./data")
cache, _ := buildcache.New(store, buildcache.SystemClock{}, 15*time.Minute)

sess, err := cache.CreateSession(buildcache.CreateSessionOptions{
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
// 覆盖已有不同摘要的键时：
// v := res.Entry.Version
// cache.Complete(sess.ID, buildcache.CompleteOptions{ExpectedVersion: &v})

r, _ := cache.Read("mod/github.com/x/y@v1.2.3")
defer r.Close()
io.Copy(os.Stdout, r)

report, _ := cache.CollectGarbage() // 只回收无引用块；决策写入审计日志
```

## 运行服务

```sh
go run ./cmd/buildcached -addr :8080 -data ./data -ttl 15m -gc-interval 10m
```

## 测试

```sh
go test ./...              # 全部单元 / 持久化 / HTTP 端到端测试
go test -race ./...        # 含竞态检测（含 GC 与并发发布对打测试）
```

测试覆盖：乱序上传与重传幂等、分片冲突分类、完成三校验、不可见性、
同摘要复用与版本 CAS、租约过期不复活、过期清理、取消、幂等键冲突、
GC 引用保护（已发布 / 活跃会话 / 过期会话 / 无主块）、覆盖发布后旧块回收、
8×25 并发发布与 GC 对打下所有已发布内容保持完整、文件存储重启恢复、
以及完整 HTTP 生命周期。
