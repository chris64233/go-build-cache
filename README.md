# go-build-cache

内容寻址的构建缓存服务：支持分片上传、原子发布、租约过期与垃圾回收，元数据持久化到本地磁盘。

开发环境：Go 1.23.0，无第三方依赖。

运行测试：

```sh
go test ./...
go test -race -count=1 ./...   # 含并发屏障用例，建议带 -race
```

## 快速上手

```go
st, err := buildcache.Open("./cache-data", buildcache.Options{
    Clock:    buildcache.RealClock{}, // 生产用墙钟；测试可注入 FakeClock
    LeaseTTL: 15 * time.Minute,
})
if err != nil { /* ... */ }
defer st.Close()

// 1. 客户端先自行分片并计算每片摘要与拼接后的总摘要
specs := []buildcache.ChunkSpec{
    {Index: 0, Size: int64(len(p0)), Hash: buildcache.Digest(sha0)},
    {Index: 1, Size: int64(len(p1)), Hash: buildcache.Digest(sha1)},
}
sess, err := st.CreateSession(buildcache.CreateSessionInput{
    Key:       "bin/linux/amd64/abc123",
    Digest:    buildcache.Digest(shaAll), // 拼接后内容的 SHA-256
    TotalSize: int64(len(p0) + len(p1)),
    Chunks:    specs,
})

// 2. 分片可乱序上传、可重传
_ = st.PutChunkBytes(sess.ID, 1, p1)
_ = st.PutChunkBytes(sess.ID, 0, p0)

// 3. 校验齐全性、总大小、最终摘要，全部通过才原子发布
entry, err := st.Complete(sess.ID)

// 4. 发布之后读者才看得到
rc, entry, err := st.OpenEntry("bin/linux/amd64/abc123")
defer rc.Close()

// 租约快到期可续期；不再需要可取消
_, _ = st.RenewLease(sess.ID)
_ = st.Cancel(sess.ID, "client aborted")

// 定期回收未被引用的内容块
res, err := st.CollectGarbage()
_ = st.ExpireSessions()
```

## 接口一览

| 接口 | 说明 |
| --- | --- |
| `Open(root, opts)` / `Close()` | 打开/初始化存储，启动时自动恢复磁盘元数据并清理遗留临时文件 |
| `CreateSession(in)` | 创建上传会话，声明缓存键、预期最终摘要、总大小、分片集合（序号须连续无空洞，片大小之和须等于总大小） |
| `PutChunk(sessionID, index, r)` / `PutChunkBytes` | 上传单个分片；允许乱序、允许相同内容重传 |
| `Complete(sessionID)` | 校验并原子发布；返回已发布（或复用）的条目 |
| `Get(key)` / `OpenEntry(key)` | 读取已发布条目的元数据 / 拼接内容流；未发布一律不可见 |
| `Cancel(sessionID, reason)` | 取消会话（幂等） |
| `RenewLease(sessionID)` / `Session(id)` | 租约续期 / 查询会话快照 |
| `ExpireSessions()` | 批量终结租约过期的活跃会话 |
| `CollectGarbage()` | 一轮带审计记录的垃圾回收 |
| `AuditLog()` | 读取 GC 决策审计日志（JSON Lines） |

## 设计要点

### 1. 会话创建：先声明，后上传

创建会话时必须给出缓存键、拼接内容的预期 SHA-256、总大小以及完整分片集合
（片号、每片大小、每片 SHA-256）。声明会做强校验：片号连续无空洞、无重复、
摘要合法、片大小之和等于总大小。分片声明允许乱序提交，内部会规范化。

### 2. 分片：乱序、重传、冲突与摘要错误分离

- 内容块存放在内容寻址存储（CAS，`blobs/<ab>/<64hex>`），写入路径是
  “临时文件 → fsync → 原子 rename”，未被接受的内容对任何读者都不可见。
- 分片可以任意乱序到达；同一片号上传**相同内容**是幂等成功。
- 同一片号上传**不同内容**返回 `*ChunkConflictError`（**幂等冲突**）：
  区别于该片号首次上传就与声明不符的 `*DigestMismatchError`（**摘要错误**）。
- 上传期间会话过期的，元数据提交会在锁内重新确认活跃状态，
  失效会话不会被迟到的分片“复活”。

### 3. 完成：三重校验后原子发布

`Complete` 依次校验：

1. 所有分片均已上传（否则 `*ChunkMissingError`）；
2. 从 CAS 顺序拼接的总大小等于声明值；
3. 拼接内容的 SHA-256 等于声明的最终摘要。

任一失败都不会产生读者可见的条目。全部通过后，条目元数据先原子写盘再置入
内存索引（同一把锁内完成），即“发布”是原子的——读者要么看不到，要么看到完整条目。

### 4. 并发发布：同摘要复用，异值版本冲突

多个会话同时发布同一个键时：

- 已发布条目的摘要与本次**相同**：内容寻址下值相同，直接复用现有条目，
  会话标记为完成（`ReusedEntry=true`），并发下保证恰有一个会话真正发布；
- 摘要**不同**：返回 `*VersionConflictError`，其中携带已发布版本号
  （`Version`，首次发布为 1，单调递增）与双方摘要，借助版本条件拒绝异值覆盖，
  已有条目保持不变。

### 5. 租约与统一时钟

- 所有过期判断都只经过注入的 `Clock`（`Options.Clock`，默认 `RealClock`），
  测试使用 `FakeClock` 精确推进时间，系统中不存在第二处时间来源。
- 会话有 `CreatedAt` / `ExpiresAt`，租约 = 创建或续期时刻 + `LeaseTTL`。
- 过期后上传、完成、续期一律返回 `ErrLeaseExpired`；过期既会在操作时惰性落盘，
  也可由 `ExpireSessions()` 批量清理。
- 已完成发布的会话永不过期；过期清理与上传/完成并发时，靠“操作前检查 +
  提交前锁内复查”保证过期会话不复活、已发布内容不被误删。

### 6. 垃圾回收：两阶段标记 + 发布屏障 + 审计

GC 只删除**未被任何已发布键或活跃会话引用**的内容块。引用集合包含：

- 全部已发布条目声明的分片；
- 未过期活跃会话（及已完成会话）声明的分片。

为防住“标记到删除之间”并发发布即将建立的引用，`Complete` 在获取锁之前先向
`inflight` WaitGroup 登记，GC 采用两阶段屏障：

1. **第一次标记**（持锁快照）；
2. **屏障等待**所有在途发布排空——Wait 返回后不存在“已决定发布但引用尚未计入”的发布；
3. **持锁第二次标记并清扫**：只有两轮都不可达的块才删除。

审计分类（每个块一条记录，追加写入 `gc-audit.log`，先落盘后删除）：

| Action | 含义 |
| --- | --- |
| `keep_published` | 被已发布条目引用 |
| `keep_active_session` | 被未过期会话引用 |
| `keep_inflight_publish` | 第一轮还没有条目引用，屏障窗口内并发发布建立了引用，被屏障救下 |
| `delete` | 两轮标记均不可达，删除 |
| `retry_remove_failed` | 决策删除但文件删除失败，保留待下轮重试 |

同一时刻只允许一轮 GC，并发调用返回 `ErrGCBusy`。

### 7. 持久化与崩溃恢复

```
<root>/
├── blobs/<ab>/<sha256>      # 不可变内容块（CAS）
├── sessions/<session>.json  # 会话元数据，每次状态变更原子重写
├── entries/<hex(key)>.json  # 已发布条目，原子写入
└── gc-audit.log             # GC 决策审计（JSON Lines，只追加）
```

- 所有元数据写入均为“临时文件 + fsync + rename”，崩溃不会产生半截文件；
- `Open` 时扫描恢复全部会话与条目，并清理上次运行遗留的 `.tmp-*` 上传临时文件；
- 会话的上传进度、终结状态（完成/取消/过期）都会落盘，重开后可继续上传或直接判过期。

## 错误分类

| 错误 | 类别 |
| --- | --- |
| `ErrInvalidArgument` | 参数非法（空键、坏摘要、分片空洞、大小不符等） |
| `*DigestMismatchError` | **摘要错误**：分片内容与声明不符（`Scope=chunk`）或拼接结果与最终声明不符（`Scope=entry`） |
| `*ChunkConflictError` | **幂等冲突**：同一分片号先后上传了不同内容 |
| `*ChunkMissingError` | 完成时分片未齐 |
| `*VersionConflictError` | **版本冲突**：同键异值发布被版本条件拒绝 |
| `ErrLeaseExpired` / `ErrSessionCompleted` / `ErrSessionCancelled` | **租约/生命周期**错误 |
| `ErrSessionNotFound` / `ErrEntryNotFound` / `ErrChunkIndex` / `ErrGCBusy` / `ErrClosed` | 其他状态错误 |

## 代码结构

- `doc.go`：包级设计说明
- `clock.go`：`Clock`、`RealClock`、`FakeClock` 与摘要类型
- `errors.go`：哨兵错误与结构化错误（摘要/冲突/缺片/版本冲突）
- `types.go`：`Session`、`Entry`、`ChunkSpec`、GC 决策与结果类型
- `blobstore.go`：内容寻址块存储（暂存、校验、原子发布、枚举）
- `store.go`：会话、发布、租约、过期与两阶段 GC 的核心实现
- `store_test.go`：覆盖上述全部语义与并发竞态的自动化测试
