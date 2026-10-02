# 流处理的状态后端

Pair-wise GSB 标注任务仓库（第 15 批 / 218）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们的流式计算需要保存中间状态，重启后要恢复到崩溃前的位置，不能重复也不能丢。请从零实现一个流处理的状态后端组件。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ）。要求：1) 支持按 key 保存与读取状态，并按事件序号记录处理位点；2) 支持周期性快照：把状态与位点一起落盘，快照期间不阻塞状态更新；3) 支持恢复：从最近快照加载状态并从对应位点继续消费，需验证不重复处理已确认事件；4) 支持增量快照：只写变化过的 key，需说明与全量快照的取舍；5) 支持状态过期清理：按 key 的存活时间清理陈旧状态并统计清理量；6) 支持并发更新：多线程更新不同 key 不得互相阻塞，同一 key 的更新必须有序；7) 提供统计：状态 key 数、快照次数、恢复耗时与清理量；8) 测试覆盖状态读写、快照不阻塞、恢复不重复、增量快照、过期清理与并发更新；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。

## 实现说明（分支 A）

### 组件结构（`com.example.gsb.state`）

- `KeyedStateBackend`：核心后端。`ConcurrentHashMap` 保存 key→状态，`AtomicLong` 记录事件位点；所有单 key 操作走 `compute/computeIfPresent`。
- `SnapshotStore`：快照文件格式、原子落盘（`.tmp` + rename + `LATEST` 指针）、全量+增量链的恢复加载与旧链回收。
- `SnapshotScheduler`：周期性触发快照与过期清理（连续增量达阈值自动切换全量）。
- `StateCodec`：key/value 编解码（内置 UTF-8 String、int64，可自定义）。
- `BackendStats` / `SnapshotInfo`：统计与快照元信息记录。

### 关键设计

1. **按 key 的状态与位点**：`processEvent(key, seq, updater)` 仅在 `seq > 当前位点` 时原子地"改状态 + 推进位点"，否则返回 `DUPLICATE`。重启后从快照位点重放时，已确认事件全部判重跳过，不重复也不丢；另有 `get/put/update/remove/advancePosition`。
2. **快照不阻塞**：快照的"收集"阶段只在内存中读出条目并交换脏表（微秒~毫秒级，不持全局锁），编码与磁盘写入交给单线程后台 executor；更新线程只在自己 key 的哈希桶锁上短暂停留，快照期间可继续写。落盘文件先写临时文件再原子 rename，崩溃不会留下半成品。
3. **恢复**：`restore` 从 `LATEST` 指针找到最近快照，加载其所属全量并按序号回放其后全部增量，位点恢复到链头快照位点；`lastRecoveryMillis` 记录恢复耗时。
4. **增量快照与全量的取舍**：
   - 增量：维护脏 key 集合（含删除墓碑），快照只写变化的 key，IO/空间开销小、快照快；代价是恢复必须回放"全量 + 增量链"，链越长恢复越慢，墓碑也会累积。
   - 全量：每次写全部状态、单文件即可恢复、最简单可靠；代价是状态量大时 IO 抖动明显。
   - 折中：默认连续若干次增量后自动全量一次（`fullEveryN`），全量落盘后回收旧链，限制恢复回放长度；无全量时增量自动升级为全量。
   - 一致性：条目按快照位点过滤，只固化 `seq ≤ 快照位点` 的内容；收集窗口内的并发更新留在脏表交给下次快照，保证"快照状态不超过快照位点"，重放区间内事件可安全重做（at-least-once + 判重）。
5. **TTL 过期清理**：每个 key 携带过期时间（`defaultTtlMillis`，0 表示永不过期），`get` 读时即可见过期；`cleanupExpired()` 批量删除陈旧 key，删除走墓碑并累计 `totalKeysCleaned`，清理结果同样通过快照持久化。
6. **并发**：单 key 更新在哈希桶锁内串行，保证同一 key 严格有序、读改写不丢失；不同 key 落在不同桶位互不阻塞。
7. **统计**：`BackendStats` 提供存活 key 数、快照成功次数、最近恢复耗时、累计清理 key 数与当前位点。

### 测试（`src/test/java/com/example/gsb/state`，共 18 个）

- `KeyedStateBackendTest`：读写/覆盖/删除、位点推进、重复与乱序事件判重。
- `SnapshotNonBlockingTest`：快照收集耗时、落盘期间更新继续推进。
- `RestoreTest`：崩溃后恢复状态与位点、重放旧事件零副作用、从位点续传。
- `IncrementalSnapshotTest`：增量只含变化 key、全量+增量链（含删除墓碑）恢复、全量回收旧链。
- `ExpiryCleanupTest`：过期删除与计数、新 key 存活、过期状态经快照不复活、0 TTL 永不过期。
- `ConcurrentUpdateTest`：多线程不同 key 无串扰、一个 key 阻塞时其他 key 不被阻塞、同 key 8×5000 次更新结果精确不丢。
- `SnapshotSchedulerTest`：周期快照自动触发与增量→全量轮换。

```bash
mvn -q verify      # 或 ./mvnw -q verify
```
