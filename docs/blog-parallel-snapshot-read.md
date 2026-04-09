<!-- 备选标题（选定后删除其余）：
1. 并行切片 + 原子读取：OceanBase Flink 连接器快照读取设计解析
2. 从单线程到并行 Exactly-Once：OceanBase Flink 连接器快照读取的设计与实现
3. 深入 OceanBase Flink Source：异步分片、原子读取与 Exactly-Once 语义
4. 大表不再等待：OceanBase Flink 连接器并行快照读取设计解析
5. 一次 pollNext() 的哲学：OceanBase Flink 连接器如何实现并行快照 Exactly-Once 读取
6. 异步切分 + 原子发送：OceanBase Flink JDBC Source 并行读取设计详解
7. 告别全表慢扫：OceanBase Flink 连接器并行快照读取核心设计
8. OceanBase × Flink：并行快照读取如何做到高效且 Exactly-Once
-->

# 并行切片 + 原子读取：OceanBase Flink 连接器快照读取设计解析

在大数据生态中，OceanBase 作为分布式关系型数据库，承载着海量业务数据。Apache Flink 作为主流的流批一体处理引擎，是数据集成链路中的关键枢纽。当我们需要将 OceanBase 中的全量数据高效导入 Flink 进行离线分析或批量处理时，单线程的 `SELECT *` 显然无法满足大表场景下的性能需求。本文将深入解析 OceanBase Flink 连接器中**并行快照读取**功能的设计原理，重点介绍异步分片、差异化切片策略和容错机制三大核心设计。

## 一、为什么需要并行快照读取？

假设一张 OceanBase 表有 1 亿行数据，传统的单线程读取方式存在明显瓶颈：

- **吞吐受限**：单个 JDBC 连接的网络带宽和数据库查询能力是有限的，读取 1 亿行可能需要数十分钟甚至更久。
- **资源浪费**：Flink 任务通常配置了多个并行度（parallelism），但如果 Source 只有一个线程在工作，其余 TaskManager 都在空转。
- **无法容错**：一旦读取过程中发生故障，只能从头重来。

并行快照读取的核心思路是：**将表按主键范围切分为多个互不重叠的分片（Split），由 Flink 的多个并行 Reader 同时读取**。这就引出了三个核心挑战：

1. 如何高效、均匀地切分表？
2. MySQL 模式与 Oracle 模式的主键差异如何处理？
3. 故障恢复时如何做到不重不漏（Exactly-Once）？

## 二、整体架构：基于 Flink Source V2 API

OceanBase Flink 连接器的并行快照读取基于 Flink 社区的 **FLIP-27 Source V2 API** 构建。该 API 将 Source 拆分为两个核心角色：

- **SplitEnumerator（分片协调者）**：运行在 JobManager 上，负责发现分片、计算切分点、将分片分配给各个 Reader。
- **SourceReader（数据读取者）**：运行在 TaskManager 上，每个并行实例负责读取分配到的分片数据。

```
                    ┌─────────────────────────────┐
                    │       OceanBaseSource        │
                    │     (Boundedness.BOUNDED)    │
                    └──────────────┬──────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                                         ▼
┌──────────────────────────┐              ┌──────────────────────────┐
│  OceanBaseSplitEnumerator │              │  OceanBaseSourceReader   │
│      (JobManager)        │              │     (TaskManager × N)    │
│                          │   assign     │                          │
│  - 异步发现分片           │ ──────────► │  - JDBC 全量读取分片       │
│  - 计算切分点             │              │  - 内存缓冲 + 原子发送    │
│  - 分配分片给 Reader      │   request   │  - 类型转换 → RowData     │
│  - 管理 checkpoint 状态   │ ◄────────── │  - checkpoint 快照       │
└──────────────────────────┘              └──────────────────────────┘
```

一个分片的完整生命周期为：**发现 → 分配 → 读取 → 完成 → 请求下一个分片**。当所有分片都被读取完毕，Source 向 Flink 报告 `END_OF_INPUT`，整个快照读取结束。

## 三、异步分片：边算边读的流式设计

### 3.1 痛点：同步分片与深分页问题

最直觉的分片实现是：在 `SplitEnumerator.start()` 中**同步**计算所有切分点，然后一次性分配给 Reader。这对于数值型主键（如 `BIGINT`）没有问题——切分点可以通过简单的算术运算（`(max - min) / numSplits`）瞬间算出。

但对于**字符串主键**（如 `VARCHAR`）的大表，情况完全不同。字符串无法做数学等分，每个切分点都需要执行一条 SQL 查询。如果使用朴素的 `LIMIT/OFFSET` 方式（如 `SELECT name FROM users ORDER BY name LIMIT 1 OFFSET 250000`），会遇到 MySQL 经典的**深分页问题**：MySQL 必须扫描前 25 万行再丢弃，只返回第 25 万零一行。越往后的分片，OFFSET 越大，扫描代价呈线性增长。

OceanBase Flink 连接器采用了**逐段推进（next-chunk-max）**策略来规避这一问题：

```sql
-- MySQL 模式：从上一个边界值开始，取 chunkSize 行的最大值
SELECT MAX(`name`) FROM (
    SELECT `name` FROM `db`.`users`
    WHERE `name` >= ?
    ORDER BY `name` ASC
    LIMIT ?
) AS T;

-- Oracle 模式：通过 ROWNUM 实现等价逻辑
SELECT MAX(val) FROM (
    SELECT val FROM (
        SELECT "ROWID" AS val FROM "DB"."USERS"
        WHERE "ROWID" >= ?
        ORDER BY "ROWID"
    ) WHERE ROWNUM <= ?
);
```

每次查询从上一个切分边界开始，只向前扫描 `chunkSize` 行（即 `split-size` 配置的值），取其最大值作为下一个边界。**无论表有多大、当前在第几个分片，每次查询的代价始终是 O(chunkSize)**。同时，如果遇到大量重复值导致边界相同，还会通过 `SELECT MIN(col) FROM table WHERE col > ?` 跳过重复区间，确保每个分片都有有效的数据范围。

但即便每次查询很快，对于 1000 万行、按 10000 行一个分片切分的表，仍需约 1000 次迭代查询。同步等待所有查询完成后 Reader 才能开始读数据，用户依然面临启动等待。

### 3.2 方案：后台线程 + 增量分配

为此，我们将分片发现改为**后台线程异步执行**：

```java
private void startAsyncSplitDiscovery() {
    // 启动后台分片发现线程
    splitDiscoveryExecutor = Executors.newSingleThreadExecutor(
            r -> { Thread t = new Thread(r, "ob-split-discovery"); t.setDaemon(true); return t; });
    splitDiscoveryExecutor.submit(this::discoverSplitsAsync);

    // 利用 Flink 的 callAsync 定期检查并分配新发现的分片
    context.callAsync(
            () -> !splitDiscoveryFinished,
            (stillRunning, error) -> {
                assignPendingSplits();
                if (splitDiscoveryFinished) signalNoMoreSplitsIfDone();
            },
            0, SPLIT_CHECK_INTERVAL_MS);
}
```

核心设计要点：

1. **数值型主键一次算完**：对于 `BIGINT`、`DECIMAL`、`DOUBLE` 等数值类型，切分点通过纯算术计算，无需查库，一次性生成所有分片。
2. **非数值型主键逐段推进**：通过 next-chunk-max 查询迭代计算每个切分点，每算出一个切分点，立即生成对应的分片并加入待分配队列。Reader 可以**零等待**地开始读取第一个分片。
3. **定期分配**：通过 Flink 的 `context.callAsync()` 每 200ms 检查一次待分配队列，将新发现的分片分配给空闲的 Reader。

```
时间线：
  后台线程：  [算分片0] [算分片1] [算分片2] [算分片3] ... [算分片999] ✓完成
  Reader 0：           [=====读分片0=====] [=====读分片4=====] ...
  Reader 1：                    [=====读分片1=====] [=====读分片5=====] ...
  Reader 2：                             [====读分片2====] [====读分片6====] ...
  Reader 3：                                      [===读分片3===] ...
```

这种设计让 Reader 在分片计算完成之前就能开始工作，将分片计算的延迟完全隐藏在数据读取过程中。

### 3.3 Checkpoint 恢复时的分片去重

如果在异步分片过程中发生了 checkpoint，enumerator 的状态会保存当前已发现的分片和 `splitDiscoveryFinished` 标志。当作业从 checkpoint 恢复时：

- 如果发现已完成（`splitDiscoveryFinished = true`）：直接使用恢复的分片，无需重新发现。
- 如果发现未完成（`splitDiscoveryFinished = false`）：重新启动异步发现，同时通过分片边界去重（`knownSplitKeys`），跳过已存在的分片，确保不会重复。

## 四、切片策略：MySQL 与 Oracle 的差异化设计

OceanBase 同时兼容 MySQL 和 Oracle 两种模式，两者在主键机制上存在本质差异，我们为此设计了差异化的切片策略。

### 4.1 MySQL 模式：基于主键切片

MySQL 模式下，通过标准 JDBC API `DatabaseMetaData.getPrimaryKeys()` 获取主键列：

```java
private String getPrimaryKeyColumn() throws SQLException {
    try (Connection conn = getDataSource().getConnection()) {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(
                null, config.getSchemaName(), config.getTableName())) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
        }
    }
    return null; // 无主键时退化为单分片全表扫描
}
```

这里选择 `DatabaseMetaData.getPrimaryKeys()` 而非直接查询 `information_schema` 的原因：

- **标准性**：这是 JDBC 标准 API，不依赖 OceanBase 特定的系统表结构。
- **可移植性**：相同的代码可以运行在任何 JDBC 兼容的数据库上。
- **安全性**：不需要额外的系统表读取权限。

### 4.2 Oracle 模式：基于 ROWID 切片

Oracle 模式下，我们利用 `ROWID` 伪列作为切片键。`ROWID` 是 Oracle 为每一行数据自动生成的唯一物理地址标识，具有以下优势：

- **始终可用**：不依赖用户是否定义了主键。
- **天然有序**：ROWID 的排序与物理存储顺序一致，范围查询效率高。
- **唯一性**：每行数据的 ROWID 唯一，适合作为切分键。

```java
private String getDefaultSplitColumn() throws SQLException {
    if (config.isOracleMode()) {
        return "ROWID";  // Oracle 模式始终使用 ROWID
    } else {
        return getPrimaryKeyColumn();  // MySQL 模式使用主键
    }
}
```

### 4.3 切分点计算：按类型精准划分

根据切片键的数据类型，采用不同的切分策略：

|          数据类型           |         切分方式         |  是否需要查库   |                       示例                       |
|-------------------------|----------------------|-----------|------------------------------------------------|
| `BIGINT` / `INT`        | 整数等分                 | 否         | `min=0, max=1000, 4片 → [0, 250, 500, 750]`     |
| `DECIMAL`               | BigDecimal 精确等分      | 否         | `min=0.00, max=10.00, 4片 → [2.50, 5.00, 7.50]` |
| `DOUBLE` / `FLOAT`      | 浮点等分                 | 否         | `min=0.0, max=100.0, 4片 → [25.0, 50.0, 75.0]`  |
| `VARCHAR` / `DATE` / 其他 | 逐段推进（next-chunk-max） | 是，每个切分点一次 | 从上一个边界取 chunkSize 行的 MAX 值作为下一个边界              |

对于需要查库的非数值类型，采用逐段推进策略（避免深分页），每得到一个切分点就立即生成分片——这就是前文所述的异步增量分片机制发挥作用的地方。

## 五、容错与数据一致性

数据不丢失是 Source 连接器的底线。OceanBase Flink 连接器的并行快照读取同时支持 Flink 的批模式和流模式，两种模式下通过不同机制保障容错：

- **分片边界（两种模式通用）**：每个分片的数据范围由切分点精确界定，各分片之间严格互不重叠，从源头上保证每行数据只属于一个分片。
- **批模式（Region Failover）**：Flink 批模式不使用 checkpoint，而是通过 blocking shuffle 持久化已完成 task 的中间结果。某个 task 失败时，Flink 仅重新执行受影响的 region（即失败的 task 及其依赖链），已完成的 task 无需重跑。对于 Source 而言，失败的分片从头重读即可，分片边界本身保证了不重不漏。
- **流模式（Checkpoint + 原子分片读取）**：通过将分片的读取与发送**全部在单次 `pollNext()` 调用中完成**，利用 Flink 的 checkpoint 机制保证了 Source 级别的 Exactly-Once 语义。

以下重点介绍流模式下的容错机制。

### 5.1 原子分片读取：单次 pollNext() 完成读取与发送

一个直觉的实现是边读边发——每从数据库读取一行，立即通过 `output.collect()` 发送给下游算子。但这种设计在 checkpoint 机制下存在根本性缺陷：Flink 在两次 `pollNext()` 调用之间注入 checkpoint barrier，如果每次 `pollNext()` 只发送一行，checkpoint 就可能插在同一个分片的两行之间。故障恢复时，Source 必须从某个中间位置续读，但如果切片键存在重复值（如联合主键的第一列），无论使用 `>` 还是 `>=` 运算符，都无法精确定位到"哪些行已发送、哪些未发送"——要么丢数据（`>`），要么重复发送已提交数据（`>=`）。

OceanBase Flink 连接器利用了 Flink 的一个关键机制来彻底解决这一问题：**checkpoint barrier 只在 `pollNext()` 调用之间注入，不会打断单次 `pollNext()` 的执行**。因此，只要将一个分片的全部读取和发送放在同一次 `pollNext()` 调用中完成，整个分片的处理就是原子的——不存在"读了一半触发 checkpoint"的可能。

```java
@Override
public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
    // ...
    // 在单次 pollNext() 中完成：JDBC 读取 → 内存缓冲 → 全量发送
    // checkpoint barrier 无法插入这一过程 → 分片级原子性
    List<RowData> rows = bufferAllRowsFromSplit(currentSplit);
    for (RowData row : rows) {
        output.collect(row);
    }
    currentSplit = null;  // 分片处理完毕
    // ...
}
```

整体执行流程如下：

```
                    checkpoint barrier 可以插入的位置
                              ↓
... ─── pollNext(split-0) ─── | ─── pollNext(split-1) ─── | ─── pollNext(split-2) ─── ...
         ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
         │ JDBC 全量读取  │          │ JDBC 全量读取  │          │ JDBC 全量读取  │
         │ 内存缓冲       │          │ 内存缓冲       │          │ 内存缓冲       │
         │ 全量 collect   │          │ 全量 collect   │          │ 全量 collect   │
         │ currentSplit=  │          │ currentSplit=  │          │ currentSplit=  │
         │   null (完成)  │          │   null (完成)  │          │   null (完成)  │
         └──────────────┘          └──────────────┘          └──────────────┘
```

当 Flink 在两次 `pollNext()` 之间触发 checkpoint 时，`snapshotState()` 被调用。此时 `currentSplit` 为 `null`（上一个分片已在 `pollNext()` 中完整处理），只有尚未开始处理的 pending 分片会被保存到状态中。恢复时只需重新读取这些未处理的分片，**已完成的分片不会被重复读取**。

### 5.2 Source 级别 Exactly-Once

在原子分片读取设计下，`snapshotState()` 的语义变得非常清晰：

```java
public List<OceanBaseSplit> snapshotState(long checkpointId) {
    // snapshotState() 在 pollNext() 之间调用（同一线程）。
    // currentSplit 非空 = 已分配但尚未处理的分片（pollNext 还没执行到它）。
    // currentSplit 为空 = 上一个分片已经在 pollNext 中完整读取+发送。
    List<OceanBaseSplit> state = new ArrayList<>();
    if (currentSplit != null) {
        state.add(currentSplit);
    }
    state.addAll(pendingSplits);
    return state;
}
```

由于每个分片要么完全没处理（在 pending 队列中），要么已完整处理（`currentSplit = null`），不存在"处理了一半"的中间状态。因此 Source 级别的语义是 **Exactly-Once**：恢复时只重新读取未完成的分片，不会重复发送已完成分片的数据。

### 5.3 分片状态的双保险

Enumerator 的 checkpoint 状态同时保存**待分配分片**（pending）和**已分配但未完成的分片**（in-flight）：

```java
public OceanBaseEnumeratorState snapshotState(long checkpointId) {
    return new OceanBaseEnumeratorState(
            new ArrayList<>(inFlightSplits.values()),  // 进行中的分片
            new ArrayList<>(pendingSplits),             // 待分配的分片
            splitDiscoveryFinished);                    // 分片发现是否完成
}
```

当某个 Reader 失败时，其持有的分片通过 `addSplitsBack()` 回退到待分配队列，重新分配给空闲 Reader 重新读取：

```java
public void addSplitsBack(List<OceanBaseSplit> splits, int subtaskId) {
    for (OceanBaseSplit split : splits) {
        pendingSplits.addLast(split);  // 重新入队
    }
    dedupePendingSplits();  // 去重防止重复
    assignPendingSplits();  // 重新分配给空闲 Reader
}
```

## 六、快速上手

使用 Flink SQL 可以快速创建一个并行快照读取作业：

```sql
CREATE TABLE ob_source (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'oceanbase',
    'url' = 'jdbc:oceanbase://127.0.0.1:2881/test_db',
    'schema-name' = 'test_db',
    'table-name' = 'orders',
    'username' = 'root@mysql_tenant',
    'password' = '******',
    'compatible-mode' = 'MySQL',
    'split-size' = '10000',               -- 每个分片的行数
    'fetch-size' = '1024'                 -- JDBC 每批拉取的行数
);

-- 并行读取到另一个系统
INSERT INTO target_table SELECT * FROM ob_source;
```

关键参数说明：

|           参数            | 默认值  |                     说明                     |
|-------------------------|------|----------------------------------------------|
| `split-size`            | 8192 | 每个分片包含的目标行数。值越小分片越多，并行度越高                  |
| `chunk-key-column`      | 自动检测 | 手动指定切片键列名，覆盖自动检测（MySQL 用主键，Oracle 用 ROWID） |
| `fetch-size`            | 1024 | JDBC ResultSet 每次从数据库拉取的行数                 |

## 总结

OceanBase Flink 连接器的并行快照读取功能在设计上追求三个目标：

1. **低延迟启动**：异步分片机制让 Reader 在分片计算完成之前就能开始工作，消除了用户的等待时间。对于字符串主键大表，这一设计尤为重要。

2. **Exactly-Once 语义**：分片边界互不重叠保证每行数据只属于一个分片；流模式下利用 Flink checkpoint barrier 的注入时机，将每个分片的读取与发送封装在单次 `pollNext()` 调用中，实现分片级原子处理，Source 级别即达到 Exactly-Once。

3. **兼容性**：MySQL 模式基于主键、Oracle 模式基于 ROWID 的差异化切片策略，配合标准 JDBC API 的主键发现机制，确保了对 OceanBase 双模式的完整支持。

该功能已在 [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) 项目中开源，欢迎试用和反馈。
