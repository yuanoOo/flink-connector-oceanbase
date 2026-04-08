# 并行切片 + 断点续读：OceanBase Flink 连接器快照读取设计解析

在大数据生态中，OceanBase 作为分布式关系型数据库，承载着海量业务数据。Apache Flink 作为主流的流批一体处理引擎，是数据集成链路中的关键枢纽。当我们需要将 OceanBase 中的全量数据高效导入 Flink 进行离线分析或批量处理时，单线程的 `SELECT *` 显然无法满足大表场景下的性能需求。本文将深入解析 OceanBase Flink 连接器中**并行快照读取**功能的设计原理，重点介绍异步分片、差异化切片策略和 Exactly-Once 语义三大核心设计。

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
│  - 异步发现分片           │ ──────────► │  - JDBC 游标逐行读取      │
│  - 计算切分点             │              │  - 类型转换 → RowData     │
│  - 分配分片给 Reader      │   request   │  - 追踪 lastReadValue    │
│  - 管理 checkpoint 状态   │ ◄────────── │  - checkpoint 快照       │
└──────────────────────────┘              └──────────────────────────┘
```

一个分片的完整生命周期为：**发现 → 分配 → 读取 → 完成 → 请求下一个分片**。当所有分片都被读取完毕，Source 向 Flink 报告 `END_OF_INPUT`，整个快照读取结束。

## 三、异步分片：边算边读的流式设计

### 3.1 痛点：同步分片与深分页问题

最直觉的分片实现是：在 `SplitEnumerator.start()` 中**同步**计算所有切分点，然后一次性分配给 Reader。这对于数值型主键（如 `BIGINT`）没有问题——切分点可以通过简单的算术运算（`(max - min) / numSplits`）瞬间算出。

但对于**字符串主键**（如 `VARCHAR`）的大表，情况完全不同。字符串无法做数学等分，每个切分点都需要执行一条 SQL 查询。如果使用朴素的 `LIMIT/OFFSET` 方式（如 `SELECT name FROM users ORDER BY name LIMIT 1 OFFSET 250000`），会遇到 MySQL 经典的**深分页问题**：MySQL 必须扫描前 25 万行再丢弃，只返回第 25 万零一行。越往后的分片，OFFSET 越大，扫描代价呈线性增长。

OceanBase Flink 连接器参考 Flink CDC 的设计，采用了**逐段推进（next-chunk-max）**策略来规避这一问题：

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

每次查询从上一个切分边界开始，只向前扫描 `chunkSize` 行（即 `scan.split-size` 配置的值），取其最大值作为下一个边界。**无论表有多大、当前在第几个分片，每次查询的代价始终是 O(chunkSize)**。同时，如果遇到大量重复值导致边界相同，还会通过 `SELECT MIN(col) FROM table WHERE col > ?` 跳过重复区间，确保每个分片都有有效的数据范围。

但即便每次查询很快，对于 1000 万行、按 10000 行一个分片切分的表，仍需约 1000 次迭代查询。同步等待所有查询完成后 Reader 才能开始读数据，用户依然面临启动等待。

### 3.2 方案：后台线程 + 增量分配

借鉴 Flink CDC 的异步分片设计思路，我们将分片发现改为**后台线程异步执行**：

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

| 数据类型 | 切分方式 | 是否需要查库 | 示例 |
|---------|---------|------------|------|
| `BIGINT` / `INT` | 整数等分 | 否 | `min=0, max=1000, 4片 → [0, 250, 500, 750]` |
| `DECIMAL` | BigDecimal 精确等分 | 否 | `min=0.00, max=10.00, 4片 → [2.50, 5.00, 7.50]` |
| `DOUBLE` / `FLOAT` | 浮点等分 | 否 | `min=0.0, max=100.0, 4片 → [25.0, 50.0, 75.0]` |
| `VARCHAR` / `DATE` / 其他 | 逐段推进（next-chunk-max） | 是，每个切分点一次 | 从上一个边界取 chunkSize 行的 MAX 值作为下一个边界 |

对于需要查库的非数值类型，采用逐段推进策略（避免深分页），每得到一个切分点就立即生成分片——这就是前文所述的异步增量分片机制发挥作用的地方。

## 五、Exactly-Once 语义的精确实现

对于批处理场景，Exactly-Once 意味着**每一行数据恰好被读取一次**——不重复、不遗漏。OceanBase Flink 连接器通过以下机制实现了这一语义。

### 5.1 渐进式 Checkpoint：lastReadValue

每个分片（`OceanBaseSplit`）维护一个 `lastReadValue` 字段，记录当前已读到的最后一个切片键值：

```java
// 每读取一行数据，更新进度标记
if (currentResultSet.next()) {
    RowData row = convertToRowData(currentResultSet);
    output.collect(row);
    if (splitColumnIndex > 0) {
        currentSplit.setLastReadValue(
            currentResultSet.getObject(splitColumnIndex));
    }
}
```

当 Flink 触发 checkpoint 时，Reader 将当前分片（含 `lastReadValue`）和待处理分片一起快照到状态后端。

### 5.2 精确恢复：`>` 与 `>=` 的选择

故障恢复的关键在于：**从 checkpoint 恢复时，如何避免重复读取已经处理过的行？**

答案在于查询条件中运算符的精确选择：

```java
// 初始读取：使用 >= 包含起始边界
// 从 checkpoint 恢复：使用 > 排除已读过的最后一行
Object effectiveStart = split.getLastReadValue() != null
        ? split.getLastReadValue()
        : split.getSplitStart();
String startOp = split.getLastReadValue() != null ? " > ?" : " >= ?";
```

举例说明（分片范围 `[50, 100)`）：

| 场景 | 运算符 | 生成的 SQL | 原因 |
|------|--------|-----------|------|
| 首次读取 | `>=` | `WHERE id >= 50 AND id < 100` | 包含起始边界值 50 |
| 已读到 75，checkpoint 后恢复 | `>` | `WHERE id > 75 AND id < 100` | 排除已读的 75，从 76 继续 |

这个设计确保了：
- **不遗漏**：首次读取使用 `>=`，不会跳过边界行。
- **不重复**：恢复读取使用 `>`，严格排除已处理的最后一行。

### 5.3 分片状态的双保险

Enumerator 的 checkpoint 状态同时保存**待分配分片**（pending）和**已分配但未完成的分片**（in-flight）：

```java
public OceanBaseEnumeratorState snapshotState(long checkpointId) {
    return new OceanBaseEnumeratorState(
            new ArrayList<>(inFlightSplits.values()),  // 进行中的分片（含 lastReadValue）
            new ArrayList<>(pendingSplits),             // 待分配的分片
            splitDiscoveryFinished);                    // 分片发现是否完成
}
```

当某个 Reader 失败时，其持有的分片通过 `addSplitsBack()` 回退到待分配队列，**带着 `lastReadValue` 一起回退**。重新分配后，新 Reader 从上次的进度继续读取，而不是从头重来：

```java
public void addSplitsBack(List<OceanBaseSplit> splits, int subtaskId) {
    for (OceanBaseSplit split : splits) {
        pendingSplits.addLast(split);  // 带着 lastReadValue 重新入队
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
    'scan.split-size' = '10000',          -- 每个分片的行数
    'scan.fetch-size' = '1024'            -- JDBC 每批拉取的行数
);

-- 并行读取到另一个系统
INSERT INTO target_table SELECT * FROM ob_source;
```

关键参数说明：

| 参数 | 默认值 | 说明 |
|------|-------|------|
| `scan.split-size` | 8192 | 每个分片包含的目标行数。值越小分片越多，并行度越高 |
| `scan.chunk-key-column` | 自动检测 | 手动指定切片键列名，覆盖自动检测（MySQL 用主键，Oracle 用 ROWID） |
| `scan.fetch-size` | 1024 | JDBC ResultSet 每次从数据库拉取的行数 |

## 总结

OceanBase Flink 连接器的并行快照读取功能在设计上追求三个目标：

1. **低延迟启动**：异步分片机制让 Reader 在分片计算完成之前就能开始工作，消除了用户的等待时间。对于字符串主键大表，这一设计尤为重要。

2. **精确一致性**：通过 `lastReadValue` 渐进式 checkpoint 和 `>` / `>=` 运算符的精确选择，实现了 Exactly-Once 语义——即使在故障恢复后，每一行数据也只会被读取恰好一次。

3. **兼容性**：MySQL 模式基于主键、Oracle 模式基于 ROWID 的差异化切片策略，配合标准 JDBC API 的主键发现机制，确保了对 OceanBase 双模式的完整支持。

该功能已在 [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) 项目中开源，欢迎试用和反馈。
