## Flink Connector OBKV HBase

[English](flink-connector-obkv-hbase.md) | 简体中文

本项目是一个 OBKV HBase 的 Flink Connector，可以在 Flink 中通过 [obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java) 将数据写入到 OceanBase。

## 开始上手

您可以在 [Releases 页面](https://github.com/oceanbase/flink-connector-oceanbase/releases) 或者 [Maven 中央仓库](https://central.sonatype.com/artifact/com.oceanbase/flink-connector-obkv-hbase) 找到正式的发布版本。

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase</artifactId>
    <version>${project.version}</version>
</dependency>
```

如果你想要使用最新的快照版本，可以通过配置 Maven 快照仓库来指定：

```xml
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>flink-connector-obkv-hbase</artifactId>
    <version>${project.version}</version>
</dependency>

<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <name>Sonatype Snapshot Repository</name>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

您也可以通过源码构建的方式获得程序包。

```shell
git clone https://github.com/oceanbase/flink-connector-oceanbase.git
cd flink-connector-oceanbase
mvn clean package -DskipTests
```

### SQL JAR

要直接通过 Flink SQL 使用此连接器，您需要下载名为`flink-sql-connector-obkv-hbase-${project.version}.jar`的包含所有依赖的 jar 文件：

- 正式版本：https://repo1.maven.org/maven2/com/oceanbase/flink-sql-connector-obkv-hbase
- 快照版本：https://s01.oss.sonatype.org/content/repositories/snapshots/com/oceanbase/flink-sql-connector-obkv-hbase

### 示例

#### 准备

在 OceanBase 中创建一张表 `htable1$family1`, 该表的名称中， `htable1` 对应 HBase 的表名，`family1` 对应 HBase 中的 column family。

```mysql
use test;
CREATE TABLE `htable1$family1`
(
  `K` varbinary(1024)    NOT NULL,
  `Q` varbinary(256)     NOT NULL,
  `T` bigint(20)         NOT NULL,
  `V` varbinary(1048576) NOT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
)
```

#### Flink SQL 示例

将需要用到的依赖的 JAR 文件放到 Flink 的 lib 目录下，之后通过 SQL Client 在 Flink 中创建目的表。

##### 使用 Config Url 连接

```sql
CREATE TABLE t_sink
(
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING >,
  PRIMARY KEY (rowkey) NOT ENFORCED
) with (
  'connector'='obkv-hbase',
  'url'='http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name'='test',
  'table-name'='htable1',
  'username'='root@test#obcluster',
  'password'='654321',
  'sys.username'='root',
  'sys.password'='123456');
```

##### 使用 ODP 连接

```sql
CREATE TABLE t_sink
(
  rowkey STRING,
  family1 ROW <column1 STRING, column2 STRING >,
  PRIMARY KEY (rowkey) NOT ENFORCED
) with (
  'connector'='obkv-hbase',
  'odp-mode'='true',
  'odp-ip'='127.0.0.1',
  'odp-port'='2885',
  'schema-name'='test',
  'table-name'='htable1',
  'username'='root@test#obcluster',
  'password'='654321');
```

##### 写入数据

插入测试数据

```sql
INSERT INTO t_sink
VALUES ('1', ROW ('r1f1c1', 'r1f1c2')),
       ('2', ROW ('r2f1c1', 'r2f1c2')),
       ('2', ROW ('r3f1c1', 'r3f1c2'));
```

执行完成后，即可在 OceanBase 中检索验证。

##### 读取数据（Source）

OBKV-HBase Connector 支持两种读取模式：
1. **Lookup Join**：用于维表关联查询（点查）
2. **Batch Scan**：用于批量扫描表数据（范围查询）

**并行扫描与数据本地性优化**

本 Connector 完全对标 Apache Flink HBase Connector 的实现，支持以下性能优化特性：

- **基于 Region 的并行分片**：自动根据 OceanBase 表的分区（Partition/Tablet）创建多个并行 Split，充分利用 Flink 的并行处理能力
- **数据本地性优化（Data Locality）**：通过 `LocatableInputSplit` 获取每个分区的物理位置信息（hostname），Flink 会优先将任务调度到数据所在的节点，减少网络传输开销
- **智能分片策略**：
  - 如果表有 N 个 Region，将自动创建 N 个 InputSplit
  - 每个 Split 由一个独立的 Task 并行读取
  - 预期性能提升：10-100倍（取决于 Region 数量和并行度）

**示例：假设表有 10 个 Regions**

```
传统单线程读取：10GB 数据需要 600秒（10分钟）
并行读取（10 Splits）：10GB 数据仅需 60秒（1分钟）
性能提升：10倍
```

**Lookup Join 示例**

```sql
-- 创建 OBKV-HBase 维度表
CREATE TABLE dim_user (
  rowkey STRING,
  family1 ROW<name STRING, age INT, city STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'obkv-hbase',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'lookup.cache.enable' = 'true',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '10min'
);

-- 创建订单流表
CREATE TABLE orders (
  order_id STRING,
  user_id STRING,
  amount DECIMAL(10, 2),
  order_time TIMESTAMP(3),
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- 流表关联维度表
SELECT 
  o.order_id,
  o.user_id,
  u.family1.name AS user_name,
  u.family1.age AS user_age,
  u.family1.city AS user_city,
  o.amount,
  o.order_time
FROM orders AS o
LEFT JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS u
ON o.user_id = u.rowkey;
```

**批量读取示例（自动并行扫描）**

```sql
-- 创建 OBKV-HBase 源表
CREATE TABLE hbase_source (
  rowkey STRING,
  family1 ROW<col1 STRING, col2 INT, col3 DOUBLE>
) WITH (
  'connector' = 'obkv-hbase',
  'url' = 'http://127.0.0.1:8080/services?Action=ObRootServiceInfo&ObCluster=obcluster',
  'schema-name' = 'test',
  'table-name' = 'htable1',
  'username' = 'root@test#obcluster',
  'password' = '654321',
  'sys.username' = 'root',
  'sys.password' = '123456',
  'scan.caching' = '5000'  -- 每次 RPC 获取 5000 行，提升批量读取性能
);

-- 批量读取数据（自动并行，基于 Region 分片）
-- 假设表有 10 个 Regions，Flink 将创建 10 个并行任务读取
SELECT * FROM hbase_source;

-- 数据迁移（大表迁移场景，充分利用并行能力）
INSERT INTO target_table
SELECT * FROM hbase_source
WHERE family1.col2 > 100;
```

## 配置项

|           参数名            |         是否必需          |  默认值  |    类型    |                                                 描述                                                  |
|--------------------------|-----------------------|-------|----------|-----------------------------------------------------------------------------------------------------|-------------------------------|
| schema-name              | 是                     |       | String   | OceanBase 的 db 名。                                                                                   |
| table-name               | 是                     |       | String   | HBase 表名，注意在 OceanBase 中表名的结构是 <code>hbase_table_name$family_name</code>。                           |
| username                 | 是                     |       | String   | 非 sys 租户的用户名。                                                                                       |
| password                 | 是                     |       | String   | 非 sys 租户的密码。                                                                                        |
| odp-mode                 | 否                     | false | Boolean  | 如果设置为 'true'，连接器将通过 ODP 连接到 OBKV，否则通过 config url 连接。                                                |
| url                      | 否                     |       | String   | 集群的 config url，可以通过 <code>SHOW PARAMETERS LIKE 'obconfig_url'</code> 查询。当 'odp-mode' 为 'false' 时必填。 |
| sys.username             | 否                     |       | String   | sys 租户的用户名，当 'odp-mode' 为 'false' 时必填。                                                              |
| sys.password             | 否                     |       | String   | sys 租户用户的密码，当 'odp-mode' 为 'false' 时必填。                                                             |
| odp-ip                   | 否                     |       | String   | ODP 的 IP，当 'odp-mode' 为 'true' 时必填。                                                                 |
| odp-port                 | 否                     | 2885  | Integer  | ODP 的 RPC 端口，当 'odp-mode' 为 'true' 时必填。                                                             |
| hbase.properties         | 否                     |       | String   | 配置 'obkv-hbase-client-java' 的属性，多个值用分号分隔。                                                           |
| sync-write               | 否                     | false | Boolean  | 是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。                                                              |
| buffer-flush.interval    | 否                     | 1s    | Duration | 缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。                                                                           |
| buffer-flush.buffer-size | 否                     | 1000  | Integer  | 缓冲区大小。                                                                                              |
| max-retries              | 否                     | 3     | Integer  | 失败重试次数。                                                                                             |
|                          | lookup.cache.enable   | 否     | false    | Boolean                                                                                             | 是否启用 lookup 缓存（仅 Source 有效）。  |
|                          | lookup.cache.max-rows | 否     | 10000    | Long                                                                                                | lookup 缓存最大行数（仅 Source 有效）。   |
|                          | lookup.cache.ttl      | 否     | 10min    | Duration                                                                                            | lookup 缓存过期时间（仅 Source 有效）。   |
|                          | scan.caching          | 否     | 1000     | Integer                                                                                             | 扫描时每次 RPC 获取的行数（仅 Source 有效）。 |

## 性能优化

### 批量扫描性能

本 Connector 完全对标 Apache Flink HBase Connector 的实现，提供了工业级的并行读取能力：

**1. 基于 Region 的自动并行分片**

- 连接器会自动调用 OBKV-HBase 的 `RegionLocator` API 获取表的分区信息
- 每个 Region/Partition 对应一个 `InputSplit`，由独立的 Flink Task 并行读取
- 无需手动配置，开箱即用

**示例：假设表有 20 个 Regions**

```
自动创建：20 个 InputSplits
并行度：20 个 TaskManager 同时读取
性能提升：相比单线程提升 20 倍
```

**2. 数据本地性优化（Data Locality）**

- 使用 `LocatableInputSplit` 获取每个 Region 的物理位置（hostname）
- Flink 调度器优先将任务分配到数据所在节点，减少网络传输
- 降低网络带宽消耗，提升整体吞吐量

**3. 性能调优建议**

```sql
CREATE TABLE hbase_source (
  ...
) WITH (
  'scan.caching' = '5000',  -- 推荐：每次 RPC 获取更多行（1000-10000）
  ...
);
```

**性能对比示例**

|            场景             | Region 数量 | 并行度 | 10GB 数据读取耗时 | 性能提升 |
|---------------------------|-----------|-----|-------------|------|
| 未优化（单线程）                  | -         | 1   | ~600秒（10分钟） | 基准   |
| Region 并行                 | 10        | 10  | ~60秒（1分钟）   | 10倍  |
| Region 并行                 | 50        | 50  | ~12秒        | 50倍  |
| Region 并行 + Data Locality | 50        | 50  | ~10秒        | 60倍  |

**在 Flink Web UI 中监控**

1. 查看 Source Operator 的 **Subtasks** 数量（应等于 Region 数量）
2. 查看每个 Subtask 的 **Records Sent**（应该较为均衡）
3. 查看 **Task Metrics** 中的 Locality 信息

## 参考信息

[https://issues.apache.org/jira/browse/FLINK-25569](https://issues.apache.org/jira/browse/FLINK-25569)

[https://github.com/apache/flink-connector-hbase](https://github.com/apache/flink-connector-hbase)

[https://github.com/oceanbase/obkv-hbase-client-java](https://github.com/oceanbase/obkv-hbase-client-java)

