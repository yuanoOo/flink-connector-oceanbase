# OceanBase Sink：文件完成 Kafka 通知

在 Flink SQL 里建 `connector = 'oceanbase'` 的 sink 表时，可比目标表**多两列**：一列 `BOOLEAN` 表示本行是否为「文件完成」行，一列 `CHAR`/`VARCHAR` 表示要发到 Kafka 的**消息正文**。这两列**不写进 OceanBase**，只参与控制与通知。

当标志列为 `true` 时：先把该行按普通业务行写入 OB，并在此之前把 sink 里已缓冲的数据刷到 OB；**OB 刷写成功后**，再向配置的 Kafka topic 发一条记录，**value** 为消息列内容（消息列为 null 时发空字符串）。发送采用同步等待完成；Kafka 失败会导致该行写入失败并向上抛错。

标志列为 `false` 或 null 时，不发 Kafka，行为与普通 sink 行一致。

---

## 何时启用

须同时配置（选项值去掉首尾空白后非空）：

|                          选项                          |                 含义                  |
|------------------------------------------------------|-------------------------------------|
| `file-completion.flag-column`                        | 标志列在 DDL 中的**物理列名**（大小写须与 DDL 完全一致） |
| `file-completion.message-column`                     | 消息列物理列名（同上）                         |
| `file-completion.kafka.topic`                        | 通知写入的 topic                         |
| `file-completion.kafka.properties.bootstrap.servers` | Kafka 地址                            |

列类型要求：标志列为 **BOOLEAN**，消息列为 **CHAR** 或 **VARCHAR**。标志列与消息列**不能同名**（忽略大小写比较）。

**未启用**：上述三个「核心」选项都未配置，且没有任何 `file-completion.kafka.properties.*` 带非空值时，不启用本功能。

**半配置会报错**：只要配置了上述任一项，或配置了任意 `file-completion.kafka.properties.*`，就必须把本节表格里的四项全部配齐，否则建表/校验阶段会失败。

其它 Kafka 客户端参数写在 `file-completion.kafka.properties.` 前缀下，前缀去掉后的键名与官方 Producer 配置一致（如 `security.protocol`、`sasl.mechanism` 等）。未指定时，key/value 序列化器默认为字符串类型。

每条通知：Kafka **record key 为 null**，由客户端默认策略选分区。

---

## DDL 与主键

业务列 + 标志列 + 消息列一起写在 sink 的 `CREATE TABLE` 里；**主键不能只由标志列和消息列组成**（须至少包含一条业务主键列）。

---

## 示例

```sql
CREATE TABLE ob_sink (
  id INT,
  name STRING,
  amount DECIMAL(10, 2),
  is_eof BOOLEAN,
  kafka_msg STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'oceanbase',
  'url' = 'jdbc:mysql://host:port/your_schema?useUnicode=true&characterEncoding=UTF-8&useSSL=false',
  'username' = 'user',
  'password' = 'password',
  'schema-name' = 'your_schema',
  'table-name' = 'your_table',
  'sync-write' = 'true',
  'file-completion.flag-column' = 'is_eof',
  'file-completion.message-column' = 'kafka_msg',
  'file-completion.kafka.topic' = 'your-completion-topic',
  'file-completion.kafka.properties.bootstrap.servers' = 'kafka:9092'
);
```

---

## 依赖

Connector 制品需带上 `kafka-clients`（与当前 connector 构建版本一致）。与 Flink 一起部署时注意 classpath 中日志 API 不要冲突。

---

## 背景与边界

并行度、shuffle 等前提见同目录 [ob-sink-file-completion-kafka-requirements.md](./ob-sink-file-completion-kafka-requirements.md)。

当前实现**不**支持：自定义 Kafka record key、固定分区、与 checkpoint 的事务对齐。仅覆盖 Flink SQL / Table API 下 `oceanbase` sink 这一条路径。
