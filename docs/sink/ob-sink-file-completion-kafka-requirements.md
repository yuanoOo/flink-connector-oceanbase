# OB Sink 文件完成 Kafka 通知需求

## 背景

客户使用 Flink 从 OSS 文件读取数据，并写入 OceanBase。上游 OSS source 会在输出 RowData 中额外携带两列：

- 文件读取完成标志列。
- 发送到外部系统的消息列。

这两列只用于控制通知逻辑，不属于 OceanBase 目标表字段。

## 目标

当 Flink 读取完某个 OSS 文件，并且该文件对应的数据已经成功写入 OceanBase 后，OceanBase sink 需要向 Kafka 发送一条消息。

## 输入约定

- OSS source 每行输出的 RowData 比 OceanBase 目标表多两列。
- 标志列用于标识该行是否为当前文件的最后一行。
- 消息列用于提供发送到 Kafka 的消息内容。
- 最后一行仍然是业务数据，需要先写入 OceanBase。
- 标志列和消息列不写入 OceanBase。

## 并行度约束

本需求基于以下运行前提：

- 每个 OSS source 并行度只读取一个文件。
- 多个 source 并行度分别读取不同文件。
- SQL 只是 OSS source 直接写入 OceanBase sink，中间没有 shuffle、聚合、join、rebalance 等会打乱数据分布的操作。
- 上下游并行度一致。

在上述前提下，同一个文件的数据和完成标志会进入同一个 sink subtask。sink 收到完成标志行后，只需要保证当前 subtask 内该文件已到达的数据 flush 成功，再发送 Kafka 消息。

## 配置需求

OceanBase sink 需要新增配置项：

- `file-completion.flag-column`：完成标志列名。
- `file-completion.message-column`：Kafka 消息列名。
- `file-completion.kafka.topic`：Kafka topic。
- `file-completion.kafka.properties.bootstrap.servers`：Kafka bootstrap servers。
- `file-completion.kafka.properties.security.protocol`：Kafka security protocol，例如 `SASL_PLAINTEXT`。
- `file-completion.kafka.properties.sasl.mechanism`：Kafka SASL mechanism。
- `file-completion.kafka.properties.sasl.jaas.config`：Kafka JAAS config。
- `file-completion.kafka.properties.group.id`：按客户配置透传；Kafka producer 本身不依赖该配置。

列名必须严格大小写匹配。若用户配置的标志列或消息列在 sink 表 schema 中不存在，应直接报错。

## 处理流程

1. OceanBase sink 根据配置识别 RowData 中的标志列和消息列。
2. 写入 OceanBase 前，从目标写入字段中排除标志列和消息列。
3. 普通行按现有逻辑写入 OceanBase。
4. 当标志列表示文件完成时：
   - 先写入该行的业务字段。
   - 再 flush 当前 writer 中已缓冲的数据。
   - flush 成功后，将消息列内容发送到 Kafka。
5. 如果 OceanBase 写入或 flush 失败，不发送 Kafka 消息。
6. 如果 Kafka 发送失败，应让 sink 写入失败并向 Flink 抛出异常。

## 一致性语义

- 本特性保证 **at-least-once**：OceanBase 写入和 Kafka 通知都是各自独立提交，与 Flink checkpoint 之间没有事务对齐。
- 当 task 在「OB flush 成功 + Kafka 通知成功」之后、但 checkpoint barrier 到达之前 crash 时，Flink 恢复后会从上一个 checkpoint 的 source offset 重读，导致**同一文件可能再次产生 completion 行，Kafka 上对应的消息会重复**。下游消费方需自行幂等去重（按 message 内容或自定义 key）。
- 如果 OB flush 失败或 Kafka send 失败，sink 抛异常让 Flink 重启，由 source 重读 + 重发保证不丢。

## 非目标

- 不修改 OSS source。
- 不支持跨 sink subtask 的全局协调。
- 不保证存在 shuffle、聚合、join、rebalance 等操作后的文件完成语义。
- 不修改 directload sink。
- 不修改 `flink-connector-oceanbase-cli` 模块。
- 不保证 Kafka 通知与 Flink checkpoint 之间的 exactly-once。

