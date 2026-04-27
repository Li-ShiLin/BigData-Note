本仓库汇总 Flink Java 学习路径中的环境搭建、基础教程、模块化示例代码与相关资料文档。

---

## 环境部署

| 文档 | 核心内容 |
| --- | --- |
| [centos.md](centos.md) | VirtualBox + Vagrant 搭建 CentOS7 多节点虚拟机环境，含网络、免密登录与常用运维命令 |
| [deploy.md](deploy.md) | Flink 集群部署实战：Standalone、YARN 等部署维度与 Session/Per-Job/Application 运行模式 |

---

## 概念与教程

| 文档 | 核心内容 |
| --- | --- |
| [tutorial.md](tutorial.md) | Flink 全链路教程：基础概念、系统架构、DataStream API、时间与窗口、状态、容错、Table/SQL、CEP |
| [flink-00-flinktutorial/README.md](flink-00-flinktutorial/README.md) | Chapter07-08 专题导读：ProcessFunction、定时器、TopN 以及多流转换案例串讲 |

---

## 示例模块

| 文档 | 核心内容 |
| --- | --- |
| [flink-01-quickstart/README.md](flink-01-quickstart/README.md) | 快速入门：WordCount 的批处理、有界流、无界流三种实现与差异对比 |
| [flink-02-DataSource/README.md](flink-02-DataSource/README.md) | 数据源读取：文件、集合、Socket、CSV、HTTP、JDBC、Kafka 与自定义 Source |
| [flink-03-Transformation/README.md](flink-03-Transformation/README.md) | 转换算子：Map/Filter/FlatMap、KeyBy、Reduce、UDF、RichFunction 与物理分区 |
| [flink-04-Sink/README.md](flink-04-Sink/README.md) | 数据输出：File、Kafka、Redis、Elasticsearch、JDBC 及自定义 Sink（HBase） |
| [flink-05-watermark/README.md](flink-05-watermark/README.md) | 时间语义与 Watermark：内置/自定义生成器、水位线传递与乱序处理 |
| [flink-06-Window/README.md](flink-06-Window/README.md) | 窗口计算：窗口分配器、增量聚合与全窗口函数、迟到数据处理 |
| [flink-07-ProcessFunction/README.md](flink-07-ProcessFunction/README.md) | ProcessFunction 进阶：定时器、侧输出流、KeyedProcessFunction 与 TopN 场景 |
| [flink-08-Multi-Stream-Transformations/README.md](flink-08-Multi-Stream-Transformations/README.md) | 多流转换：分流、Union/Connect、CoProcessFunction、Window Join/Interval Join/CoGroup |
| [flink-09-State/README.md](flink-09-State/README.md) | 状态编程：Keyed State、Operator State、Broadcast State 与 State TTL |
| [flink-10-FaultTolerance/README.md](flink-10-FaultTolerance/README.md) | 容错机制：Checkpoint、Savepoint、状态一致性与端到端 Exactly-Once |
| [flink-11-TableAPI-SQL/README.md](flink-11-TableAPI-SQL/README.md) | Table API & SQL：表流转换、窗口聚合、TopN、Join 与 UDF |
| [flink-12-FlinkCEP/README.md](flink-12-FlinkCEP/README.md) | CEP 复杂事件处理：Pattern API、登录失败检测、订单超时检测与 NFA 示例 |

---

## 扩展资料

| 文档 | 核心内容 |
| --- | --- |
| [extra.md](extra.md) | Flink 补充学习资料：术语、关键机制、实践经验与延伸阅读索引 |

---

## 官方文档资料

| 文档 | 核心内容 |
| --- | --- |
| 快速入门 | 1.DataStream 入门、开发流程与基础示例 [Flink 官方文档 - 快速开始](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/overview/)  2.批流统一理念、处理模型与核心概念 [Flink 官方文档 - 批处理与流处理](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/concepts/overview/)  3.DataStream API 总览与常用算子入口 [Flink 官方文档 - DataStream API](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/overview/)  4.执行环境创建、配置与程序提交流程 [Flink 官方文档 - 执行环境](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/execution_environment/) |
| 数据源（Source） | 1.Source API 总览与常见数据接入方式 [Flink 官方文档 - 数据源（Source）](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/)  2.文件、集合、Socket 等内置 Source 用法 [Flink 官方文档 - 内置数据源](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#内置数据源)  3.Kafka Source 配置、消费语义与起始位点 [Flink 官方文档 - Kafka 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/kafka/)  4.自定义 Source 生命周期与并行实现方式 [Flink 官方文档 - 自定义数据源](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#自定义数据源)  5.SourceFunction / RichSourceFunction 说明 [Flink 官方文档 - 数据源函数](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#数据源函数) |
| 转换算子（Transformation） | 1.转换算子体系与分类总览 [Flink 官方文档 - 转换算子](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/)  2.map/filter/flatMap 等基础算子说明 [Flink 官方文档 - 基本转换算子](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#基本转换算子)  3.KeyBy 分区语义与按键聚合机制 [Flink 官方文档 - KeyBy 和分组](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#keyby-和分组)  4.shuffle/rebalance/rescale 等分区策略 [Flink 官方文档 - 物理分区](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#物理分区) |
| 数据输出（Sink） | 1.Sink API 总览与输出语义 [Flink 官方文档 - 数据输出（Sink）](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sinks/)  2.FileSink 滚动策略、分桶与提交机制 [Flink 官方文档 - 文件 Sink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/filesystem/)  3.Elasticsearch Sink 写入配置与失败处理 [Flink 官方文档 - Elasticsearch 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/elasticsearch/)  4.JDBC Sink 批量写入与幂等实践 [Flink 官方文档 - JDBC 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/jdbc/)  5.自定义 SinkFunction / RichSinkFunction 实现 [Flink 官方文档 - 自定义 Sink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sinks/#自定义-sink) |
| 时间语义与水位线（Watermark） | 1.处理时间、事件时间与时间属性定义 [Flink 官方文档 - 时间属性](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/concepts/time_attributes/)  2.Watermark 生成策略与乱序事件处理 [Flink 官方文档 - 水位线](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/event-time/generating_watermarks/) |
| 窗口（Window） | 1.滚动/滑动/会话窗口与触发机制 [Flink 官方文档 - 窗口](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/)  2.Reduce/Aggregate/ProcessWindowFunction 用法 [Flink 官方文档 - 窗口函数](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/#窗口函数window-functions) |
| 处理函数（ProcessFunction） | 1.ProcessFunction 家族、状态与侧输出流 [Flink 官方文档 - 处理函数](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/process_function/)  2.ProcessingTime / EventTime 定时器机制 [Flink 官方文档 - 定时器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/process_function/#timers) |
| 多流转换 | 1.Connect、CoMap、CoProcess 等多流算子入口 [Flink 官方文档 - 多流转换](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/)  2.基于窗口边界的双流 Join 语义 [Flink 官方文档 - 窗口 Join](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/joining/#窗口-join)  3.基于时间区间的 Interval Join 用法 [Flink 官方文档 - 区间 Join](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/joining/#区间-joininterval-join) |
| 状态编程（State） | 1.Keyed State、Operator State 与状态 API [Flink 官方文档 - 状态编程](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state/)  2.HashMapStateBackend / RocksDBStateBackend 配置 [Flink 官方文档 - 状态后端](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/state/state_backends/) |
| 容错机制（Fault Tolerance） | 1.Checkpoint 原理、配置项与恢复流程 [Flink 官方文档 - 检查点](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/checkpointing/)  2.Savepoint 生命周期与版本迁移实践 [Flink 官方文档 - 保存点](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/state/savepoints/)  3.at-least-once / exactly-once 语义保证 [Flink 官方文档 - 容错保证](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/)  4.State TTL 迁移兼容性与升级注意事项 [Flink 官方文档 - State TTL Migration Compatibility](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state_migration/) |
| Table API 和 SQL | 1.动态表、声明式查询与流批统一能力 [Flink 官方文档 - Table API 和 SQL](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/overview/)  2.TUMBLE/HOP/CUMULATE 等 TVF 窗口语法 [Flink 官方文档 - 窗口表值函数](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/sql/queries/window-tvf/) |
| CEP 复杂事件处理 | 1.CEP 基础概念、模式匹配与输出处理 [Flink 官方文档 - CEP](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/libs/cep/)  2.Pattern 定义、组合条件与时间约束 [Flink 官方文档 - Pattern API](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/libs/cep/#the-pattern-api) |

