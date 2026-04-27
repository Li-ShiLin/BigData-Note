本目录汇总 Kafka 核心教程、Spring Boot 集成示例、生产与消费进阶能力、以及集群部署实践文档。

---

## 核心教程与总览

| 文档 | 核心内容 |
|------|----------|
| [kafka-tutorial.md](kafka-tutorial.md) | Kafka 全景教程：核心概念、安装部署（ZK/KRaft/Docker）、命令操作、集群与 SpringBoot 集成实践总览 |

---

## 模块文档总览（kafka-00-common ～ kafka-15-cluster）

| 文档 | 核心内容 |
|------|----------|
| [kafka-00-common/README.md](kafka-00-common/README.md) | Kafka 示例公共基础模块：统一返回体、异常处理、工具类与通用常量 |
| [kafka-01-hello-world/README.md](kafka-01-hello-world/README.md) | Kafka 入门示例：原生 Producer/Consumer 基础发送与消费 |
| [kafka-02-springboot/README.md](kafka-02-springboot/README.md) | Spring Boot + Kafka 快速集成：Topic 创建、生产消费、手动提交 offset |
| [kafka-03-offset/README.md](kafka-03-offset/README.md) | offset 行为演示：`latest` / `earliest` / 手动提交与 Admin API 重置消费位点 |
| [kafka-04-offset/README.md](kafka-04-offset/README.md) | offset 策略专题：`earliest` / `latest` / `none` 三种重置策略与测试用例 |
| [kafka-05-KafkaTemplate-Send/README.md](kafka-05-KafkaTemplate-Send/README.md) | KafkaTemplate 发送能力全景：多种 `send` 重载、对象消息发送、统一响应与异常处理 |
| [kafka-05-KafkaTemplate-Send/Spring-Kafka-Producer-Partition-Strategy-Analysis.md](kafka-05-KafkaTemplate-Send/Spring-Kafka-Producer-Partition-Strategy-Analysis.md) | Spring Kafka 生产者分区策略源码分析：DefaultPartitioner、哈希与轮询逻辑 |
| [kafka-06-producer-partition-strategy/README.md](kafka-06-producer-partition-strategy/README.md) | 生产者分区实战：默认分区、自定义分区、按 Key 分区、轮询分区 |
| [kafka-07-Producer-Interceptors/README.md](kafka-07-Producer-Interceptors/README.md) | 生产者拦截器实战：消息改写、指标统计、拦截器链执行机制 |
| [kafka-08-Event-Consumer/README.md](kafka-08-Event-Consumer/README.md) | 消费者事件消费：普通消费、批量消费、手动提交与消费配置对比 |
| [kafka-09-object-message/README.md](kafka-09-object-message/README.md) | Kafka 对象消息：对象序列化/反序列化、多主题隔离与对象监听消费 |
| [kafka-10-consumer-topic-partition-offset/README.md](kafka-10-consumer-topic-partition-offset/README.md) | 精准消费控制：指定 Topic/Partition/Offset 消费与对应发送测试 |
| [kafka-11-batch-consumer/README.md](kafka-11-batch-consumer/README.md) | 批量消费专题：批量拉取、批量处理、手动确认与高吞吐场景演示 |
| [kafka-12-consumer-Interceptor/README.md](kafka-12-consumer-Interceptor/README.md) | 消费者拦截器实战：消息过滤、指标统计、消费链路监控 |
| [kafka-13-Consumer-SendTo/README.md](kafka-13-Consumer-SendTo/README.md) | 消费消息转发：`@SendTo` 自动转发、增强转发与条件转发 |
| [kafka-14-consumer-partition-assignor/README.md](kafka-14-consumer-partition-assignor/README.md) | 分区分配策略深度解析：Range、RoundRobin、Sticky、CooperativeSticky 对比 |
| [kafka-15-cluster/README.md](kafka-15-cluster/README.md) | Kafka 集群实战：多 Broker 集群、分区副本、并发消费与可靠性验证 |
