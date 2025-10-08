<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [生产者分区策略](#%E7%94%9F%E4%BA%A7%E8%80%85%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [分区策略说明](#%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E8%AF%B4%E6%98%8E)
    - [1. 默认分区策略 (DefaultPartitioner)](#1-%E9%BB%98%E8%AE%A4%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5-defaultpartitioner)
    - [2. 自定义分区策略 (CustomPartitioner)](#2-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5-custompartitioner)
    - [3. 基于 Key 的分区策略 (KeyBasedPartitioner)](#3-%E5%9F%BA%E4%BA%8E-key-%E7%9A%84%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5-keybasedpartitioner)
    - [4. 轮询分区策略 (RoundRobinPartitioner)](#4-%E8%BD%AE%E8%AF%A2%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5-roundrobinpartitioner)
  - [API 接口](#api-%E6%8E%A5%E5%8F%A3)
    - [1. 发送默认分区策略消息](#1-%E5%8F%91%E9%80%81%E9%BB%98%E8%AE%A4%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E6%B6%88%E6%81%AF)
    - [2. 发送自定义分区策略消息](#2-%E5%8F%91%E9%80%81%E8%87%AA%E5%AE%9A%E4%B9%89%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E6%B6%88%E6%81%AF)
    - [3. 发送基于 Key 分区策略消息](#3-%E5%8F%91%E9%80%81%E5%9F%BA%E4%BA%8E-key-%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E6%B6%88%E6%81%AF)
    - [4. 发送轮询分区策略消息](#4-%E5%8F%91%E9%80%81%E8%BD%AE%E8%AF%A2%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E6%B6%88%E6%81%AF)
    - [5. 批量发送消息演示](#5-%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E6%BC%94%E7%A4%BA)
    - [6. 获取分区策略说明](#6-%E8%8E%B7%E5%8F%96%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E8%AF%B4%E6%98%8E)
  - [运行说明](#%E8%BF%90%E8%A1%8C%E8%AF%B4%E6%98%8E)
  - [配置说明](#%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
  - [演示效果](#%E6%BC%94%E7%A4%BA%E6%95%88%E6%9E%9C)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 生产者分区策略

本项目演示了 Kafka 生产者发送消息时的不同分区策略。

## 项目结构

```
kafka-06-producer-partition-strategy/
├── src/main/java/com/action/kafka06producerpartitionstrategy/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka 配置类
│   ├── constants/
│   │   └── KafkaConstants.java                 # 常量定义
│   ├── consumer/
│   │   └── PartitionStrategyConsumer.java      # 消费者类
│   ├── controller/
│   │   └── PartitionStrategyController.java    # REST 控制器
│   ├── model/
│   │   └── Message.java                        # 消息模型
│   ├── partition/
│   │   ├── CustomPartitioner.java              # 自定义分区策略
│   │   ├── KeyBasedPartitioner.java            # 基于 Key 的分区策略
│   │   └── RoundRobinPartitioner.java          # 轮询分区策略
│   ├── service/
│   │   └── PartitionStrategyProducerService.java # 生产者服务
│   └── Kafka06ProducerPartitionStrategyApplication.java # 启动类
├── src/main/resources/
│   └── application.properties                  # 应用配置
└── pom.xml                                     # Maven 配置
```

## 分区策略说明

### 1. 默认分区策略 (DefaultPartitioner)
- 使用 Kafka 默认的轮询分区策略
- 当有 key 时，根据 key 的 hash 值选择分区
- 当没有 key 时，使用轮询方式分配分区

### 2. 自定义分区策略 (CustomPartitioner)
- 根据 key 的前缀进行分区：
  - `ORDER-*` → 分区 0
  - `USER-*` → 分区 1
  - `SYSTEM-*` → 分区 2
  - 其他 → 使用 hash 分区

### 3. 基于 Key 的分区策略 (KeyBasedPartitioner)
- 根据 key 的 hash 值进行分区
- 确保相同 key 的消息总是发送到同一个分区

### 4. 轮询分区策略 (RoundRobinPartitioner)
- 按照轮询方式分配消息到不同分区
- 当有 key 时，使用 key 的 hash 值
- 当没有 key 时，使用计数器轮询

## API 接口

### 1. 发送默认分区策略消息
```http
POST /api/partition-strategy/default
Content-Type: application/x-www-form-urlencoded

key=test-key&message=测试消息
```

### 2. 发送自定义分区策略消息
```http
POST /api/partition-strategy/custom
Content-Type: application/x-www-form-urlencoded

key=ORDER-001&message=订单消息
```

### 3. 发送基于 Key 分区策略消息
```http
POST /api/partition-strategy/key-based
Content-Type: application/x-www-form-urlencoded

key=user-123&message=用户消息
```

### 4. 发送轮询分区策略消息
```http
POST /api/partition-strategy/round-robin
Content-Type: application/x-www-form-urlencoded

key=&message=轮询消息
```

### 5. 批量发送消息演示
```http
POST /api/partition-strategy/batch-demo
```

### 6. 获取分区策略说明
```http
GET /api/partition-strategy/info
```

## 运行说明

1. 确保 Kafka 服务器正在运行 (默认地址: 192.168.56.10:9092)
2. 启动应用程序
3. 使用 API 接口发送消息
4. 查看控制台日志了解分区分配情况

## 配置说明

- 应用端口: 8096
- Kafka 服务器: 192.168.56.10:9092
- 消费者组: kafka-06-partition-strategy-group
- 主题分区数: 3

## 演示效果

通过查看日志输出，可以观察到：
- 不同分区策略的消息分配情况
- 相同 key 的消息是否分配到同一分区
- 轮询策略的分配规律
- 自定义策略的分配规则

## 注意事项

1. 确保 Kafka 集群中有足够的主题分区
2. 消费者组配置正确，避免消息重复消费
3. 根据实际需求调整分区策略
4. 监控分区负载均衡情况
