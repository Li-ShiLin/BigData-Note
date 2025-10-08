<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [消费者指定Topic、Partition、Offset进行消费](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%8C%87%E5%AE%9Atopicpartitionoffset%E8%BF%9B%E8%A1%8C%E6%B6%88%E8%B4%B9)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. Kafka配置类](#3-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [4. 指定Topic、Partition、Offset消费演示消费者](#4-%E6%8C%87%E5%AE%9Atopicpartitionoffset%E6%B6%88%E8%B4%B9%E6%BC%94%E7%A4%BA%E6%B6%88%E8%B4%B9%E8%80%85)
    - [5. 生产者服务](#5-%E7%94%9F%E4%BA%A7%E8%80%85%E6%9C%8D%E5%8A%A1)
    - [6. REST控制器](#6-rest%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [Kafka指定Topic、Partition、Offset消费详解](#kafka%E6%8C%87%E5%AE%9Atopicpartitionoffset%E6%B6%88%E8%B4%B9%E8%AF%A6%E8%A7%A3)
    - [1. 消费者监听配置](#1-%E6%B6%88%E8%B4%B9%E8%80%85%E7%9B%91%E5%90%AC%E9%85%8D%E7%BD%AE)
    - [2. 监听配置说明](#2-%E7%9B%91%E5%90%AC%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
    - [3. 消息发送方式](#3-%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%96%B9%E5%BC%8F)
      - [方式一：指定分区发送](#%E6%96%B9%E5%BC%8F%E4%B8%80%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E5%8F%91%E9%80%81)
      - [方式二：自动分区发送](#%E6%96%B9%E5%BC%8F%E4%BA%8C%E8%87%AA%E5%8A%A8%E5%88%86%E5%8C%BA%E5%8F%91%E9%80%81)
      - [方式三：批量发送](#%E6%96%B9%E5%BC%8F%E4%B8%89%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送消息到指定分区测试](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95)
      - [发送消息到所有分区测试（自动分区）](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%89%80%E6%9C%89%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95%E8%87%AA%E5%8A%A8%E5%88%86%E5%8C%BA)
      - [批量发送消息到指定分区测试](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95)
      - [批量发送消息到所有分区测试（自动分区）](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%89%80%E6%9C%89%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95%E8%87%AA%E5%8A%A8%E5%88%86%E5%8C%BA)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [指定Topic、Partition、Offset消费日志](#%E6%8C%87%E5%AE%9Atopicpartitionoffset%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
      - [指定偏移量消费日志](#%E6%8C%87%E5%AE%9A%E5%81%8F%E7%A7%BB%E9%87%8F%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 消费者指定Topic、Partition、Offset进行消费

## 项目作用

本项目演示了SpringBoot中Kafka消费者如何指定具体的Topic、Partition、Offset进行消费，包括监听指定分区的所有消息、监听指定分区的指定偏移量开始的消息，帮助开发者理解Kafka消费者精确控制消费位置的工作原理和实际应用场景。

## 项目结构

```
kafka-10-consumer-topic-partition-offset/
├── src/main/java/com/action/kafka10consumertopicpartitionoffset/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类
│   ├── consumer/
│   │   └── TopicPartitionOffsetConsumer.java   # 指定Topic、Partition、Offset消费演示消费者
│   ├── service/
│   │   └── TopicPartitionOffsetProducerService.java # 生产者服务
│   ├── controller/
│   │   └── TopicPartitionOffsetController.java # REST API控制器
│   └── Kafka10ConsumerTopicPartitionOffsetApplication.java # 主启动类
├── src/main/resources/
│   └── application.yml                         # 配置文件
├── src/test/java/com/action/kafka10consumertopicpartitionoffset/
│   └── Kafka10ConsumerTopicPartitionOffsetApplicationTests.java # 测试类
└── pom.xml                                     # Maven配置
```

## 核心实现

### 1. 依赖配置

`pom.xml`：引入Kafka相关依赖

```xml
<dependencies>
    <!-- Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Spring Kafka -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- Spring Boot Test -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>

    <!-- Spring Kafka Test -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.yml`：Kafka服务器和指定Topic、Partition、Offset消费配置

```yaml
# ========================================
# Kafka 指定Topic、Partition、Offset消费演示应用配置
# ========================================

# 应用名称
spring:
  application:
    name: kafka-10-consumer-topic-partition-offset

  # ========================================
  # Kafka 连接配置
  # ========================================
  # Kafka 服务器地址（多个地址用逗号分隔）
  # 默认：localhost:9092
  kafka:
    bootstrap-servers: localhost:9092

    # ========================================
    # 生产者配置
    # ========================================
    # 键序列化器：将消息键序列化为字节数组
    # 使用字符串序列化器，适合演示场景
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      # 值序列化器：将消息值序列化为字节数组
      # 使用字符串序列化器，适合演示场景
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

    # ========================================
    # 消费者配置
    # ========================================
    consumer:
      # 消费者组ID
      group-id: topic-partition-offset-group
      # 键反序列化器：将字节数组反序列化为字符串
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # 值反序列化器：将字节数组反序列化为字符串
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # 自动提交偏移量：false表示手动提交
      enable-auto-commit: false
      # 偏移量重置策略：earliest表示从最早的消息开始消费
      auto-offset-reset: earliest

    # ========================================
    # 消息监听器配置
    # ========================================
    listener:
      # 开启消息监听的手动确认模式
      ack-mode: manual

# ========================================
# 服务器配置
# ========================================
# 服务器端口配置
server:
  port: 9100

# ========================================
# 自定义的配置
# ========================================
kafka:
  topic:
    # 演示主题名称
    name: topic-partition-offset-demo
  consumer:
    # 消费者组ID（与spring.kafka.consumer.group-id保持一致）
    group: topic-partition-offset-group
```

### 3. Kafka配置类

`config/KafkaConfig.java`：配置Kafka主题创建

```java
package com.action.kafka10consumertopicpartitionoffset.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 * 
 * 功能说明：
 * 1) 配置 Kafka 主题创建
 * 2) 定义指定Topic、Partition、Offset消费演示用的 Topic
 * 3) 支持自动创建 Topic，便于演示
 * 
 * 实现细节：
 * - 使用 @Configuration 注解标识为配置类
 * - 通过 @Value 注解读取配置文件中的 Topic 名称
 * - 使用 TopicBuilder 创建 Topic 定义
 * - 配置分区数和副本数，适合演示环境
 * 
 * 关键参数说明：
 * - partitions: 分区数，影响并行处理能力
 * - replicas: 副本数，影响可用性（生产环境建议 >= 3）
 */
@Configuration
public class KafkaConfig {

    /**
     * 演示主题名称配置
     * 从 application.yml 读取，默认 topic-partition-offset-demo
     */
    @Value("${kafka.topic.name:topic-partition-offset-demo}")
    private String topicName;

    /**
     * 指定Topic、Partition、Offset消费演示用 Topic Bean
     * 
     * 作用：自动创建用于演示的 Kafka Topic
     * 配置：5个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic topicPartitionOffsetDemoTopic() {
        return TopicBuilder.name(topicName)
                .partitions(5)    // 分区数：影响并行处理能力，设置为5个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
```

### 4. 指定Topic、Partition、Offset消费演示消费者

`consumer/TopicPartitionOffsetConsumer.java`：监听Kafka主题，接收指定分区和偏移量的消息

```java
package com.action.kafka10consumertopicpartitionoffset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 指定Topic、Partition、Offset消费演示消费者
 * 
 * 功能说明：
 * 1) 演示如何指定具体的Topic、Partition、Offset进行消费
 * 2) 监听指定分区的所有消息
 * 3) 监听指定分区的指定偏移量开始的消息
 * 4) 支持手动确认消息处理
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解配置监听
 * - 使用 topicPartitions 属性指定详细的监听配置
 * - 使用 @TopicPartition 指定主题和分区
 * - 使用 @PartitionOffset 指定分区的起始偏移量
 * - 使用 Acknowledgment 进行手动消息确认
 * 
 * 关键参数说明：
 * - topicPartitions: 可配置更加详细的监听信息，可指定topic、partition、offset监听
 * - @TopicPartition: 指定要监听的主题和分区
 * - @PartitionOffset: 指定分区的起始偏移量
 * - 注意：topics和topicPartitions不能同时使用
 */
@Component
public class TopicPartitionOffsetConsumer {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionOffsetConsumer.class);

    /**
     * 指定Topic、Partition、Offset消费演示
     * 
     * 监听配置说明：
     * 1) 监听 topic-partition-offset-demo 主题的 0、1、2 号分区（从最新偏移量开始）
     * 2) 监听 topic-partition-offset-demo 主题的 3 号分区（从偏移量 3 开始）
     * 3) 监听 topic-partition-offset-demo 主题的 4 号分区（从偏移量 3 开始）
     * 
     * 执行流程：
     * 1) 消费者启动后开始监听指定的分区
     * 2) 接收消息并打印详细信息
     * 3) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录，包含所有元数据
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(
            groupId = "${kafka.consumer.group}",
            topicPartitions = {
                    @TopicPartition(
                            topic = "${kafka.topic.name}",
                            partitions = {"0", "1", "2"},
                            partitionOffsets = {
                                    @PartitionOffset(partition = "3", initialOffset = "3"),
                                    @PartitionOffset(partition = "4", initialOffset = "3")
                            })
            })
    public void consumeMessage(ConsumerRecord<String, String> record,
                              @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                              @Header(value = KafkaHeaders.OFFSET) String offset,
                              Acknowledgment ack) {
        try {
            // 打印接收到的消息详细信息
            log.info("=== 指定Topic、Partition、Offset消费演示 ===");
            log.info("主题: {}", topic);
            log.info("分区: {}", partition);
            log.info("偏移量: {}", offset);
            log.info("消息键: {}", record.key());
            log.info("消息值: {}", record.value());
            log.info("消息时间戳: {}", record.timestamp());
            log.info("消息头: {}", record.headers());
            log.info("完整消息记录: {}", record.toString());
            log.info("==========================================");
            
            // 模拟消息处理逻辑
            processMessage(record);
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("消息处理完成并已确认: topic={}, partition={}, offset={}", 
                    topic, partition, offset);
            
        } catch (Exception e) {
            log.error("处理消息时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 模拟消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟实际业务处理逻辑
     * 2) 根据不同的分区和偏移量进行不同的处理
     * 3) 演示消息处理的完整流程
     * 
     * @param record 消息记录
     * @throws Exception 处理异常
     */
    private void processMessage(ConsumerRecord<String, String> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();
        String value = record.value();
        
        log.info("开始处理消息: topic={}, partition={}, offset={}, key={}, value={}", 
                topic, partition, offset, key, value);
        
        // 根据分区进行不同的处理逻辑
        switch (partition) {
            case 0:
                log.info("处理分区0的消息: {}", value);
                break;
            case 1:
                log.info("处理分区1的消息: {}", value);
                break;
            case 2:
                log.info("处理分区2的消息: {}", value);
                break;
            case 3:
                log.info("处理分区3的消息（从偏移量3开始）: {}", value);
                break;
            case 4:
                log.info("处理分区4的消息（从偏移量3开始）: {}", value);
                break;
            default:
                log.warn("未知分区: {}", partition);
        }
        
        // 模拟处理时间
        Thread.sleep(100);
        
        log.info("消息处理完成: topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
```

### 5. 生产者服务

`service/TopicPartitionOffsetProducerService.java`：封装KafkaTemplate消息发送逻辑

```java
package com.action.kafka10consumertopicpartitionoffset.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 指定Topic、Partition、Offset消费演示生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供多种消息发送方式，便于测试不同分区的消费
 * 3) 支持指定分区发送消息
 * 4) 支持异步消息发送
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 返回 CompletableFuture 支持异步处理和回调
 * - 提供多种发送方式，便于测试不同场景
 * 
 * 关键参数说明：
 * - KafkaTemplate: Spring 提供的 Kafka 操作模板，线程安全
 * - CompletableFuture: 异步编程模型，支持链式操作和异常处理
 */
@Service
public class TopicPartitionOffsetProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 目标 Topic 名称
     * 从配置文件读取，默认 topic-partition-offset-demo
     */
    @Value("${kafka.topic.name:topic-partition-offset-demo}")
    private String topicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public TopicPartitionOffsetProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送消息到指定分区
     * 
     * 执行流程：
     * 1) 使用指定的分区号发送消息
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param key 消息键（可选）
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessageToPartition(int partition, String key, String value) {
        // 使用 KafkaTemplate 发送消息到指定分区
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(topicName, partition, key, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到指定分区（无键）
     * 
     * 执行流程：
     * 1) 使用指定的分区号发送消息（无键）
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessageToPartition(int partition, String value) {
        // 使用 KafkaTemplate 发送消息到指定分区（无键）
        return kafkaTemplate.send(topicName, partition, null, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到所有分区（让Kafka自动分区）
     * 
     * 执行流程：
     * 1) 发送消息，让Kafka根据键的哈希值自动选择分区
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param key 消息键（用于分区路由）
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String key, String value) {
        // 使用 KafkaTemplate 发送消息，让Kafka自动选择分区
        return kafkaTemplate.send(topicName, key, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到所有分区（无键，让Kafka自动分区）
     * 
     * 执行流程：
     * 1) 发送消息（无键），让Kafka自动选择分区
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String value) {
        // 使用 KafkaTemplate 发送消息（无键），让Kafka自动选择分区
        return kafkaTemplate.send(topicName, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 批量发送消息到指定分区
     * 
     * 执行流程：
     * 1) 循环发送指定数量的消息到指定分区
     * 2) 为每个消息生成唯一的键和值
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchMessagesToPartition(int partition, int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成唯一的键和值
            String key = "partition-" + partition + "-key-" + i;
            String value = "partition-" + partition + "-message-" + i;
            
            // 发送消息到指定分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, partition, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送消息到所有分区（用于测试自动分区）
     * 
     * 执行流程：
     * 1) 循环发送指定数量的消息到所有分区
     * 2) 为每个消息生成唯一的键和值
     * 3) 让Kafka根据键的哈希值自动选择分区
     * 4) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchMessages(int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成唯一的键和值
            String key = "auto-partition-key-" + i;
            String value = "auto-partition-message-" + i;
            
            // 发送消息，让Kafka自动选择分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }
}
```

### 6. REST控制器

`controller/TopicPartitionOffsetController.java`：提供HTTP接口用于测试消息发送

```java
package com.action.kafka10consumertopicpartitionoffset.controller;

import com.action.kafka10consumertopicpartitionoffset.service.TopicPartitionOffsetProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 指定Topic、Partition、Offset消费演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试消息发送
 * 2) 演示如何向不同分区发送消息
 * 3) 支持指定分区发送和自动分区发送
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/partition: 发送消息到指定分区
 * - GET /api/send/auto: 发送消息到所有分区（自动分区）
 * - GET /api/send/batch/partition: 批量发送消息到指定分区
 * - GET /api/send/batch/auto: 批量发送消息到所有分区（自动分区）
 * 
 * 使用示例：
 * curl "http://localhost:9100/api/send/partition?partition=0&key=test-key&value=test-message"
 * curl "http://localhost:9100/api/send/auto?key=auto-key&value=auto-message"
 * curl "http://localhost:9100/api/send/batch/partition?partition=1&count=5"
 * curl "http://localhost:9100/api/send/batch/auto?count=10"
 */
@RestController
@RequestMapping("/api/send")
public class TopicPartitionOffsetController {

    /**
     * 指定Topic、Partition、Offset消费演示生产者服务
     * 负责实际的消息发送逻辑
     */
    private final TopicPartitionOffsetProducerService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 指定Topic、Partition、Offset消费演示生产者服务
     */
    public TopicPartitionOffsetController(TopicPartitionOffsetProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送消息到指定分区接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-4）
     * - key: 消息键（可选）
     * - value: 消息值（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param key 消息键（可选）
     * @param value 消息值
     * @return HTTP 响应实体
     */
    @GetMapping("/partition")
    public ResponseEntity<Map<String, Object>> sendMessageToPartition(
            @RequestParam("partition") int partition,
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 4) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-4 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层发送消息到指定分区
            if (key != null && !key.isEmpty()) {
                producerService.sendMessageToPartition(partition, key, value);
            } else {
                producerService.sendMessageToPartition(partition, value);
            }
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "消息发送到指定分区成功");
            response.put("data", Map.of("partition", partition, "key", key, "value", value));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "消息发送到指定分区失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送消息到所有分区接口（自动分区）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/auto
     * 
     * 参数说明：
     * - key: 消息键（可选）
     * - value: 消息值（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param key 消息键（可选）
     * @param value 消息值
     * @return HTTP 响应实体
     */
    @GetMapping("/auto")
    public ResponseEntity<Map<String, Object>> sendMessageAuto(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 调用服务层发送消息（自动分区）
            if (key != null && !key.isEmpty()) {
                producerService.sendMessage(key, value);
            } else {
                producerService.sendMessage(value);
            }
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "消息发送成功（自动分区）");
            response.put("data", Map.of("key", key, "value", value));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到指定分区接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-4）
     * - count: 要发送的消息数量（可选，默认 5）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/partition")
    public ResponseEntity<Map<String, Object>> sendBatchMessagesToPartition(
            @RequestParam("partition") int partition,
            @RequestParam(value = "count", defaultValue = "5") int count) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 4) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-4 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 50) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-50 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送消息到指定分区
            producerService.sendBatchMessagesToPartition(partition, count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量消息发送到指定分区成功");
            response.put("data", Map.of("partition", partition, "count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量消息发送到指定分区失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到所有分区接口（自动分区）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/auto
     * 
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 10）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/auto")
    public ResponseEntity<Map<String, Object>> sendBatchMessagesAuto(
            @RequestParam(value = "count", defaultValue = "10") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 100) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-100 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送消息（自动分区）
            producerService.sendBatchMessages(count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量消息发送成功（自动分区）");
            response.put("data", Map.of("count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
```

## Kafka指定Topic、Partition、Offset消费详解

### 1. 消费者监听配置

```java
@KafkaListener(
        groupId = "${kafka.consumer.group}",
        topicPartitions = {
                @TopicPartition(
                        topic = "${kafka.topic.name}",
                        partitions = {"0", "1", "2"},
                        partitionOffsets = {
                                @PartitionOffset(partition = "3", initialOffset = "3"),
                                @PartitionOffset(partition = "4", initialOffset = "3")
                        })
        })
public void consumeMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
    // 处理消息
}
```

### 2. 监听配置说明

- **partitions = {"0", "1", "2"}**: 监听分区 0、1、2 的所有消息（从最新偏移量开始）
- **partitionOffsets**: 监听分区 3、4 从偏移量 3 开始的消息
- **注意**: `topics` 和 `topicPartitions` 不能同时使用

### 3. 消息发送方式

#### 方式一：指定分区发送

```java
// 发送消息到指定分区
producerService.sendMessageToPartition(0, "key", "value");

// 发送消息到指定分区（无键）
producerService.sendMessageToPartition(0, "value");
```

#### 方式二：自动分区发送

```java
// 发送消息，让Kafka自动选择分区
producerService.sendMessage("key", "value");

// 发送消息（无键），让Kafka自动选择分区
producerService.sendMessage("value");
```

#### 方式三：批量发送

```java
// 批量发送消息到指定分区
producerService.sendBatchMessagesToPartition(0, 10);

// 批量发送消息到所有分区（自动分区）
producerService.sendBatchMessages(20);
```

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-10-consumer-topic-partition-offset

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送消息到指定分区测试

```bash
# 发送消息到分区0
curl "http://localhost:9100/api/send/partition?partition=0&key=test-key&value=test-message"

# 发送消息到分区1（无键）
curl "http://localhost:9100/api/send/partition?partition=1&value=test-message-no-key"

# 发送消息到分区3（从偏移量3开始消费）
curl "http://localhost:9100/api/send/partition?partition=3&key=offset-test&value=offset-message"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "消息发送到指定分区成功",
  "data": {
    "partition": 0,
    "key": "test-key",
    "value": "test-message"
  }
}
```

#### 发送消息到所有分区测试（自动分区）

```bash
# 发送消息，让Kafka自动选择分区
curl "http://localhost:9100/api/send/auto?key=auto-key&value=auto-message"

# 发送消息（无键），让Kafka自动选择分区
curl "http://localhost:9100/api/send/auto?value=auto-message-no-key"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "消息发送成功（自动分区）",
  "data": {
    "key": "auto-key",
    "value": "auto-message"
  }
}
```

#### 批量发送消息到指定分区测试

```bash
# 批量发送消息到分区0
curl "http://localhost:9100/api/send/batch/partition?partition=0&count=5"

# 批量发送消息到分区2
curl "http://localhost:9100/api/send/batch/partition?partition=2&count=10"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量消息发送到指定分区成功",
  "data": {
    "partition": 0,
    "count": 5
  }
}
```

#### 批量发送消息到所有分区测试（自动分区）

```bash
# 批量发送消息，让Kafka自动选择分区
curl "http://localhost:9100/api/send/batch/auto?count=20"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量消息发送成功（自动分区）",
  "data": {
    "count": 20
  }
}
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 指定Topic、Partition、Offset消费日志

```
INFO  - === 指定Topic、Partition、Offset消费演示 ===
INFO  - 主题: topic-partition-offset-demo
INFO  - 分区: 0
INFO  - 偏移量: 0
INFO  - 消息键: test-key
INFO  - 消息值: test-message
INFO  - 消息时间戳: 1640995200000
INFO  - 消息头: RecordHeaders(headers = [], isReadOnly = false)
INFO  - 完整消息记录: ConsumerRecord(topic = topic-partition-offset-demo, partition = 0, offset = 0, CreateTime = 1640995200000, serialized key size = 8, serialized value size = 12, headers = RecordHeaders(headers = [], isReadOnly = false), key = test-key, value = test-message)
INFO  - ==========================================
INFO  - 开始处理消息: topic=topic-partition-offset-demo, partition=0, offset=0, key=test-key, value=test-message
INFO  - 处理分区0的消息: test-message
INFO  - 消息处理完成: topic=topic-partition-offset-demo, partition=0, offset=0
INFO  - 消息处理完成并已确认: topic=topic-partition-offset-demo, partition=0, offset=0
```

#### 指定偏移量消费日志

```
INFO  - === 指定Topic、Partition、Offset消费演示 ===
INFO  - 主题: topic-partition-offset-demo
INFO  - 分区: 3
INFO  - 偏移量: 3
INFO  - 消息键: offset-test
INFO  - 消息值: offset-message
INFO  - 消息时间戳: 1640995200000
INFO  - 消息头: RecordHeaders(headers = [], isReadOnly = false)
INFO  - 完整消息记录: ConsumerRecord(topic = topic-partition-offset-demo, partition = 3, offset = 3, CreateTime = 1640995200000, serialized key size = 10, serialized value size = 13, headers = RecordHeaders(headers = [], isReadOnly = false), key = offset-test, value = offset-message)
INFO  - ==========================================
INFO  - 开始处理消息: topic=topic-partition-offset-demo, partition=3, offset=3, key=offset-test, value=offset-message
INFO  - 处理分区3的消息（从偏移量3开始）: offset-message
INFO  - 消息处理完成: topic=topic-partition-offset-demo, partition=3, offset=3
INFO  - 消息处理完成并已确认: topic=topic-partition-offset-demo, partition=3, offset=3
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9092`运行
2. **分区配置**: 确保Topic有足够的分区数（本示例使用5个分区）
3. **偏移量设置**: 指定偏移量时，确保偏移量存在，否则会从最新偏移量开始消费
4. **消费者组**: 不同消费者组可以独立消费消息
5. **手动确认**: 消费者使用手动确认模式，需要调用ack.acknowledge()
6. **异常处理**: 消息处理失败时不会确认消息，会被重新消费
7. **分区路由**: 带键的消息会根据键的哈希值路由到特定分区
8. **批量发送**: 批量发送时注意控制数量，避免过载
9. **配置限制**: topics和topicPartitions不能同时使用
10. **线程安全**: KafkaTemplate是线程安全的，可以并发使用
11. **配置管理**: 建议将Topic名称等配置放在配置文件中管理
