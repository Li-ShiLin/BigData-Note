<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [消费者批量消息消费](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%89%B9%E9%87%8F%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. Kafka配置类](#3-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [4. 批量消息消费演示消费者](#4-%E6%89%B9%E9%87%8F%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E6%BC%94%E7%A4%BA%E6%B6%88%E8%B4%B9%E8%80%85)
    - [5. 用户行为数据模型](#5-%E7%94%A8%E6%88%B7%E8%A1%8C%E4%B8%BA%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [UserBehavior.java](#userbehaviorjava)
    - [6. 批量消息发送生产者服务](#6-%E6%89%B9%E9%87%8F%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E7%94%9F%E4%BA%A7%E8%80%85%E6%9C%8D%E5%8A%A1)
    - [6. REST控制器](#6-rest%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [Kafka批量消费详解](#kafka%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E8%AF%A6%E8%A7%A3)
    - [1. 批量消费配置](#1-%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E9%85%8D%E7%BD%AE)
    - [2. 消费者监听配置](#2-%E6%B6%88%E8%B4%B9%E8%80%85%E7%9B%91%E5%90%AC%E9%85%8D%E7%BD%AE)
    - [3. 批量消费优势](#3-%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E4%BC%98%E5%8A%BF)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送单条消息测试](#%E5%8F%91%E9%80%81%E5%8D%95%E6%9D%A1%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
      - [批量发送消息测试（自动分区）](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95%E8%87%AA%E5%8A%A8%E5%88%86%E5%8C%BA)
      - [批量发送消息到指定分区测试](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95)
      - [发送用户行为数据测试（模拟真实业务场景）](#%E5%8F%91%E9%80%81%E7%94%A8%E6%88%B7%E8%A1%8C%E4%B8%BA%E6%95%B0%E6%8D%AE%E6%B5%8B%E8%AF%95%E6%A8%A1%E6%8B%9F%E7%9C%9F%E5%AE%9E%E4%B8%9A%E5%8A%A1%E5%9C%BA%E6%99%AF)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [批量消息消费日志](#%E6%89%B9%E9%87%8F%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
      - [用户行为数据消费日志](#%E7%94%A8%E6%88%B7%E8%A1%8C%E4%B8%BA%E6%95%B0%E6%8D%AE%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 消费者批量消息消费

## 项目作用

本项目演示了SpringBoot中Kafka批量消息消费的实现，包括批量消息发送、批量消息消费、手动确认机制等核心功能，帮助开发者理解Kafka批量消费的工作原理和实际应用场景。

## 项目结构

```
kafka-11-batch-consumer/
├── src/main/java/com/action/kafka11batchconsumer/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类
│   ├── consumer/
│   │   └── BatchConsumer.java                  # 批量消息消费演示消费者
│   ├── model/
│   │   └── UserBehavior.java                   # 用户行为数据模型
│   ├── service/
│   │   └── BatchProducerService.java           # 批量消息发送生产者服务
│   ├── controller/
│   │   └── BatchConsumerController.java        # REST API控制器
│   └── Kafka11BatchConsumerApplication.java    # 主启动类
├── src/main/resources/
│   └── application.properties                  # 配置文件
├── src/test/java/com/action/kafka11batchconsumer/
│   └── Kafka11BatchConsumerApplicationTests.java # 测试类
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

`application.properties`：Kafka服务器和批量消费配置

```properties
# ========================================
# Kafka 批量消息消费演示应用配置
# ========================================

# 应用名称
spring.application.name=kafka-11-batch-consumer

# ========================================
# Kafka 连接配置
# ========================================
# Kafka 服务器地址（多个地址用逗号分隔）
# 默认：localhost:9092
spring.kafka.bootstrap-servers=localhost:9092

# ========================================
# 生产者配置
# ========================================
# 键序列化器：将消息键序列化为字节数组
# 使用字符串序列化器，适合演示场景
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
# 值序列化器：将消息值序列化为字节数组
# 使用字符串序列化器，适合演示场景
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# ========================================
# 消费者配置
# ========================================
# 消费者组ID
spring.kafka.consumer.group-id=batch-consumer-group
# 键反序列化器：将字节数组反序列化为字符串
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# 值反序列化器：将字节数组反序列化为字符串
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# 自动提交偏移量：false表示手动提交
spring.kafka.consumer.enable-auto-commit=false
# 偏移量重置策略：earliest表示从最早的消息开始消费
spring.kafka.consumer.auto-offset-reset=earliest

# ========================================
# 消息监听器配置
# ========================================
# 开启消息监听的手动确认模式
spring.kafka.listener.ack-mode=manual
# 设置批量消费，默认是单个消息消费
spring.kafka.listener.type=batch
# 批量消费每次最多消费多少条消息
spring.kafka.consumer.max-poll-records=20

# ========================================
# 服务器配置
# ========================================
# 服务器端口配置
server.port=9101

# ========================================
# 自定义的配置
# ========================================
kafka.topic.name=batch-consumer-demo
kafka.consumer.group=batch-consumer-group
```

### 3. Kafka配置类

`config/KafkaConfig.java`：配置Kafka主题创建

```java
package com.action.kafka11batchconsumer.config;

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
 * 2) 定义批量消息消费演示用的 Topic
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
     * 从 application.properties 读取，默认 batch-consumer-demo
     */
    @Value("${kafka.topic.name:batch-consumer-demo}")
    private String topicName;

    /**
     * 批量消息消费演示用 Topic Bean
     * 
     * 作用：自动创建用于演示的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic batchConsumerDemoTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)    // 分区数：影响并行处理能力，设置为3个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
```

### 4. 批量消息消费演示消费者

`consumer/BatchConsumer.java`：监听Kafka主题，接收批量消息

```java
package com.action.kafka11batchconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 批量消息消费演示消费者
 * 
 * 功能说明：
 * 1) 演示如何批量消费Kafka消息
 * 2) 监听指定主题的所有消息
 * 3) 支持批量处理多条消息
 * 4) 支持手动确认消息处理
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解配置监听
 * - 使用 topics 属性指定要监听的主题
 * - 使用 List<ConsumerRecord> 接收批量消息
 * - 使用 Acknowledgment 进行手动消息确认
 * 
 * 关键参数说明：
 * - topics: 要监听的主题名称
 * - groupId: 消费者组ID，用于负载均衡
 * - 注意：批量消费需要配置 spring.kafka.listener.type=batch
 */
@Component
public class BatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);

    /**
     * 批量消息消费演示
     * 
     * 监听配置说明：
     * 1) 监听 batch-consumer-demo 主题的所有消息
     * 2) 使用批量消费模式，每次最多消费20条消息
     * 3) 使用手动确认模式，确保消息处理完成后再确认
     * 
     * 执行流程：
     * 1) 消费者启动后开始监听指定主题
     * 2) 接收批量消息并打印详细信息
     * 3) 批量处理消息
     * 4) 手动确认消息处理完成
     * 
     * 参数说明：
     * - List<ConsumerRecord> records: 批量消息记录列表
     * - Acknowledgment ack: 手动确认对象
     * 
     * 注意：在批量消费模式下，不能使用 @Header 注解获取主题信息，
     * 需要从 ConsumerRecord 中获取主题信息
     */
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.group}")
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records,
                                   @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   Acknowledgment ack) {
        try {
            // 打印接收到的批量消息信息
            log.info("=== 批量消息消费演示 ===");
            log.info("主题: {}", topic);
            log.info("批量消息数量: {}", records.size());
            log.info("==========================================");
            
            // 批量处理消息
            processBatchMessages(records);
            
            // 手动确认所有消息处理完成
            ack.acknowledge();
            log.info("批量消息处理完成并已确认: 主题={}, 数量={}", topic, records.size());
            
        } catch (Exception e) {
            log.error("处理批量消息时发生错误: 主题={}, 数量={}, 错误={}", 
                    topic, records.size(), e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 批量处理消息逻辑
     * 
     * 功能说明：
     * 1) 遍历批量消息列表
     * 2) 打印每条消息的详细信息（合并成一行）
     * 3) 根据分区进行不同的处理逻辑
     * 4) 演示批量处理的优势
     * 
     * @param records 批量消息记录列表
     * @throws Exception 处理异常
     */
    private void processBatchMessages(List<ConsumerRecord<String, String>> records) throws Exception {
        log.info("开始批量处理消息，数量: {}", records.size());
        
        // 遍历批量消息
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> record = records.get(i);
            
            // 根据分区确定处理描述
            String partitionDesc = switch (record.partition()) {
                case 0 -> "分区0";
                case 1 -> "分区1";
                case 2 -> "分区2";
                default -> "未知分区" + record.partition();
            };
            
            // 打印单条消息信息（合并成一行，包含分区处理信息）
            log.info("批量消息[{}]: topic={}, partition={}, offset={}, key={}, value={}, timestamp={}, 处理{}: {}", 
                    i + 1, record.topic(), record.partition(), record.offset(), 
                    record.key(), record.value(), record.timestamp(), partitionDesc, record.value());
        }
        
        log.info("批量消息处理完成，总数量: {}", records.size());
    }
}
```

### 5. 用户行为数据模型

#### UserBehavior.java

```java
/**
 * 用户行为数据模型
 * 
 * 功能说明：
 * 1) 定义用户行为数据的结构
 * 2) 用于模拟真实业务场景
 * 3) 支持JSON序列化和反序列化
 * 
 * 字段说明：
 * - userId: 用户ID
 * - action: 用户行为（login/view/purchase）
 * - timestamp: 行为发生时间戳
 * - partition: 目标分区号
 */
public class UserBehavior {
    private String userId;
    private String action;
    private Long timestamp;
    private Integer partition;
    
    // 构造函数、getter、setter方法
}
```

### 6. 批量消息发送生产者服务

`service/BatchProducerService.java`：封装KafkaTemplate消息发送逻辑

```java
package com.action.kafka11batchconsumer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 批量消息发送演示生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供多种批量消息发送方式，便于测试批量消费
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
public class BatchProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 目标 Topic 名称
     * 从配置文件读取，默认 batch-consumer-demo
     */
    @Value("${kafka.topic.name:batch-consumer-demo}")
    private String topicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public BatchProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送单条消息到指定分区
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
     * 发送单条消息到指定分区（无键）
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
     * 发送单条消息到所有分区（让Kafka自动分区）
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
     * 发送单条消息到所有分区（无键，让Kafka自动分区）
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
     * 批量发送消息到所有分区（用于测试自动分区）
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

    /**
     * 发送批量消息到指定分区（模拟真实业务场景）
     * 
     * 执行流程：
     * 1) 模拟发送用户行为数据
     * 2) 为每个消息生成业务相关的键和值
     * 3) 使用CommonUtils进行对象到字符串的转换
     * 4) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserBehaviorMessages(int partition, int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成业务相关的用户行为数据
            String userId = "user-" + (i % 100); // 模拟100个用户
            String action = i % 3 == 0 ? "login" : (i % 3 == 1 ? "view" : "purchase");
            Long timestamp = System.currentTimeMillis();
            
            // 创建用户行为对象
            UserBehavior userBehavior = new UserBehavior(userId, action, timestamp, partition);
            
            // 使用CommonUtils将对象转换为JSON字符串
            String key = userId;
            String value = CommonUtils.convert(userBehavior, String.class);
            
            // 发送消息到指定分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, partition, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }
}
```

### 6. REST控制器

`controller/BatchConsumerController.java`：提供HTTP接口用于测试消息发送

```java
package com.action.kafka11batchconsumer.controller;

import com.action.kafka11batchconsumer.service.BatchProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 批量消息消费演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试批量消息发送
 * 2) 演示如何向不同分区发送批量消息
 * 3) 支持指定分区发送和自动分区发送
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/single: 发送单条消息
 * - GET /api/send/batch: 批量发送消息
 * - GET /api/send/batch/partition: 批量发送消息到指定分区
 * - GET /api/send/batch/user-behavior: 发送用户行为数据（模拟真实业务场景）
 * 
 * 使用示例：
 * curl "http://localhost:9101/api/send/single?key=test-key&value=test-message"
 * curl "http://localhost:9101/api/send/batch?count=10"
 * curl "http://localhost:9101/api/send/batch/partition?partition=0&count=5"
 * curl "http://localhost:9101/api/send/batch/user-behavior?partition=1&count=8"
 */
@RestController
@RequestMapping("/api/send")
public class BatchConsumerController {

    /**
     * 批量消息发送演示生产者服务
     * 负责实际的消息发送逻辑
     */
    private final BatchProducerService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 批量消息发送演示生产者服务
     */
    public BatchConsumerController(BatchProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送单条消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/single
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
    @GetMapping("/single")
    public ResponseEntity<Map<String, Object>> sendSingleMessage(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam("value") String value) {
        try {
            // 调用服务层发送单条消息
            if (key != null && !key.isEmpty()) {
                producerService.sendMessage(key, value);
            } else {
                producerService.sendMessage(value);
            }
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "单条消息发送成功");
            response.put("data", Map.of("key", key, "value", value));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "单条消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息接口（自动分区）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch
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
    @GetMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchMessages(
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

    /**
     * 批量发送消息到指定分区接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/partition
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
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
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");
                
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
     * 发送用户行为数据接口（模拟真实业务场景）
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch/user-behavior
     * 
     * 参数说明：
     * - partition: 目标分区号（必填，0-2）
     * - count: 要发送的消息数量（可选，默认 8）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch/user-behavior")
    public ResponseEntity<Map<String, Object>> sendUserBehaviorMessages(
            @RequestParam("partition") int partition,
            @RequestParam(value = "count", defaultValue = "8") int count) {
        try {
            // 验证分区号
            if (partition < 0 || partition > 2) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "分区号必须在 0-2 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 30) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-30 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层发送用户行为数据
            producerService.sendUserBehaviorMessages(partition, count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "用户行为数据发送成功");
            response.put("data", Map.of("partition", partition, "count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "用户行为数据发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
```

## Kafka批量消费详解

### 1. 批量消费配置

```properties
# 设置批量消费，默认是单个消息消费
spring.kafka.listener.type=batch
# 批量消费每次最多消费多少条消息
spring.kafka.consumer.max-poll-records=20
# 开启消息监听的手动确认模式
spring.kafka.listener.ack-mode=manual
```

### 2. 消费者监听配置

```java
@KafkaListener(
        topics = "${kafka.topic.name}",
        groupId = "${kafka.consumer.group}")
public void consumeBatchMessages(List<ConsumerRecord<String, String>> records,
                               @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                               Acknowledgment ack) {
    // 处理批量消息
}
```

### 3. 批量消费优势

- **提高吞吐量**：一次处理多条消息，减少网络开销
- **提高效率**：批量处理减少系统调用次数
- **事务性**：批量确认，要么全部成功，要么全部失败
- **资源优化**：减少线程切换和上下文切换

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-11-batch-consumer

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送单条消息测试

```bash
# 发送单条消息（带键）
curl "http://localhost:9101/api/send/single?key=test-key&value=test-message"

# 发送单条消息（无键）
curl "http://localhost:9101/api/send/single?value=test-message-no-key"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "单条消息发送成功",
  "data": {
    "key": "test-key",
    "value": "test-message"
  }
}
```

#### 批量发送消息测试（自动分区）

```bash
# 批量发送消息，让Kafka自动选择分区
curl "http://localhost:9101/api/send/batch?count=15"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量消息发送成功（自动分区）",
  "data": {
    "count": 15
  }
}
```

#### 批量发送消息到指定分区测试

```bash
# 批量发送消息到分区0
curl "http://localhost:9101/api/send/batch/partition?partition=0&count=8"

# 批量发送消息到分区1
curl "http://localhost:9101/api/send/batch/partition?partition=1&count=12"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量消息发送到指定分区成功",
  "data": {
    "partition": 0,
    "count": 8
  }
}
```

#### 发送用户行为数据测试（模拟真实业务场景）

```bash
# 发送用户行为数据到分区1
curl "http://localhost:9101/api/send/batch/user-behavior?partition=1&count=10"

# 发送用户行为数据到分区2
curl "http://localhost:9101/api/send/batch/user-behavior?partition=2&count=6"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "用户行为数据发送成功",
  "data": {
    "partition": 1,
    "count": 10
  }
}
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 批量消息消费日志

```
INFO  - === 批量消息消费演示 ===
INFO  - 主题: batch-consumer-demo
INFO  - 批量消息数量: 8
INFO  - ==========================================
INFO  - 开始批量处理消息，数量: 8
INFO  - 批量消息[1]: topic=batch-consumer-demo, partition=0, offset=0, key=partition-0-key-0, value=partition-0-message-0, timestamp=1640995200000, 处理分区0: partition-0-message-0
INFO  - 批量消息[2]: topic=batch-consumer-demo, partition=0, offset=1, key=partition-0-key-1, value=partition-0-message-1, timestamp=1640995200000, 处理分区0: partition-0-message-1
INFO  - 批量消息[3]: topic=batch-consumer-demo, partition=1, offset=0, key=partition-1-key-0, value=partition-1-message-0, timestamp=1640995200000, 处理分区1: partition-1-message-0
INFO  - 批量消息[4]: topic=batch-consumer-demo, partition=1, offset=1, key=partition-1-key-1, value=partition-1-message-1, timestamp=1640995200000, 处理分区1: partition-1-message-1
INFO  - 批量消息[5]: topic=batch-consumer-demo, partition=2, offset=0, key=partition-2-key-0, value=partition-2-message-0, timestamp=1640995200000, 处理分区2: partition-2-message-0
INFO  - 批量消息[6]: topic=batch-consumer-demo, partition=2, offset=1, key=partition-2-key-1, value=partition-2-message-1, timestamp=1640995200000, 处理分区2: partition-2-message-1
INFO  - 批量消息[7]: topic=batch-consumer-demo, partition=0, offset=2, key=partition-0-key-2, value=partition-0-message-2, timestamp=1640995200000, 处理分区0: partition-0-message-2
INFO  - 批量消息[8]: topic=batch-consumer-demo, partition=1, offset=2, key=partition-1-key-2, value=partition-1-message-2, timestamp=1640995200000, 处理分区1: partition-1-message-2
INFO  - 批量消息处理完成，总数量: 8
INFO  - 批量消息处理完成并已确认: 主题=batch-consumer-demo, 数量=8
```

#### 用户行为数据消费日志

```
INFO  - === 批量消息消费演示 ===
INFO  - 主题: batch-consumer-demo
INFO  - 批量消息数量: 6
INFO  - ==========================================
INFO  - 开始批量处理消息，数量: 6
INFO  - 批量消息[1]: topic=batch-consumer-demo, partition=1, offset=0, key=user-0, value={"userId":"user-0","action":"login","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-0","action":"login","timestamp":1640995200000,"partition":1}
INFO  - 批量消息[2]: topic=batch-consumer-demo, partition=1, offset=1, key=user-1, value={"userId":"user-1","action":"view","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-1","action":"view","timestamp":1640995200000,"partition":1}
INFO  - 批量消息[3]: topic=batch-consumer-demo, partition=1, offset=2, key=user-2, value={"userId":"user-2","action":"purchase","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-2","action":"purchase","timestamp":1640995200000,"partition":1}
INFO  - 批量消息[4]: topic=batch-consumer-demo, partition=1, offset=3, key=user-3, value={"userId":"user-3","action":"login","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-3","action":"login","timestamp":1640995200000,"partition":1}
INFO  - 批量消息[5]: topic=batch-consumer-demo, partition=1, offset=4, key=user-4, value={"userId":"user-4","action":"view","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-4","action":"view","timestamp":1640995200000,"partition":1}
INFO  - 批量消息[6]: topic=batch-consumer-demo, partition=1, offset=5, key=user-5, value={"userId":"user-5","action":"purchase","timestamp":1640995200000,"partition":1}, timestamp=1640995200000, 处理分区1: {"userId":"user-5","action":"purchase","timestamp":1640995200000,"partition":1}
INFO  - 批量消息处理完成，总数量: 6
INFO  - 批量消息处理完成并已确认: 主题=batch-consumer-demo, 数量=6
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`192.168.56.10:9092`运行
2. **分区配置**: 确保Topic有足够的分区数（本示例使用3个分区）
3. **批量大小**: 根据业务需求调整`max-poll-records`参数
4. **消费者组**: 不同消费者组可以独立消费消息
5. **手动确认**: 消费者使用手动确认模式，需要调用ack.acknowledge()
6. **异常处理**: 消息处理失败时不会确认消息，会被重新消费
7. **批量处理**: 批量消费时，要么全部成功，要么全部失败
8. **性能优化**: 批量消费可以提高吞吐量，但需要合理设置批量大小
9. **内存管理**: 批量消费会占用更多内存，需要监控内存使用情况
10. **配置管理**: 建议将Topic名称等配置放在配置文件中管理
