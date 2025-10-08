<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [offset偏移量行为](#offset%E5%81%8F%E7%A7%BB%E9%87%8F%E8%A1%8C%E4%B8%BA)
  - [目标](#%E7%9B%AE%E6%A0%87)
  - [重要Topic与消费者组](#%E9%87%8D%E8%A6%81topic%E4%B8%8E%E6%B6%88%E8%B4%B9%E8%80%85%E7%BB%84)
  - [核心概念](#%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5)
    - [偏移量策略对比](#%E5%81%8F%E7%A7%BB%E9%87%8F%E7%AD%96%E7%95%A5%E5%AF%B9%E6%AF%94)
    - [重要说明](#%E9%87%8D%E8%A6%81%E8%AF%B4%E6%98%8E)
  - [技术实现](#%E6%8A%80%E6%9C%AF%E5%AE%9E%E7%8E%B0)
    - [1. 项目依赖](#1-%E9%A1%B9%E7%9B%AE%E4%BE%9D%E8%B5%96)
    - [2. 应用配置](#2-%E5%BA%94%E7%94%A8%E9%85%8D%E7%BD%AE)
    - [3. 常量定义](#3-%E5%B8%B8%E9%87%8F%E5%AE%9A%E4%B9%89)
    - [4. Topic 自动创建](#4-topic-%E8%87%AA%E5%8A%A8%E5%88%9B%E5%BB%BA)
    - [5. 消费者配置](#5-%E6%B6%88%E8%B4%B9%E8%80%85%E9%85%8D%E7%BD%AE)
    - [6. 消息生产者](#6-%E6%B6%88%E6%81%AF%E7%94%9F%E4%BA%A7%E8%80%85)
    - [7. 消息消费者](#7-%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E8%80%85)
      - [最新偏移量消费者](#%E6%9C%80%E6%96%B0%E5%81%8F%E7%A7%BB%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85)
      - [最早偏移量消费者](#%E6%9C%80%E6%97%A9%E5%81%8F%E7%A7%BB%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85)
      - [手动偏移量消费者](#%E6%89%8B%E5%8A%A8%E5%81%8F%E7%A7%BB%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85)
    - [8. 偏移量重置服务](#8-%E5%81%8F%E7%A7%BB%E9%87%8F%E9%87%8D%E7%BD%AE%E6%9C%8D%E5%8A%A1)
    - [9. REST 控制器](#9-rest-%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [API 接口测试](#api-%E6%8E%A5%E5%8F%A3%E6%B5%8B%E8%AF%95)
    - [关键端点](#%E5%85%B3%E9%94%AE%E7%AB%AF%E7%82%B9)
    - [Windows 11 测试方法](#windows-11-%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
      - [方法1：浏览器直接测试（最简单）](#%E6%96%B9%E6%B3%951%E6%B5%8F%E8%A7%88%E5%99%A8%E7%9B%B4%E6%8E%A5%E6%B5%8B%E8%AF%95%E6%9C%80%E7%AE%80%E5%8D%95)
      - [方法2：PowerShell 测试](#%E6%96%B9%E6%B3%952powershell-%E6%B5%8B%E8%AF%95)
      - [方法3：curl 命令测试](#%E6%96%B9%E6%B3%953curl-%E5%91%BD%E4%BB%A4%E6%B5%8B%E8%AF%95)
  - [演示步骤与预期结果](#%E6%BC%94%E7%A4%BA%E6%AD%A5%E9%AA%A4%E4%B8%8E%E9%A2%84%E6%9C%9F%E7%BB%93%E6%9E%9C)
    - [1. 启动前准备](#1-%E5%90%AF%E5%8A%A8%E5%89%8D%E5%87%86%E5%A4%87)
    - [2. 启动应用](#2-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [3. 执行测试](#3-%E6%89%A7%E8%A1%8C%E6%B5%8B%E8%AF%95)
      - [步骤1：查看行为说明](#%E6%AD%A5%E9%AA%A41%E6%9F%A5%E7%9C%8B%E8%A1%8C%E4%B8%BA%E8%AF%B4%E6%98%8E)
      - [步骤2：批量发送消息](#%E6%AD%A5%E9%AA%A42%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF)
      - [步骤3：发送单条消息](#%E6%AD%A5%E9%AA%A43%E5%8F%91%E9%80%81%E5%8D%95%E6%9D%A1%E6%B6%88%E6%81%AF)
      - [步骤4：重置偏移量](#%E6%AD%A5%E9%AA%A44%E9%87%8D%E7%BD%AE%E5%81%8F%E7%A7%BB%E9%87%8F)
    - [4. 查看消费日志](#4-%E6%9F%A5%E7%9C%8B%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
  - [生产环境建议](#%E7%94%9F%E4%BA%A7%E7%8E%AF%E5%A2%83%E5%BB%BA%E8%AE%AE)
  - [总结](#%E6%80%BB%E7%BB%93)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# offset偏移量行为

## 目标

演示不同 `auto.offset.reset` 策略下（latest/earliest）以及手动提交偏移量时的消费行为差异，并提供通过 Admin API
重置消费者组偏移量的示例。

## 重要Topic与消费者组

- **Topic**: `offset-demo-topic`
- **消费者组**：
    - `demo-group-latest` - 最新偏移量策略
    - `demo-group-earliest` - 最早偏移量策略
    - `demo-group-manual` - 手动偏移量控制

## 核心概念

### 偏移量策略对比

| 策略         | 行为                       | 适用场景              |
|------------|--------------------------|-------------------|
| `latest`   | 新消费者组从最新位置开始消费，不会收到历史消息  | 实时数据处理，不关心历史消息    |
| `earliest` | 新消费者组从最早位置开始消费，会收到所有历史消息 | 需要处理所有数据，包括历史消息   |
| `manual`   | 手动控制偏移量提交，实现精确的消费控制      | 需要确保消息处理成功后才提交偏移量 |

### 重要说明

⚠️ **注意**：如果消费者组已有提交的偏移量，`auto.offset.reset` 配置将不会生效。只有新消费者组或没有已提交偏移量的消费者组才会使用此配置。

---

## 技术实现

### 1. 项目依赖

`pom.xml`：引入kafka依赖

```xml

<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
</dependency>
<dependency>
<groupId>org.springframework.boot</groupId>
<artifactId>spring-boot-starter-web</artifactId>
</dependency>
<dependency>
<groupId>org.springframework.kafka</groupId>
<artifactId>spring-kafka</artifactId>
</dependency>
<dependency>
<groupId>org.projectlombok</groupId>
<artifactId>lombok</artifactId>
<version>1.18.32</version>
<optional>true</optional>
</dependency>
```

### 2. 应用配置

`application.yml`：添加kafka配置

```yml
server:
  port: 8091

spring:
  application:
    name: kafka-03-offset
  kafka:
    # Kafka服务器地址，多个用逗号分隔
    bootstrap-servers: 192.168.56.10:9092

    # 生产者配置
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: 1 # 消息确认机制

    # 消费者配置
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false # 禁用自动提交，使用手动控制

logging:
  level:
    org.apache.kafka.clients.NetworkClient: error
```

### 3. 常量定义

`config/KafkaConstants.java` 统一管理 Topic 名称、分区、副本等常量

```java
package com.action.kafka03offset.config;

/**
 * Kafka常量定义类
 * 统一管理Topic名称、消费者组、分区和副本等配置常量
 */
public class KafkaConstants {

    /** 演示Topic名称 */
    public static final String TOPIC_DEMO = "offset-demo-topic";

    /** 最新偏移量策略消费者组 */
    public static final String CONSUMER_GROUP_LATEST = "demo-group-latest";
    /** 最早偏移量策略消费者组 */
    public static final String CONSUMER_GROUP_EARLIEST = "demo-group-earliest";
    /** 手动偏移量控制消费者组 */
    public static final String CONSUMER_GROUP_MANUAL = "demo-group-manual";

    /** Topic分区数量 */
    public static final int DEMO_TOPIC_PARTITIONS = 2;
    /** Topic副本因子 */
    public static final short DEMO_TOPIC_REPLICATION_FACTOR = 1;
}
```

### 4. Topic 自动创建

`config/KafkaTopicConfig.java` 应用启动时自动创建 Topic，避免手动命令行创建

```java
package com.action.kafka03offset.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka Topic配置类
 * 应用启动时自动创建Topic，避免手动命令行创建
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 创建演示Topic
     * 使用TopicBuilder构建Topic配置
     * 应用启动时会自动创建该Topic
     */
    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(KafkaConstants.TOPIC_DEMO)
                .partitions(KafkaConstants.DEMO_TOPIC_PARTITIONS)
                .replicas(KafkaConstants.DEMO_TOPIC_REPLICATION_FACTOR)
                .build();
    }
}
```

### 5. 消费者配置

`config/CustomConsumerConfig.java` 演示不同偏移量策略的消费者配置

```java
package com.action.kafka03offset.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Kafka消费者配置类
 * 演示不同偏移量策略的消费者配置：
 * 1. latestOffsetFactory - 从最新位置开始消费（默认策略）
 * 2. earliestOffsetFactory - 从最早位置开始消费
 * 3. manualOffsetFactory - 手动控制偏移量提交
 */
@Configuration
public class CustomConsumerConfig {

    /**
     * 最新偏移量消费者工厂
     * 使用默认的auto.offset.reset=latest策略
     * 新消费者组从最新位置开始消费，不会收到历史消息
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> latestOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * 最早偏移量消费者工厂
     * 显式设置auto.offset.reset=earliest策略
     * 新消费者组从最早位置开始消费，会收到所有历史消息
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> earliestOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {

        // 复制原有配置并添加earliest策略
        Map<String, Object> props = new HashMap<>();
        props.putAll(consumerFactory.getConfigurationProperties());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建新的消费者工厂
        ConsumerFactory<String, String> earliestConsumerFactory =
                new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(earliestConsumerFactory);
        return factory;
    }

    /**
     * 手动偏移量消费者工厂
     * 设置手动立即提交偏移量模式
     * 允许精确控制消息处理完成后再提交偏移量
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 设置手动立即提交模式，需要手动调用ack.acknowledge()
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
```

### 6. 消息生产者

`producer/KafkaProducer.java` 通过 KafkaTemplate 异步发送消息到指定 Topic

```java
package com.action.kafka03offset.producer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka消息生产者
 * 通过KafkaTemplate异步发送消息到指定Topic
 * 支持单条消息发送和批量消息发送
 */
@Service
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送单条演示消息
     * 使用异步方式发送，通过回调处理发送结果
     *
     * @param message 要发送的消息内容
     */
    public void sendDemoMessage(String message) {
        log.info("发送演示消息: {}", message);

        // 异步发送消息，使用whenComplete处理结果
        kafkaTemplate.send(KafkaConstants.TOPIC_DEMO, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        // 发送成功，记录分区和偏移量信息
                        log.info("✅ 消息发送成功: partition={}, offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        // 发送失败，记录错误信息
                        log.error("❌ 消息发送失败: {}", ex.getMessage());
                    }
                });
    }

    /**
     * 批量发送测试消息
     * 用于演示不同偏移量策略的消费行为
     *
     * @param count 要发送的消息数量
     */
    public void sendBatchMessages(int count) {
        log.info("开始批量发送 {} 条测试消息", count);
        for (int i = 1; i <= count; i++) {
            // 生成带时间戳的测试消息
            String message = "测试消息-" + i + "-" + System.currentTimeMillis();
            kafkaTemplate.send(KafkaConstants.TOPIC_DEMO, message);
        }
        log.info("✅ 批量消息发送完成");
    }
}
```

### 7. 消息消费者

#### 最新偏移量消费者

`consumer/LatestOffsetConsumer.java` 使用 `@KafkaListener` 注解监听 Topic，从最新位置开始消费

```java
package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 最新偏移量策略消费者
 * 使用latestOffsetFactory，从最新位置开始消费
 * 新消费者组不会收到历史消息，只消费启动后的新消息
 */
@Component
@Slf4j
public class LatestOffsetConsumer {

    /**
     * 消费最新偏移量策略的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用latestOffsetFactory配置
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_LATEST,
            containerFactory = "latestOffsetFactory"
    )
    public void consumeLatest(String message) {
        log.info("🔵 [LATEST组] 消费消息: {}", message);
    }
}
```

#### 最早偏移量消费者

`consumer/EarliestOffsetConsumer.java` 从最早位置开始消费，会收到所有历史消息

```java
package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 最早偏移量策略消费者
 * 使用earliestOffsetFactory，从最早位置开始消费
 * 新消费者组会收到所有历史消息，包括启动前已存在的消息
 */
@Component
@Slf4j
public class EarliestOffsetConsumer {

    /**
     * 消费最早偏移量策略的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用earliestOffsetFactory配置
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_EARLIEST,
            containerFactory = "earliestOffsetFactory"
    )
    public void consumeEarliest(String message) {
        log.info("🟢 [EARLIEST组] 消费消息: {}", message);
    }
}
```

#### 手动偏移量消费者

`consumer/ManualOffsetConsumer.java` 手动控制偏移量提交，确保消息处理成功后才提交

```java
package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 手动偏移量控制消费者
 * 使用manualOffsetFactory，手动控制偏移量提交
 * 只有在消息处理成功后才提交偏移量，确保消息不丢失
 */
@Component
@Slf4j
public class ManualOffsetConsumer {

    /**
     * 消费手动偏移量控制的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用manualOffsetFactory配置
     * Acknowledgment参数用于手动提交偏移量
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_MANUAL,
            containerFactory = "manualOffsetFactory"
    )
    public void consumeManual(String message, Acknowledgment ack) {
        try {
            log.info("🟠 [MANUAL组] 消费消息: {}", message);
            // 处理消息
            processMessage(message);
            // 消息处理成功后手动提交偏移量
            ack.acknowledge();
            log.info("✅ 偏移量已提交");
        } catch (Exception e) {
            log.error("❌ 消息处理失败: {}", e.getMessage());
            // 处理失败时不提交偏移量，消息会被重新消费
        }
    }

    /**
     * 模拟消息处理逻辑
     * 如果消息包含"error"关键字，则抛出异常模拟处理失败
     */
    private void processMessage(String message) {
        if (message.contains("error")) {
            throw new RuntimeException("模拟业务处理异常");
        }
    }
}
```

### 8. 偏移量重置服务

`service/OffsetResetService.java` 通过 Admin API 重置消费者组偏移量

```java
package com.action.kafka03offset.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 偏移量重置服务
 * 通过Kafka Admin API重置消费者组的偏移量
 * 支持重置到earliest或latest位置
 */
@Service
@Slf4j
public class OffsetResetService {

    private final KafkaAdmin kafkaAdmin;

    public OffsetResetService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * 重置消费者组偏移量
     * 使用Admin API将指定消费者组的偏移量重置到指定位置
     *
     * @param groupId 消费者组ID
     * @param topic Topic名称
     * @param resetTo 重置目标位置（earliest/latest）
     */
    public void resetConsumerOffset(String groupId, String topic, String resetTo) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {

            // 获取Topic的所有分区
            Set<TopicPartition> partitions = getTopicPartitions(adminClient, topic);
            if (partitions.isEmpty()) {
                throw new RuntimeException("Topic " + topic + " 不存在或没有分区");
            }

            // 准备新的偏移量映射
            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();

            // 根据重置策略获取目标偏移量
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets;
            if ("earliest".equalsIgnoreCase(resetTo)) {
                // 重置到最早位置
                Map<TopicPartition, OffsetSpec> spec = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
                offsets = adminClient.listOffsets(spec).all().get(10, TimeUnit.SECONDS);
            } else if ("latest".equalsIgnoreCase(resetTo)) {
                // 重置到最新位置
                Map<TopicPartition, OffsetSpec> spec = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
                offsets = adminClient.listOffsets(spec).all().get(10, TimeUnit.SECONDS);
            } else {
                throw new IllegalArgumentException("不支持的resetTo参数: " + resetTo);
            }

            // 构建新的偏移量映射
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : offsets.entrySet()) {
                newOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
            }

            // 执行偏移量重置
            adminClient.alterConsumerGroupOffsets(groupId, newOffsets).all().get(10, TimeUnit.SECONDS);
            log.info("✅ 消费者组 {} 的偏移量已重置到 {}", groupId, resetTo);
        } catch (Exception e) {
            log.error("❌ 重置消费者组偏移量失败: {}", e.getMessage());
            throw new RuntimeException("重置失败", e);
        }
    }

    /**
     * 获取Topic的所有分区信息
     *
     * @param adminClient Admin客户端
     * @param topic Topic名称
     * @return 分区集合
     * @throws Exception 异常
     */
    private Set<TopicPartition> getTopicPartitions(AdminClient adminClient, String topic) throws Exception {
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singleton(topic));
        TopicDescription topicDescription = topicsResult.topicNameValues().get(topic).get();
        return topicDescription.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toSet());
    }
}
```

### 9. REST 控制器

`controller/KafkaController.java` 提供 REST 接口用于测试不同偏移量策略的消费行为

```java
package com.action.kafka03offset.controller;

import com.action.kafka03offset.producer.KafkaProducer;
import com.action.kafka03offset.service.OffsetResetService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka演示控制器
 * 提供REST接口用于测试不同偏移量策略的消费行为
 * 所有接口都使用GET方法，简化测试
 */
@RestController
@RequestMapping("/api/kafka/demo")
@Slf4j
public class KafkaController {

    private final KafkaProducer kafkaProducer;
    private final OffsetResetService offsetResetService;

    public KafkaController(KafkaProducer kafkaProducer, OffsetResetService offsetResetService) {
        this.kafkaProducer = kafkaProducer;
        this.offsetResetService = offsetResetService;
    }

    /**
     * 发送单条消息接口
     * 用于测试不同消费者组的消费行为
     *
     * @param message 要发送的消息内容
     * @return 发送结果
     */
    @GetMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        kafkaProducer.sendDemoMessage(message);
        return ResponseEntity.ok("消息已发送: " + message);
    }

    /**
     * 批量发送消息接口
     * 用于生成测试数据，演示不同偏移量策略
     *
     * @param count 要发送的消息数量，默认10条
     * @return 发送结果
     */
    @GetMapping("/send-batch")
    public ResponseEntity<String> sendBatchMessages(@RequestParam(defaultValue = "10") int count) {
        kafkaProducer.sendBatchMessages(count);
        return ResponseEntity.ok("已发送 " + count + " 条测试消息");
    }

    /**
     * 重置消费者组偏移量接口
     * 通过Admin API重置指定消费者组的偏移量
     *
     * @param groupId 消费者组ID
     * @param topic Topic名称
     * @param resetTo 重置目标位置（earliest/latest）
     * @return 重置结果
     */
    @GetMapping("/reset-offset")
    public ResponseEntity<String> resetOffset(@RequestParam String groupId,
                                              @RequestParam String topic,
                                              @RequestParam(defaultValue = "earliest") String resetTo) {
        try {
            offsetResetService.resetConsumerOffset(groupId, topic, resetTo);
            return ResponseEntity.ok("消费者组 " + groupId + " 偏移量已重置到 " + resetTo);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("重置失败: " + e.getMessage());
        }
    }

}
```

---

## API 接口测试

### 关键端点

所有接口都使用 GET 方法，可以直接在浏览器中测试：

- **发送单条消息**：
  ```
  http://localhost:8091/api/kafka/demo/send?message=hello
  ```

- **批量发送消息**：
  ```
  http://localhost:8091/api/kafka/demo/send-batch?count=5
  ```

- **重置偏移量**：
  ```
  http://localhost:8091/api/kafka/demo/reset-offset?groupId=demo-group-latest&topic=offset-demo-topic&resetTo=earliest
  ```

### Windows 11 测试方法

#### 方法1：浏览器直接测试（最简单）

直接在浏览器地址栏输入上述URL即可测试。

#### 方法2：PowerShell 测试

```powershell
# 发送单条消息
Invoke-RestMethod -Uri "http://localhost:8091/api/kafka/demo/send?message=hello"

# 批量发送消息
Invoke-RestMethod -Uri "http://localhost:8091/api/kafka/demo/send-batch?count=5"

# 重置偏移量
Invoke-RestMethod -Uri "http://localhost:8091/api/kafka/demo/reset-offset?groupId=demo-group-latest&topic=offset-demo-topic&resetTo=earliest"

# 查看行为说明
Invoke-RestMethod -Uri "http://localhost:8091/api/kafka/demo/behavior"
```

#### 方法3：curl 命令测试

```bash
# 发送单条消息
curl "http://localhost:8091/api/kafka/demo/send?message=hello"

# 批量发送消息
curl "http://localhost:8091/api/kafka/demo/send-batch?count=5"

# 重置偏移量
curl "http://localhost:8091/api/kafka/demo/reset-offset?groupId=demo-group-latest&topic=offset-demo-topic&resetTo=earliest"
```

---

## 演示步骤与预期结果

### 1. 启动前准备

确保 Kafka 服务运行，`offset-demo-topic` 不存在或为空。

### 2. 启动应用

```bash
mvn spring-boot:run
```

应用启动时会自动创建 Topic。

### 3. 执行测试

#### 步骤1：查看行为说明

```
http://localhost:8091/api/kafka/demo/behavior
```

#### 步骤2：批量发送消息

```
http://localhost:8091/api/kafka/demo/send-batch?count=5
```

**预期结果**：

- `latest组`：不会消费历史消息（从最新位置）
- `earliest组`：会消费全部历史消息（从最早位置）
- `manual组`：按手动提交逻辑处理

#### 步骤3：发送单条消息

```
http://localhost:8091/api/kafka/demo/send?message=新消息
```

**预期结果**：三个组都会消费（已有提交偏移量）

#### 步骤4：重置偏移量

```
http://localhost:8091/api/kafka/demo/reset-offset?groupId=demo-group-latest&topic=offset-demo-topic&resetTo=earliest
```

**预期结果**：`demo-group-latest` 会重新消费历史消息

### 4. 查看消费日志

控制台可见生产和消费的详细日志：

```
c.a.k.producer.KafkaProducer             : 发送演示消息: 测试消息-1-1703123456789
c.a.k.producer.KafkaProducer             : ✅ 消息发送成功: partition=0, offset=0
c.a.k.consumer.LatestOffsetConsumer      : 🔵 [LATEST组] 消费消息: 测试消息-1-1703123456789
c.a.k.consumer.EarliestOffsetConsumer    : 🟢 [EARLIEST组] 消费消息: 测试消息-1-1703123456789
c.a.k.consumer.ManualOffsetConsumer      : 🟠 [MANUAL组] 消费消息: 测试消息-1-1703123456789
c.a.k.consumer.ManualOffsetConsumer      : ✅ 偏移量已提交
```

---

## 生产环境建议

1. **明确设置偏移策略**：根据业务需求选择合适的 `auto.offset.reset` 策略
2. **监控消费者滞后**：使用 Kafka 监控工具监控消费者组的消费进度
3. **建立偏移重置流程**：制定标准化的偏移量重置流程和审批机制
4. **消息幂等性**：确保消息处理逻辑的幂等性，避免重复消费造成的问题
5. **错误处理**：建立完善的错误处理和重试机制

---

## 总结

本模块通过实际代码演示了 Kafka 中不同偏移量策略的行为差异：

- **latest 策略**：适合实时数据处理场景
- **earliest 策略**：适合需要处理所有数据的场景
- **manual 策略**：适合需要精确控制消息处理的场景

通过 Admin API 可以动态重置消费者组偏移量，为生产环境提供了灵活的数据重放能力。
