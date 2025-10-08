<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [消费者事件消费](#%E6%B6%88%E8%B4%B9%E8%80%85%E4%BA%8B%E4%BB%B6%E6%B6%88%E8%B4%B9)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. Kafka消费者配置类](#3-kafka%E6%B6%88%E8%B4%B9%E8%80%85%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [4. 简单消费者](#4-%E7%AE%80%E5%8D%95%E6%B6%88%E8%B4%B9%E8%80%85)
    - [5. 批量消费者](#5-%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85)
    - [6. 手动提交消费者](#6-%E6%89%8B%E5%8A%A8%E6%8F%90%E4%BA%A4%E6%B6%88%E8%B4%B9%E8%80%85)
  - [Kafka Consumer消费模式详解](#kafka-consumer%E6%B6%88%E8%B4%B9%E6%A8%A1%E5%BC%8F%E8%AF%A6%E8%A7%A3)
    - [1. 消费者类型说明](#1-%E6%B6%88%E8%B4%B9%E8%80%85%E7%B1%BB%E5%9E%8B%E8%AF%B4%E6%98%8E)
    - [2. 消费模式对比](#2-%E6%B6%88%E8%B4%B9%E6%A8%A1%E5%BC%8F%E5%AF%B9%E6%AF%94)
    - [3. 消费者配置说明](#3-%E6%B6%88%E8%B4%B9%E8%80%85%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
      - [自动提交模式配置](#%E8%87%AA%E5%8A%A8%E6%8F%90%E4%BA%A4%E6%A8%A1%E5%BC%8F%E9%85%8D%E7%BD%AE)
      - [手动提交模式配置](#%E6%89%8B%E5%8A%A8%E6%8F%90%E4%BA%A4%E6%A8%A1%E5%BC%8F%E9%85%8D%E7%BD%AE)
    - [4. 消费者组说明](#4-%E6%B6%88%E8%B4%B9%E8%80%85%E7%BB%84%E8%AF%B4%E6%98%8E)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 发送测试消息](#2-%E5%8F%91%E9%80%81%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [2.1 简单消费者测试](#21-%E7%AE%80%E5%8D%95%E6%B6%88%E8%B4%B9%E8%80%85%E6%B5%8B%E8%AF%95)
      - [2.2 批量消费者测试](#22-%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85%E6%B5%8B%E8%AF%95)
      - [2.3 手动提交消费者测试](#23-%E6%89%8B%E5%8A%A8%E6%8F%90%E4%BA%A4%E6%B6%88%E8%B4%B9%E8%80%85%E6%B5%8B%E8%AF%95)
      - [2.4 通用测试命令](#24-%E9%80%9A%E7%94%A8%E6%B5%8B%E8%AF%95%E5%91%BD%E4%BB%A4)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [简单消费者日志](#%E7%AE%80%E5%8D%95%E6%B6%88%E8%B4%B9%E8%80%85%E6%97%A5%E5%BF%97)
      - [批量消费者日志](#%E6%89%B9%E9%87%8F%E6%B6%88%E8%B4%B9%E8%80%85%E6%97%A5%E5%BF%97)
      - [手动提交消费者日志](#%E6%89%8B%E5%8A%A8%E6%8F%90%E4%BA%A4%E6%B6%88%E8%B4%B9%E8%80%85%E6%97%A5%E5%BF%97)
  - [测试命令快速参考](#%E6%B5%8B%E8%AF%95%E5%91%BD%E4%BB%A4%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [消费者类型与测试命令对应关系](#%E6%B6%88%E8%B4%B9%E8%80%85%E7%B1%BB%E5%9E%8B%E4%B8%8E%E6%B5%8B%E8%AF%95%E5%91%BD%E4%BB%A4%E5%AF%B9%E5%BA%94%E5%85%B3%E7%B3%BB)
    - [特殊测试场景](#%E7%89%B9%E6%AE%8A%E6%B5%8B%E8%AF%95%E5%9C%BA%E6%99%AF)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 消费者事件消费

## 项目作用

本项目演示了SpringBoot中Kafka Consumer的多种消费模式实现和使用，包括简单消费者、批量消费者、手动提交消费者等不同类型，帮助开发者理解Kafka消费者的工作原理和实际应用场景。

## 项目结构

```
kafka-08-Event-Consumer/
├── src/main/java/com/action/kafka08eventconsumer/
│   ├── config/
│   │   └── KafkaConsumerConfig.java              # Kafka消费者配置类
│   ├── consumer/
│   │   ├── SimpleConsumer.java                   # 简单消费者
│   │   ├── BatchConsumer.java                    # 批量消费者
│   │   └── ManualCommitConsumer.java             # 手动提交消费者
│   └── Kafka08EventConsumerApplication.java      # 主启动类
├── src/main/resources/
│   └── application.properties                    # 配置文件
└── pom.xml                                       # Maven配置
```

## 核心实现

### 1. 依赖配置

`pom.xml`：引入Kafka相关依赖

```xml

<dependencies>
    <!-- Spring Boot Starter -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
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
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.properties`：Kafka服务器和消费者配置

```properties
# ========================================
# Kafka 事件消费者演示应用配置
# ========================================
# 应用名称
spring.application.name=kafka-08-Event-Consumer
# ========================================
# Kafka 连接配置
# ========================================
# Kafka 服务器地址（多个地址用逗号分隔）
# 默认：192.168.56.10:9092
spring.kafka.bootstrap-servers=192.168.56.10:9092
# ========================================
# 演示 Topic 配置
# ========================================
# 演示用的 Topic 名称
# 应用启动时会自动创建该 Topic（3个分区，1个副本）
demo.topic.name=demo-consumer-topic
# ========================================
# 消费者配置
# ========================================
# 消费者组ID：用于标识消费者组
spring.kafka.consumer.group-id=demo-consumer-group
# 键反序列化器：将字节数组反序列化为字符串
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# 值反序列化器：将字节数组反序列化为字符串
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# 自动提交偏移量：true表示自动提交，false表示手动提交
spring.kafka.consumer.enable-auto-commit=true
# 自动提交间隔：自动提交模式下的提交间隔（毫秒）
spring.kafka.consumer.auto-commit-interval=1000
# 偏移量重置策略：earliest（从最早开始）、latest（从最新开始）、none（无偏移量时抛出异常）
spring.kafka.consumer.auto-offset-reset=earliest
# 会话超时时间：消费者与协调器失去连接的超时时间（毫秒）
spring.kafka.consumer.session-timeout=30000
# 心跳间隔：消费者发送心跳的间隔时间（毫秒）
spring.kafka.consumer.heartbeat-interval=3000
# 最大拉取记录数：单次拉取的最大记录数
spring.kafka.consumer.max-poll-records=500
# 拉取超时时间：拉取数据的超时时间（毫秒）
spring.kafka.consumer.fetch-max-wait=500
# ========================================
# 监听器配置
# ========================================
# 监听器确认模式：batch（批量确认）、manual（手动确认）、manual_immediate（立即手动确认）
spring.kafka.listener.ack-mode=batch
# 并发消费者数量：每个监听器容器的并发消费者数量
spring.kafka.listener.concurrency=1
# 监听器容器类型：single（单线程）、batch（批处理）
spring.kafka.listener.type=single
```

### 3. Kafka消费者配置类

`config/KafkaConsumerConfig.java`：配置多种消费者模式和监听器容器工厂

```java
package com.action.kafka08eventconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消费者配置类
 *
 * 功能说明：
 * 1) 配置消费者基本参数：反序列化器、服务器地址、消费者组等
 * 2) 配置多种监听器容器工厂：支持不同的消费模式
 * 3) 创建演示用的 Topic
 * 4) 支持自动提交和手动提交两种模式
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:192.168.56.10:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:demo-consumer-group}")
    private String groupId;

    @Value("${demo.topic.name:demo-consumer-topic}")
    private String demoTopic;

    /**
     * 消费者配置参数（自动提交模式）
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 偏移量配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        // 性能配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        return props;
    }

    /**
     * 消费者配置参数（手动提交模式）
     */
    @Bean
    public Map<String, Object> manualCommitConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-manual");

        // 偏移量配置（手动提交）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 性能配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        return props;
    }

    /**
     * 消费者工厂（自动提交模式）
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * 消费者工厂（手动提交模式）
     */
    @Bean
    public ConsumerFactory<String, String> manualCommitConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(manualCommitConsumerConfigs());
    }

    /**
     * 监听器容器工厂（自动提交模式）
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);
        return factory;
    }

    /**
     * 监听器容器工厂（手动提交模式）
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualCommitKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(manualCommitConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(1);
        return factory;
    }

    /**
     * 监听器容器工厂（批量消费模式）
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);
        return factory;
    }

    /**
     * 演示用 Topic Bean
     */
    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(demoTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
```

### 4. 简单消费者

`consumer/SimpleConsumer.java`：演示基本的消息消费功能

```java
package com.action.kafka08eventconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 简单消费者 - 演示基本的消息消费功能
 *
 * 功能说明：
 * 1) 自动提交模式：消费消息后自动提交偏移量
 * 2) 单条消费：一次处理一条消息
 * 3) 日志记录：记录消费的消息详情
 * 4) 异常处理：捕获并记录消费异常
 */
@Component
public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    /**
     * 简单消息消费方法（使用 ConsumerRecord）
     *
     * 执行时机：当有消息到达指定 topic 时
     * 作用：处理单条消息，记录消费详情
     *
     * 参数说明：
     * - topics: 监听的 topic 名称
     * - groupId: 消费者组ID
     * - containerFactory: 使用的监听器容器工厂
     *
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * # 发送测试消息示例
     * echo "Hello Kafka Consumer" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "key1:test message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * @param record 消息记录对象
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(ConsumerRecord<String, String> record) {
        try {
            // 消费信息日志
            log.info("📦 Simple Consumer (ConsumerRecord) 👥 Group: {} 📋 Topic: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} ⏰ Timestamp: {} 📄 Headers: {}", 
                    groupId, record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), record.headers());

            // 模拟消息处理逻辑
            processMessage(record.value());

        } catch (Exception ex) {
            log.error("Error processing message from topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), ex);
        }
    }

    /**
     * 简单消息消费方法（使用 @Payload 和 @Header）
     *
     * 执行时机：当有消息到达指定 topic 时
     * 作用：使用注解方式简化参数处理
     *
     * 参数说明：
     * - @Payload: 直接获取消息值，类型自动转换
     * - @Header: 获取指定的消息头信息
     *
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * # 发送测试消息示例
     * echo "Payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "payload-key:Payload test with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * @param message 消息值
     * @param partition 分区号
     * @param offset 偏移量
     * @param key 消息键
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-payload",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessageWithPayload(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        try {
            // 消费信息日志
            log.info("📦 Simple Consumer (@Payload/@Header) 👥 Group: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} 🔄 Acknowledgment: Auto", 
                    groupId + "-payload", partition, offset, key != null ? key : "null", message);

            processMessage(message);

        } catch (Exception ex) {
            log.error("Error processing message from partition={}, offset={}",
                    partition, offset, ex);
        }
    }

    /**
     * 消息处理逻辑
     */
    private void processMessage(String message) {
        try {
            Thread.sleep(100);

            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
            } else {
                log.info("Processing normal message: {}", message);
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
        }
    }
}
```

### 5. 批量消费者

`consumer/BatchConsumer.java`：演示批量消息消费功能

```java
package com.action.kafka08eventconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 批量消费者 - 演示批量消息消费功能
 *
 * 功能说明：
 * 1) 批量消费：一次处理多条消息，提高消费效率
 * 2) 自动提交模式：消费完成后自动提交偏移量
 * 3) 批量处理：适合高吞吐量场景
 * 4) 统计信息：记录批量消费的统计信息
 */
@Component
public class BatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);

    // 批量消费统计信息
    private long totalBatches = 0;
    private long totalMessages = 0;

    /**
     * 批量消息消费方法（使用 List<ConsumerRecord>）
     *
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：批量处理消息，提高消费效率
     *
     * 参数说明：
     * - topics: 监听的 topic 名称
     * - groupId: 消费者组ID
     * - containerFactory: 使用批量消费的监听器容器工厂
     *
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..5}; do echo "batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     *
     * # 发送批量消息（带键）
     * for i in {1..3}; do echo "batch-key-$i:batch message with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     *
     * # 快速发送多条消息测试批量消费
     * for i in {1..10}; do echo "rapid message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.1; done
     *
     * @param records 批量消息记录列表
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-batch",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records) {
        try {
            totalBatches++;
            totalMessages += records.size();

            // 批量消费信息日志
            log.info("📦 Batch Consumer (List<ConsumerRecord>) 👥 Group: {} 📊 Batch size: {} 📈 Total batches: {} 📈 Total messages: {}", 
                    groupId + "-batch", records.size(), totalBatches, totalMessages);

            processBatchMessages(records);

            log.info("==========================================");

        } catch (Exception ex) {
            log.error("Error processing batch messages, batch size: {}", records.size(), ex);
        }
    }

    /**
     * 批量消息消费方法（使用 List<String>）
     *
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：使用注解方式批量处理消息
     *
     * 参数说明：
     * - @Payload: 批量消息值列表
     * - @Header: 批量消息的公共头信息
     *
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..8}; do echo "payload batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     *
     * # 发送批量消息（带键）
     * for i in {1..4}; do echo "payload-key-$i:payload batch with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     *
     * # 混合发送测试（部分有键，部分无键）
     * echo "mixed message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "mixed-key:mixed message with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * echo "mixed message 3" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * @param messages 批量消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-batch-payload",
            containerFactory = "manualBatchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessagesWithPayload(
            @Payload List<String> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) List<String> keys) {

        try {
            totalBatches++;
            totalMessages += messages.size();

            // 批量消费信息日志
            log.info("📦 Batch Consumer (@Payload/@Header) 👥 Group: {} 📊 Batch size: {} 🔢 Partitions: {} 📍 Offsets: {} 🔑 Keys: {} 📈 Total batches: {} 📈 Total messages: {}", 
                    groupId + "-batch-payload", messages.size(), partitions, offsets, keys, totalBatches, totalMessages);

            processBatchMessagesWithDetails(messages, partitions, offsets, keys);

            log.info("=========================================");

        } catch (Exception ex) {
            log.error("Error processing batch messages, batch size: {}", messages.size(), ex);
        }
    }

    /**
     * 处理批量消息（使用 ConsumerRecord）
     */
    private void processBatchMessages(List<ConsumerRecord<String, String>> records) {
        try {
            records.stream()
                    .collect(java.util.stream.Collectors.groupingBy(ConsumerRecord::partition))
                    .forEach((partition, partitionRecords) -> {
                        log.info("Processing {} messages from partition {}",
                                partitionRecords.size(), partition);

                        partitionRecords.forEach(record -> {
                            log.debug("Message from partition {}: key={}, value={}, offset={}",
                                    record.partition(), record.key(), record.value(), record.offset());
                            processMessage(record.value());
                        });
                    });

            Thread.sleep(200);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
        }
    }

    /**
     * 处理批量消息（使用详细信息）
     */
    private void processBatchMessagesWithDetails(List<String> messages,
                                                 List<Integer> partitions,
                                                 List<Long> offsets,
                                                 List<String> keys) {
        try {
            for (int i = 0; i < messages.size(); i++) {
                String message = messages.get(i);
                Integer partition = partitions.get(i);
                Long offset = offsets.get(i);
                String key = keys.get(i);

                log.debug("Message from partition {}: key={}, value={}, offset={}",
                        partition, key, message, offset);

                processMessage(message);
            }

            Thread.sleep(200);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
        }
    }

    /**
     * 消息处理逻辑
     */
    private void processMessage(String message) {
        try {
            Thread.sleep(50);

            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
            } else {
                log.debug("Processing normal message: {}", message);
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
        }
    }

    /**
     * 获取批量消费统计信息
     */
    public String getStatistics() {
        return String.format("Batch Consumer Statistics - Total Batches: %d, Total Messages: %d",
                totalBatches, totalMessages);
    }
}
```

### 6. 手动提交消费者

`consumer/ManualCommitConsumer.java`：演示手动提交偏移量的消息消费功能

```java
package com.action.kafka08eventconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 手动提交消费者 - 演示手动提交偏移量的消息消费功能
 *
 * 功能说明：
 * 1) 手动提交模式：消费消息后需要手动确认偏移量
 * 2) 精确控制：可以控制何时提交偏移量，避免消息丢失
 * 3) 异常处理：支持消费失败时的重试和回滚机制
 * 4) 批量确认：支持批量处理后的批量确认
 */
@Component
public class ManualCommitConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);

    // 手动提交统计信息
    private long totalMessages = 0;
    private long successMessages = 0;
    private long failedMessages = 0;

    /**
     * 手动提交消息消费方法（使用 ConsumerRecord）
     *
     * 执行时机：当有消息到达指定 topic 时
     * 作用：手动控制偏移量提交，确保消息处理完成后再提交
     *
     * 参数说明：
     * - topics: 监听的 topic 名称
     * - groupId: 消费者组ID
     * - containerFactory: 使用手动提交的监听器容器工厂
     *
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * # 发送测试消息示例
     * echo "Manual commit test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "manual-key:Manual commit with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * @param record 消息记录对象
     * @param ack 手动确认接口
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeMessageWithManualCommit(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            totalMessages++;

            // 手动提交消费信息日志
            log.info("📦 Manual Commit Consumer (ConsumerRecord) 👥 Group: {} 📋 Topic: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} ⏰ Timestamp: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual", record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), totalMessages, successMessages, failedMessages);

            boolean success = processMessage(record.value());

            if (success) {
                ack.acknowledge();
                successMessages++;
                log.info("Message acknowledged successfully");
            } else {
                failedMessages++;
                log.warn("Message processing failed, will be retried");
            }

            log.info("=============================================");

        } catch (Exception ex) {
            failedMessages++;
            log.error("Error processing message from topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), ex);
        }
    }

    /**
     * 手动提交消息消费方法（使用 @Payload 和 @Header）
     *
     * 执行时机：当有消息到达指定 topic 时
     * 作用：使用注解方式简化参数处理，手动控制偏移量提交
     *
     * 参数说明：
     * - @Payload: 直接获取消息值，类型自动转换
     * - @Header: 获取指定的消息头信息
     * - Acknowledgment: 手动确认接口
     *
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * # 发送测试消息示例
     * echo "Manual payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "manual-payload-key:Manual payload with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     *
     * @param message 消息值
     * @param partition 分区号
     * @param offset 偏移量
     * @param key 消息键
     * @param ack 手动确认接口
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual-payload",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeMessageWithManualCommitPayload(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            Acknowledgment ack) {

        try {
            totalMessages++;

            // 手动提交消费信息日志
            log.info("📦 Manual Commit Consumer (@Payload/@Header) 👥 Group: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual-payload", partition, offset, key, message, totalMessages, successMessages, failedMessages);

            boolean success = processMessage(message);

            if (success) {
                ack.acknowledge();
                successMessages++;
                log.info("Message acknowledged successfully");
            } else {
                failedMessages++;
                log.warn("Message processing failed, will be retried");
            }

            log.info("===============================================");

        } catch (Exception ex) {
            failedMessages++;
            log.error("Error processing message from partition={}, offset={}",
                    partition, offset, ex);
        }
    }

    /**
     * 手动提交批量消息消费方法
     *
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：批量处理消息，手动控制偏移量提交
     *
     * 参数说明：
     * - @Payload: 批量消息值列表
     * - @Header: 批量消息的公共头信息
     * - Acknowledgment: 手动确认接口
     *
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..6}; do echo "manual batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     *
     * # 发送批量消息（带键）
     * for i in {1..4}; do echo "manual-batch-key-$i:manual batch with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     *
     * # 快速发送多条消息测试批量手动提交
     * for i in {1..15}; do echo "rapid manual batch $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.05; done
     *
     * # 发送包含错误消息的批次测试
     * echo "error message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "normal message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     *
     * @param messages 批量消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     * @param ack 手动确认接口
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual-batch",
            containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeBatchMessagesWithManualCommit(
            @Payload List<String> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) List<String> keys,
            Acknowledgment ack) {

        try {
            totalMessages += messages.size();

            // 批量手动提交消费信息日志
            log.info("📦 Manual Commit Batch Consumer 👥 Group: {} 📊 Batch size: {} 🔢 Partitions: {} 📍 Offsets: {} 🔑 Keys: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual-batch", messages.size(), partitions, offsets, keys, totalMessages, successMessages, failedMessages);

            boolean allSuccess = processBatchMessages(messages, partitions, offsets, keys);

            if (allSuccess) {
                ack.acknowledge();
                successMessages += messages.size();
                log.info("Batch acknowledged successfully");
            } else {
                failedMessages += messages.size();
                log.warn("Batch processing failed, will be retried");
            }

            log.info("=====================================");

        } catch (Exception ex) {
            failedMessages += messages.size();
            log.error("Error processing batch messages, batch size: {}", messages.size(), ex);
        }
    }

    /**
     * 处理批量消息
     */
    private boolean processBatchMessages(List<String> messages,
                                         List<Integer> partitions,
                                         List<Long> offsets,
                                         List<String> keys) {
        try {
            boolean allSuccess = true;

            for (int i = 0; i < messages.size(); i++) {
                String message = messages.get(i);
                Integer partition = partitions.get(i);
                Long offset = offsets.get(i);
                String key = keys.get(i);

                log.debug("Processing message from partition {}: key={}, value={}, offset={}",
                        partition, key, message, offset);

                boolean success = processMessage(message);
                if (!success) {
                    allSuccess = false;
                    log.warn("Message processing failed: partition={}, offset={}", partition, offset);
                }
            }

            Thread.sleep(300);
            return allSuccess;

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
            return false;
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
            return false;
        }
    }

    /**
     * 消息处理逻辑
     */
    private boolean processMessage(String message) {
        try {
            Thread.sleep(100);

            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
                return Math.random() > 0.3; // 70% 成功率
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
                return true;
            } else {
                log.debug("Processing normal message: {}", message);
                return Math.random() > 0.05; // 95% 成功率
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
            return false;
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
            return false;
        }
    }

    /**
     * 获取手动提交统计信息
     */
    public String getStatistics() {
        return String.format("Manual Commit Consumer Statistics - Total: %d, Success: %d, Failed: %d, Success Rate: %.2f%%",
                totalMessages, successMessages, failedMessages,
                totalMessages > 0 ? (double) successMessages / totalMessages * 100 : 0);
    }
}
```

## Kafka Consumer消费模式详解

### 1. 消费者类型说明

本项目实现了4种不同类型的消费者：

- **简单消费者（SimpleConsumer）**：自动提交模式，单条消费，支持 ConsumerRecord 和 @Payload/@Header 两种方式
- **批量消费者（BatchConsumer）**：自动提交模式，批量消费
- **手动提交消费者（ManualCommitConsumer）**：手动提交模式，单条消费
- **手动提交批量消费者**：手动提交模式，批量消费

### 2. 消费模式对比

| 消费模式      | 提交方式 | 消费方式 | 适用场景      | 优点        | 缺点     |
|-----------|------|------|-----------|-----------|--------|
| 简单消费者     | 自动提交 | 单条   | 简单业务逻辑    | 简单易用      | 可能丢失消息 |
| 批量消费者     | 自动提交 | 批量   | 高吞吐量      | 效率高       | 可能丢失消息 |
| 手动提交消费者   | 手动提交 | 单条   | 精确控制      | 消息不丢失     | 复杂度高   |
| 手动提交批量消费者 | 手动提交 | 批量   | 高吞吐量+精确控制 | 效率高+消息不丢失 | 复杂度最高  |

### 3. 消费者配置说明

#### 自动提交模式配置

```properties
# 启用自动提交
spring.kafka.consumer.enable-auto-commit=true
# 自动提交间隔
spring.kafka.consumer.auto-commit-interval=1000
# 确认模式
spring.kafka.listener.ack-mode=batch
```

#### 手动提交模式配置

```properties
# 禁用自动提交
spring.kafka.consumer.enable-auto-commit=false
# 确认模式
spring.kafka.listener.ack-mode=manual
```

### 4. 消费者组说明

本项目使用了不同的消费者组ID来避免冲突：

- `demo-consumer-group`：简单消费者
- `demo-consumer-group-payload`：简单消费者（@Payload方式）
- `demo-consumer-group-batch`：批量消费者
- `demo-consumer-group-batch-payload`：批量消费者（@Payload方式）
- `demo-consumer-group-manual`：手动提交消费者
- `demo-consumer-group-manual-payload`：手动提交消费者（@Payload方式）
- `demo-consumer-group-manual-batch`：手动提交批量消费者

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-08-Event-Consumer

# 启动应用
mvn spring-boot:run
```

### 2. 发送测试消息

使用Kafka命令行工具发送消息到 `demo-consumer-topic` 来测试不同的消费者：

#### 2.1 简单消费者测试

```bash
# 发送单条消息（无键）
echo "Hello Kafka Consumer" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic

# 发送单条消息（带键）
echo "key1:test message with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"

# 发送Payload测试消息
echo "Payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
echo "payload-key:Payload test with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
```

#### 2.2 批量消费者测试

```bash
# 发送批量消息（无键）
for i in {1..5}; do echo "batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done

# 发送批量消息（带键）
for i in {1..3}; do echo "batch-key-$i:batch message with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done

# 快速发送多条消息测试批量消费
for i in {1..10}; do echo "rapid message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.1; done

# 发送Payload批量消息
for i in {1..8}; do echo "payload batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done

# 混合发送测试（部分有键，部分无键）
echo "mixed message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
echo "mixed-key:mixed message with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
echo "mixed message 3" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
```

#### 2.3 手动提交消费者测试

```bash
# 发送单条消息（无键）
echo "Manual commit test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic

# 发送单条消息（带键）
echo "manual-key:Manual commit with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"

# 发送错误消息测试重试机制
echo "error message for retry test" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic

# 发送紧急消息测试
echo "urgent message for priority test" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic

# 发送Payload手动提交测试
echo "Manual payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
echo "manual-payload-key:Manual payload with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"

# 发送批量手动提交测试
for i in {1..6}; do echo "manual batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done

# 快速发送多条消息测试批量手动提交
for i in {1..15}; do echo "rapid manual batch $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.05; done

# 发送包含错误消息的批次测试
echo "normal message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
echo "error message for batch retry" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
echo "normal message 3" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
```

#### 2.4 通用测试命令

```bash
# 发送单条消息（交互式）
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic

# 发送带键的消息（交互式）
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"

# 发送大量消息进行压力测试
for i in {1..100}; do echo "stress test message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 简单消费者日志

```
INFO  - === Simple Consumer (ConsumerRecord) ===
INFO  - Topic: demo-consumer-topic
INFO  - Partition: 0
INFO  - Offset: 123
INFO  - Key: null
INFO  - Value: test message
INFO  - Timestamp: 1640995200000
INFO  - Headers: []
INFO  - ================================
```

#### 批量消费者日志

```
INFO  - === Batch Consumer (List<ConsumerRecord>) ===
INFO  - Batch size: 5
INFO  - Total batches processed: 1
INFO  - Total messages processed: 5
INFO  - ==========================================
```

#### 手动提交消费者日志

```
INFO  - === Manual Commit Consumer (ConsumerRecord) ===
INFO  - Topic: demo-consumer-topic
INFO  - Partition: 0
INFO  - Offset: 124
INFO  - Key: null
INFO  - Value: test message
INFO  - Total messages: 1, Success: 1, Failed: 0
INFO  - Message acknowledged successfully
INFO  - =============================================
```

## 测试命令快速参考

### 消费者类型与测试命令对应关系

| 消费者类型                                 | 消费者组ID                             | 测试命令示例                                                                                                                                                                                         |
|---------------------------------------|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SimpleConsumer (ConsumerRecord)       | demo-consumer-group                | `echo "Hello Kafka" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic`                                                                                |
| SimpleConsumer (@Payload)             | demo-consumer-group-payload        | `echo "payload-key:Payload test" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"`          |
| BatchConsumer (ConsumerRecord)        | demo-consumer-group-batch          | `for i in {1..5}; do echo "batch $i" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done`                                                         |
| BatchConsumer (@Payload)              | demo-consumer-group-batch-payload  | `for i in {1..8}; do echo "payload batch $i" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done`                                                 |
| ManualCommitConsumer (ConsumerRecord) | demo-consumer-group-manual         | `echo "Manual commit test" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic`                                                                         |
| ManualCommitConsumer (@Payload)       | demo-consumer-group-manual-payload | `echo "manual-payload-key:Manual payload" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"` |
| ManualCommitConsumer (Batch)          | demo-consumer-group-manual-batch   | `for i in {1..6}; do echo "manual batch $i" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done`                                                  |

### 特殊测试场景

| 测试场景   | 命令                                                                                                                                                                                                                                                                                                                            | 说明             |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| 错误消息重试 | `echo "error message for retry test" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic`                                                                                                                                                                                              | 测试手动提交消费者的重试机制 |
| 紧急消息处理 | `echo "urgent message for priority test" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic`                                                                                                                                                                                          | 测试紧急消息的优先处理    |
| 批量错误测试 | `echo "normal" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; echo "error" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; echo "normal" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic` | 测试批量消费中的错误处理   |
| 压力测试   | `for i in {1..100}; do echo "stress test $i" \| kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done`                                                                                                                                                                                | 测试大量消息的处理能力    |

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9092`运行
2. **消费者组**: 不同消费者使用不同的消费者组ID，避免冲突
3. **偏移量管理**: 手动提交模式下需要正确处理异常情况
4. **批量消费**: 批量消费时异常会中断整个批次
5. **性能调优**: 根据业务需求调整批量大小和并发数
6. **监控告警**: 利用统计信息实现消费者监控和告警
7. **错误处理**: 实现完善的异常处理和重试机制
8. **资源管理**: 合理配置消费者参数，避免资源浪费
9. **测试命令**: 每个消费者方法都包含详细的测试命令注释，可直接复制使用
10. **键值分离**: 使用`--property "parse.key=true" --property "key.separator=:"`来发送带键的消息
