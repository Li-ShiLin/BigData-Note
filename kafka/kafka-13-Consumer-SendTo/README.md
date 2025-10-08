<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [消费者消息转发](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. Kafka配置类](#3-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [4. 消息转发消费者](#4-%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E6%B6%88%E8%B4%B9%E8%80%85)
    - [5. 目标消息消费者](#5-%E7%9B%AE%E6%A0%87%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E8%80%85)
  - [Kafka消息转发详解](#kafka%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E8%AF%A6%E8%A7%A3)
    - [1. 消息转发流程](#1-%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E6%B5%81%E7%A8%8B)
    - [2. @SendTo注解使用方式](#2-sendto%E6%B3%A8%E8%A7%A3%E4%BD%BF%E7%94%A8%E6%96%B9%E5%BC%8F)
      - [方式一：基础消息转发](#%E6%96%B9%E5%BC%8F%E4%B8%80%E5%9F%BA%E7%A1%80%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91)
      - [方式二：条件消息转发](#%E6%96%B9%E5%BC%8F%E4%BA%8C%E6%9D%A1%E4%BB%B6%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91)
    - [3. 消息转发类型](#3-%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E7%B1%BB%E5%9E%8B)
      - [基础转发](#%E5%9F%BA%E7%A1%80%E8%BD%AC%E5%8F%91)
      - [增强转发](#%E5%A2%9E%E5%BC%BA%E8%BD%AC%E5%8F%91)
      - [条件转发](#%E6%9D%A1%E4%BB%B6%E8%BD%AC%E5%8F%91)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送基础转发测试消息](#%E5%8F%91%E9%80%81%E5%9F%BA%E7%A1%80%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [发送增强转发测试消息](#%E5%8F%91%E9%80%81%E5%A2%9E%E5%BC%BA%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [发送条件转发测试消息](#%E5%8F%91%E9%80%81%E6%9D%A1%E4%BB%B6%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [批量发送转发测试消息](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [发送各种类型的转发测试消息](#%E5%8F%91%E9%80%81%E5%90%84%E7%A7%8D%E7%B1%BB%E5%9E%8B%E7%9A%84%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
      - [发送指定分区的转发测试消息](#%E5%8F%91%E9%80%81%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E7%9A%84%E8%BD%AC%E5%8F%91%E6%B5%8B%E8%AF%95%E6%B6%88%E6%81%AF)
    - [测试用例与预期效果](#%E6%B5%8B%E8%AF%95%E7%94%A8%E4%BE%8B%E4%B8%8E%E9%A2%84%E6%9C%9F%E6%95%88%E6%9E%9C)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [基础消息转发日志](#%E5%9F%BA%E7%A1%80%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E6%97%A5%E5%BF%97)
      - [增强消息转发日志](#%E5%A2%9E%E5%BC%BA%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E6%97%A5%E5%BF%97)
      - [条件消息转发日志](#%E6%9D%A1%E4%BB%B6%E6%B6%88%E6%81%AF%E8%BD%AC%E5%8F%91%E6%97%A5%E5%BF%97)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 消费者消息转发

## 项目作用

本项目演示了SpringBoot中Kafka消息转发的实现，包括使用@SendTo注解实现消息自动转发、多种转发方式、条件转发等，帮助开发者理解Kafka消息转发的工作原理和实际应用场景。

## 项目结构

```
kafka-13-Consumer-SendTo/
├── src/main/java/com/action/kafka13consumersendto/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类
│   ├── consumer/
│   │   ├── MessageForwardConsumer.java         # 消息转发消费者
│   │   └── TargetMessageConsumer.java          # 目标消息消费者
│   ├── service/
│   │   └── MessageForwardProducerService.java # 生产者服务
│   ├── controller/
│   │   └── MessageForwardController.java      # REST API控制器
│   └── Kafka13ConsumerSendToApplication.java   # 主启动类
├── src/main/resources/
│   └── application.properties                  # 配置文件
├── src/test/java/com/action/kafka13consumersendto/
│   └── Kafka13ConsumerSendToApplicationTests.java # 测试类
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

`application.properties`：Kafka服务器和消息转发配置

```properties
# ========================================
# Kafka 消息转发演示应用配置
# ========================================

# 应用名称
spring.application.name=kafka-13-Consumer-SendTo

# ========================================
# Kafka 连接配置
# ========================================
# Kafka 服务器地址（多个地址用逗号分隔）
# 默认：localhost:9092
spring.kafka.bootstrap-servers=192.168.56.10:9092

# ========================================
# 演示 Topic 配置
# ========================================
# 源消息 Topic 名称
# 应用启动时会自动创建该 Topic（3个分区，1个副本）
kafka.topic.source=source-topic

# 目标消息 Topic 名称
# 应用启动时会自动创建该 Topic（3个分区，1个副本）
kafka.topic.target=target-topic

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
# 键反序列化器：将字节数组反序列化为字符串
# 使用字符串反序列化器，适合演示场景
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# 值反序列化器：将字节数组反序列化为字符串
# 使用字符串反序列化器，适合演示场景
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# 自动提交偏移量：false表示手动提交
spring.kafka.consumer.enable-auto-commit=false

# 偏移量重置策略：earliest表示从最早的消息开始消费
spring.kafka.consumer.auto-offset-reset=earliest

# ========================================
# 消息监听器配置
# ========================================
# 开启消息监听的手动确认模式
spring.kafka.listener.ack-mode=manual_immediate

# ========================================
# 服务器配置
# ========================================
# 服务器端口配置
server.port=9103
```

### 3. Kafka配置类

`config/KafkaConfig.java`：配置Kafka主题创建

```java
package com.action.kafka13consumersendto.config;

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
 * 2) 定义消息转发演示用的多个 Topic
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
     * 源消息主题名称配置
     * 从 application.properties 读取，默认 source-topic
     */
    @Value("${kafka.topic.source:source-topic}")
    private String sourceTopicName;

    /**
     * 目标消息主题名称配置
     * 从 application.properties 读取，默认 target-topic
     */
    @Value("${kafka.topic.target:target-topic}")
    private String targetTopicName;

    /**
     * 消息转发演示用源 Topic Bean
     * 
     * 作用：自动创建用于接收原始消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic sourceTopic() {
        return TopicBuilder.name(sourceTopicName)
                .partitions(3)    // 分区数：影响并行处理能力，设置为3个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 消息转发演示用目标 Topic Bean
     * 
     * 作用：自动创建用于接收转发消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic targetTopic() {
        return TopicBuilder.name(targetTopicName)
                .partitions(3)    // 分区数：影响并行处理能力，设置为3个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
```

### 4. 消息转发消费者

`consumer/MessageForwardConsumer.java`：使用@SendTo注解实现消息转发

```java
package com.action.kafka13consumersendto.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * 消息转发消费者
 * 
 * 功能说明：
 * 1) 演示 Kafka 消息转发功能
 * 2) 使用 @SendTo 注解实现消息自动转发
 * 3) 支持消息处理和转发逻辑
 * 4) 提供多种消息转发方式
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听源主题
 * - 使用 @SendTo 注解指定转发目标主题
 * - 使用 Acknowledgment 进行手动消息确认
 * - 支持消息内容修改和增强
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - @SendTo: 指定消息转发目标主题
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class MessageForwardConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageForwardConsumer.class);

    /**
     * 基础消息转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 sourceGroup 消费者组
     * 3) 接收消息并添加转发标识
     * 4) 自动转发到 target-topic 主题
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 转发消息内容
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "sourceGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String forwardMessage(ConsumerRecord<String, String> record,
                                @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                @Header(value = KafkaHeaders.OFFSET) String offset,
                                Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[forward] === 消息转发演示 ===");
            log.info("[forward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            // 处理消息并添加转发标识
            String originalMessage = record.value();
            String forwardedMessage = "[FORWARDED] " + originalMessage + " -> processed at " + System.currentTimeMillis();
            
            // 打印转发消息
            log.info("[forward] 转发消息: {}", forwardedMessage);
            log.info("[forward] 消息转发完成: {} -> {}", topic, "target-topic");
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("[forward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
            // 返回转发消息内容
            return forwardedMessage;
            
        } catch (Exception e) {
            log.error("消息转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 消息增强转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 enhanceGroup 消费者组
     * 3) 接收消息并进行业务处理
     * 4) 增强消息内容并转发
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 增强后的转发消息内容
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "enhanceGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String enhanceAndForwardMessage(ConsumerRecord<String, String> record,
                                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                         @Header(value = KafkaHeaders.OFFSET) String offset,
                                         Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[enhanceAndForward] === 消息增强转发演示 ===");
            log.info("[enhanceAndForward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            // 模拟业务处理逻辑
            String originalMessage = record.value();
            String processedMessage = processMessage(originalMessage);
            
            // 构建增强后的消息
            String enhancedMessage = String.format(
                "[ENHANCED] Original: %s | Processed: %s | Timestamp: %d | Source: %s", 
                originalMessage, processedMessage, System.currentTimeMillis(), topic
            );
            
            // 打印增强后的消息
            log.info("增强消息: {}", enhancedMessage);
            log.info("[enhanceAndForward] 消息增强转发完成: {} -> {}", topic, "target-topic");
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("[enhanceAndForward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
            // 返回增强后的转发消息内容
            return enhancedMessage;
            
        } catch (Exception e) {
            log.error("消息增强转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 条件消息转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 conditionGroup 消费者组
     * 3) 根据消息内容决定是否转发
     * 4) 满足条件时转发，不满足时返回null
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 满足条件时返回转发消息，否则返回null
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "conditionGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String conditionalForwardMessage(ConsumerRecord<String, String> record,
                                           @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                           @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                           @Header(value = KafkaHeaders.OFFSET) String offset,
                                           Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[conditionalForward] === 条件消息转发演示 ===");
            log.info("[conditionalForward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            String originalMessage = record.value();
            
            // 条件判断：只转发包含特定关键词的消息
            if (shouldForwardMessage(originalMessage)) {
                String forwardedMessage = "[CONDITIONAL-FORWARD] " + originalMessage + " | Condition: PASSED";
                
                // 打印转发消息
                log.info("[conditionalForward] 条件转发消息: {}", forwardedMessage);
                log.info("[conditionalForward] 消息条件转发完成: {} -> {}", topic, "target-topic");
                
                // 手动确认消息处理完成
                ack.acknowledge();
                log.info("[conditionalForward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
                
                // 返回转发消息内容
                return forwardedMessage;
            } else {
                // 不满足转发条件
                log.info("[conditionalForward] 消息不满足转发条件，跳过转发: {}", originalMessage);
                
                // 手动确认消息处理完成（即使不转发也要确认）
                ack.acknowledge();
                log.info("[conditionalForward] 消息处理完成（未转发）并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
                
                // 返回null表示不转发
                return null;
            }
            
        } catch (Exception e) {
            log.error("条件消息转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 模拟消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟实际业务处理逻辑
     * 2) 对消息内容进行转换和增强
     * 3) 用于演示消息处理流程
     * 
     * @param originalMessage 原始消息内容
     * @return 处理后的消息内容
     */
    private String processMessage(String originalMessage) {
        // 模拟处理时间
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 模拟业务处理：转换为大写并添加处理标识
        return originalMessage.toUpperCase() + " [PROCESSED]";
    }

    /**
     * 判断是否应该转发消息
     * 
     * 功能说明：
     * 1) 根据消息内容判断是否满足转发条件
     * 2) 演示条件转发的业务逻辑
     * 3) 用于演示选择性转发
     * 
     * @param message 消息内容
     * @return 是否应该转发
     */
    private boolean shouldForwardMessage(String message) {
        // 转发条件：消息长度大于5且包含特定关键词
        return message != null && 
               message.length() > 5 && 
               (message.contains("forward") || message.contains("test") || message.contains("demo"));
    }
}
```

### 5. 目标消息消费者

`consumer/TargetMessageConsumer.java`：接收转发后的消息

```java
package com.action.kafka13consumersendto.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 目标消息消费者
 * 
 * 功能说明：
 * 1) 监听转发后的目标消息
 * 2) 接收来自消息转发的消息
 * 3) 演示消息转发的完整流程
 * 4) 提供消息接收确认
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听目标主题
 * - 使用 Acknowledgment 进行手动消息确认
 * - 记录转发消息的详细信息
 * - 演示消息转发的最终接收
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class TargetMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(TargetMessageConsumer.class);

    /**
     * 接收转发消息演示
     * 
     * 执行流程：
     * 1) 监听 target-topic 主题
     * 2) 使用 targetGroup 消费者组
     * 3) 接收来自消息转发的消息
     * 4) 打印转发消息的详细信息
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = "${kafka.topic.target:target-topic}", groupId = "targetGroup")
    public void receiveForwardedMessage(ConsumerRecord<String, String> record,
                                      @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                      @Header(value = KafkaHeaders.OFFSET) String offset,
                                      Acknowledgment ack) {
        try {
            // 打印接收到的转发消息
            log.info("=== 接收转发消息演示 ===");
            log.info("转发消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            log.info("消息时间戳: {}", record.timestamp());
            log.info("消息头: {}", record.headers());
            log.info("完整消息记录: {}", record.toString());
            log.info("==========================================");
            
            // 模拟消息处理逻辑
            processForwardedMessage(record);
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("转发消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
        } catch (Exception e) {
            log.error("处理转发消息时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
        }
    }

    /**
     * 模拟转发消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟对转发消息的业务处理
     * 2) 演示消息转发的最终处理流程
     * 3) 用于验证消息转发的完整性
     * 
     * @param record 转发消息记录
     * @throws Exception 处理异常
     */
    private void processForwardedMessage(ConsumerRecord<String, String> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();
        String value = record.value();
        
        log.info("开始处理转发消息: topic={}, partition={}, offset={}, key={}, value={}", 
                topic, partition, offset, key, value);
        
        // 模拟处理时间
        Thread.sleep(100);
        
        // 根据消息内容进行不同的处理
        if (value.contains("[FORWARDED]")) {
            log.info("处理基础转发消息: {}", value);
        } else if (value.contains("[ENHANCED]")) {
            log.info("处理增强转发消息: {}", value);
        } else if (value.contains("[CONDITIONAL-FORWARD]")) {
            log.info("处理条件转发消息: {}", value);
        } else {
            log.info("处理其他类型转发消息: {}", value);
        }
        
        log.info("转发消息处理完成: topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
```

## Kafka消息转发详解

### 1. 消息转发流程

```
源消息 → 转发消费者(@SendTo) → 目标主题 → 目标消费者
```

### 2. @SendTo注解使用方式

#### 方式一：基础消息转发

```java
@KafkaListener(topics = "source-topic", groupId = "sourceGroup")
@SendTo("target-topic")
public String forwardMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
    // 处理消息
    String forwardedMessage = "[FORWARDED] " + record.value();
    ack.acknowledge();
    return forwardedMessage; // 返回的消息会自动发送到target-topic
}
```

#### 方式二：条件消息转发

```java
@KafkaListener(topics = "source-topic", groupId = "conditionGroup")
@SendTo("target-topic")
public String conditionalForwardMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
    if (shouldForward(record.value())) {
        ack.acknowledge();
        return "[FORWARDED] " + record.value(); // 转发消息
    } else {
        ack.acknowledge();
        return null; // 不转发，返回null
    }
}
```

### 3. 消息转发类型

#### 基础转发
- 直接转发消息内容
- 添加转发标识
- 保持消息完整性

#### 增强转发
- 对消息进行业务处理
- 添加处理结果和元数据
- 提供更丰富的信息

#### 条件转发
- 根据消息内容决定是否转发
- 满足条件时转发，否则跳过
- 实现选择性消息处理

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-13-Consumer-SendTo

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送基础转发测试消息

```bash
# 发送基础转发测试消息
curl "http://localhost:9103/api/send/basic?message=hello world"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "基础转发测试消息发送成功",
  "data": "hello world"
}
```

#### 发送增强转发测试消息

```bash
# 发送增强转发测试消息
curl "http://localhost:9103/api/send/enhance?message=enhance test"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "增强转发测试消息发送成功",
  "data": "enhance test"
}
```

#### 发送条件转发测试消息

```bash
# 发送条件转发测试消息（满足条件）
curl "http://localhost:9103/api/send/conditional?message=conditional forward test"

# 发送条件转发测试消息（不满足条件）
curl "http://localhost:9103/api/send/conditional?message=skip message"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "条件转发测试消息发送成功",
  "data": "conditional forward test"
}
```

#### 批量发送转发测试消息

```bash
# 批量发送转发测试消息
curl "http://localhost:9103/api/send/batch?count=5"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量转发测试消息发送成功",
  "data": {
    "count": 5
  }
}
```

#### 发送各种类型的转发测试消息

```bash
# 发送各种类型的转发测试消息
curl "http://localhost:9103/api/send/various"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "各种类型转发测试消息发送成功",
  "data": "包含基础转发、增强转发、条件转发等消息"
}
```

#### 发送指定分区的转发测试消息

```bash
# 发送指定分区的转发测试消息
curl "http://localhost:9103/api/send/partition?partition=0&message=partition test"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "指定分区转发测试消息发送成功",
  "data": {
    "partition": 0,
    "message": "partition test"
  }
}
```

### 测试用例与预期效果

为便于验证消息确实被“源主题 → 转发 → 目标主题”链路处理，下面给出每个接口的预期转发情况与日志前缀。

- 基础转发接口: GET /api/send/basic
  - 预期效果：
    - `source-topic` 收到原始消息后，由方法 `forwardMessage` 处理并转发到 `target-topic`。
    - 目标消费者接收到的消息内容前缀包含 `[FORWARDED]`。
    - 日志前缀：源端为 `[forward]`，目标端为 `[目标主题]`。

- 增强转发接口: GET /api/send/enhance
  - 预期效果：
    - `source-topic` 收到原始消息后，由方法 `enhanceAndForwardMessage` 进行业务处理（转大写并附加元数据），再转发到 `target-topic`。
    - 目标消费者接收到的消息内容前缀包含 `[ENHANCED]`，内容中含有 `Processed:`、`Timestamp:`、`Source:` 等元信息。
    - 日志前缀：源端为 `[enhanceAndForward]`，目标端为 `[目标主题]`。

- 条件转发接口: GET /api/send/conditional
  - 预期效果：
    - 当消息内容满足条件（长度>5 且包含 `forward`/`test`/`demo`）时：
      - 由方法 `conditionalForwardMessage` 转发到 `target-topic`，目标端接收的消息前缀包含 `[CONDITIONAL-FORWARD]`，并在源端日志出现“消息条件转发完成”。
    - 当消息内容不满足条件时：
      - 不进行转发（返回 null），仅源端出现“消息处理完成（未转发）并已确认”，目标端不会收到本条消息。
    - 日志前缀：源端为 `[conditionalForward]`，目标端为 `[目标主题]`。

- 批量发送接口: GET /api/send/batch
  - 预期效果：
    - 连续向 `source-topic` 发送 N 条消息，按发送顺序依次被源端转发，最终 `target-topic` 能按消费顺序看到 N 条对应的转发结果（不同类型均为基础转发格式，前缀 `[FORWARDED]`）。
    - 源端与目标端均出现等量的“处理完成并已确认”日志。

- 多类型综合接口: GET /api/send/various
  - 预期效果：
    - 依次触发三类转发：基础转发（前缀 `[FORWARDED]`）、增强转发（前缀 `[ENHANCED]`）、条件转发（前缀 `[CONDITIONAL-FORWARD]`），以及一条不满足条件的消息（不会出现在目标端）。
    - 目标端将只看到三条消息（基础、增强、条件），第四条“不满足条件”的消息不会出现。
    - 日志前缀与上述各接口一致：`[forward]`、`[enhanceAndForward]`、`[conditionalForward]`、`[目标主题]`。

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 基础消息转发日志

```
INFO  - === 消息转发演示 ===
INFO  - 原始消息: topic=source-topic, partition=0, offset=0, key=null, value=hello world
INFO  - 转发消息: [FORWARDED] hello world -> processed at 1640995200000
INFO  - 消息转发完成: source-topic -> target-topic
INFO  - 消息处理完成并已确认: topic=source-topic, partition=0, offset=0

INFO  - === 接收转发消息演示 ===
INFO  - 转发消息: topic=target-topic, partition=0, offset=0, key=null, value=[FORWARDED] hello world -> processed at 1640995200000
INFO  - 消息时间戳: 1640995200000
INFO  - 消息头: RecordHeaders(headers = [], isReadOnly = false)
INFO  - 完整消息记录: ConsumerRecord(topic = target-topic, partition = 0, offset = 0, CreateTime = 1640995200000, serialized key size = -1, serialized value size = 65, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = [FORWARDED] hello world -> processed at 1640995200000)
INFO  - ==========================================
INFO  - 开始处理转发消息: topic=target-topic, partition=0, offset=0, key=null, value=[FORWARDED] hello world -> processed at 1640995200000
INFO  - 处理基础转发消息: [FORWARDED] hello world -> processed at 1640995200000
INFO  - 转发消息处理完成: topic=target-topic, partition=0, offset=0
INFO  - 转发消息处理完成并已确认: topic=target-topic, partition=0, offset=0
```

#### 增强消息转发日志

```
INFO  - === 消息增强转发演示 ===
INFO  - 原始消息: topic=source-topic, partition=1, offset=1, key=null, value=enhance test
INFO  - 增强消息: [ENHANCED] Original: enhance test | Processed: ENHANCE TEST [PROCESSED] | Timestamp: 1640995200000 | Source: source-topic
INFO  - 消息增强转发完成: source-topic -> target-topic
INFO  - 消息处理完成并已确认: topic=source-topic, partition=1, offset=1

INFO  - === 接收转发消息演示 ===
INFO  - 转发消息: topic=target-topic, partition=1, offset=1, key=null, value=[ENHANCED] Original: enhance test | Processed: ENHANCED TEST [PROCESSED] | Timestamp: 1640995200000 | Source: source-topic
INFO  - 处理增强转发消息: [ENHANCED] Original: enhance test | Processed: ENHANCED TEST [PROCESSED] | Timestamp: 1640995200000 | Source: source-topic
INFO  - 转发消息处理完成: topic=target-topic, partition=1, offset=1
INFO  - 转发消息处理完成并已确认: topic=target-topic, partition=1, offset=1
```

#### 条件消息转发日志

```
INFO  - === 条件消息转发演示 ===
INFO  - 原始消息: topic=source-topic, partition=2, offset=2, key=null, value=conditional forward test
INFO  - 条件转发消息: [CONDITIONAL-FORWARD] conditional forward test | Condition: PASSED
INFO  - 消息条件转发完成: source-topic -> target-topic
INFO  - 消息处理完成并已确认: topic=source-topic, partition=2, offset=2

INFO  - === 接收转发消息演示 ===
INFO  - 转发消息: topic=target-topic, partition=2, offset=2, key=null, value=[CONDITIONAL-FORWARD] conditional forward test | Condition: PASSED
INFO  - 处理条件转发消息: [CONDITIONAL-FORWARD] conditional forward test | Condition: PASSED
INFO  - 转发消息处理完成: topic=target-topic, partition=2, offset=2
INFO  - 转发消息处理完成并已确认: topic=target-topic, partition=2, offset=2
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9092`运行
2. **@SendTo注解**: 必须与@KafkaListener一起使用，方法返回值会自动发送到指定主题
3. **消息确认**: 转发消费者和目标消费者都使用手动确认模式，需要调用ack.acknowledge()
4. **异常处理**: 消息处理失败时不会确认消息，会被重新消费
5. **消费者组**: 不同消费者组可以独立消费消息
6. **条件转发**: 返回null表示不转发消息，返回非null值表示转发消息
7. **消息内容**: 转发消息的内容由方法返回值决定，可以修改和增强
8. **分区路由**: 转发消息会重新进行分区路由，可能与原消息分区不同
9. **线程安全**: KafkaTemplate是线程安全的，可以并发使用
10. **配置管理**: 建议将Topic名称等配置放在配置文件中管理
