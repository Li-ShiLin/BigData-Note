<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [offset偏移量策略](#offset%E5%81%8F%E7%A7%BB%E9%87%8F%E7%AD%96%E7%95%A5)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 常量定义](#3-%E5%B8%B8%E9%87%8F%E5%AE%9A%E4%B9%89)
    - [4. Kafka配置类](#4-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [5. 消费者实现](#5-%E6%B6%88%E8%B4%B9%E8%80%85%E5%AE%9E%E7%8E%B0)
      - [Earliest策略消费者](#earliest%E7%AD%96%E7%95%A5%E6%B6%88%E8%B4%B9%E8%80%85)
      - [Latest策略消费者](#latest%E7%AD%96%E7%95%A5%E6%B6%88%E8%B4%B9%E8%80%85)
      - [None策略消费者](#none%E7%AD%96%E7%95%A5%E6%B6%88%E8%B4%B9%E8%80%85)
    - [6. 生产者服务](#6-%E7%94%9F%E4%BA%A7%E8%80%85%E6%9C%8D%E5%8A%A1)
    - [7. REST控制器](#7-rest%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [偏移量策略说明](#%E5%81%8F%E7%A7%BB%E9%87%8F%E7%AD%96%E7%95%A5%E8%AF%B4%E6%98%8E)
    - [1. earliest 策略](#1-earliest-%E7%AD%96%E7%95%A5)
    - [2. latest 策略](#2-latest-%E7%AD%96%E7%95%A5)
    - [3. none 策略](#3-none-%E7%AD%96%E7%95%A5)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送消息到earliest主题](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0earliest%E4%B8%BB%E9%A2%98)
      - [发送消息到latest主题](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0latest%E4%B8%BB%E9%A2%98)
      - [发送消息到none主题](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0none%E4%B8%BB%E9%A2%98)
      - [发送消息到测试主题（所有消费者都会监听）](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%B5%8B%E8%AF%95%E4%B8%BB%E9%A2%98%E6%89%80%E6%9C%89%E6%B6%88%E8%B4%B9%E8%80%85%E9%83%BD%E4%BC%9A%E7%9B%91%E5%90%AC)
      - [批量发送消息到所有主题](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%89%80%E6%9C%89%E4%B8%BB%E9%A2%98)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [消息发送日志](#%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%97%A5%E5%BF%97)
      - [消费者消费日志](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
    - [4. 测试场景](#4-%E6%B5%8B%E8%AF%95%E5%9C%BA%E6%99%AF)
      - [场景1：测试earliest策略](#%E5%9C%BA%E6%99%AF1%E6%B5%8B%E8%AF%95earliest%E7%AD%96%E7%95%A5)
      - [场景2：测试latest策略](#%E5%9C%BA%E6%99%AF2%E6%B5%8B%E8%AF%95latest%E7%AD%96%E7%95%A5)
      - [场景3：测试通用主题](#%E5%9C%BA%E6%99%AF3%E6%B5%8B%E8%AF%95%E9%80%9A%E7%94%A8%E4%B8%BB%E9%A2%98)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# offset偏移量策略

## 项目作用

本项目演示了SpringBoot中Kafka的三种主要偏移量策略：`earliest`、`latest`和`none`。通过具体的代码实现，展示不同偏移量策略的配置、消费者实现和测试方法，帮助开发者理解Kafka偏移量管理机制。

## 项目结构

```
kafka-04-offset/
├── src/main/java/com/action/kafka04offset/
│   ├── config/
│   │   └── KafkaConfig.java              # Kafka配置类 - 多策略配置
│   ├── constants/
│   │   └── KafkaConstants.java           # 常量定义
│   ├── consumer/
│   │   ├── EarliestOffsetConsumer.java   # Earliest策略消费者
│   │   ├── LatestOffsetConsumer.java     # Latest策略消费者
│   │   └── NoneOffsetConsumer.java       # None策略消费者
│   ├── controller/
│   │   └── KafkaOffsetController.java    # REST API控制器
│   ├── service/
│   │   └── KafkaProducerService.java     # 生产者服务
│   └── Kafka04OffsetApplication.java     # 主启动类
├── src/main/resources/
│   └── application.yml                   # 配置文件
└── pom.xml                              # Maven配置
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

    <!-- Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.yml`：Kafka服务器和消费者配置

```yaml
spring:
  application:
    name: kafka-04-offset
  
  kafka:
    # Kafka服务器配置
    bootstrap-servers: 192.168.56.10:9092
    
    # 管理配置 - 自动创建主题
    admin:
      client-id: kafka-04-offset-admin
      properties:
        auto.create.topics.enable: true
    
    # 生产者配置
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
      batch-size: 16384
      linger-ms: 1
      buffer-memory: 33554432
      properties:
        auto.create.topics.enable: true
    
    # 消费者配置 - 默认配置
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      group-id: kafka-04-offset-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      auto-commit-interval-ms: 1000
      session-timeout-ms: 30000
      max-poll-records: 500
      max-poll-interval-ms: 300000

# 日志配置
logging:
  level:
    com.action.kafka04offset: DEBUG
    org.springframework.kafka: INFO
    org.apache.kafka: WARN
```

### 3. 常量定义

`constants/KafkaConstants.java`：统一管理主题名称、消费者组和偏移量策略常量

```java
package com.action.kafka04offset.constants;

/**
 * Kafka 常量定义
 * 
 * @author action
 * @since 2024
 */
public class KafkaConstants {
    
    /**
     * 主题名称
     */
    public static final String TEST_TOPIC = "kafka-04-offset-test";
    public static final String EARLIEST_TOPIC = "kafka-04-offset-earliest";
    public static final String LATEST_TOPIC = "kafka-04-offset-latest";
    public static final String NONE_TOPIC = "kafka-04-offset-none";
    
    /**
     * 消费者组名称
     */
    public static final String EARLIEST_GROUP = "kafka-04-earliest-group";
    public static final String LATEST_GROUP = "kafka-04-latest-group";
    public static final String NONE_GROUP = "kafka-04-none-group";
    
    /**
     * 偏移量策略
     */
    public static final String OFFSET_EARLIEST = "earliest";
    public static final String OFFSET_LATEST = "latest";
    public static final String OFFSET_NONE = "none";
    
    /**
     * 消息键前缀
     */
    public static final String MESSAGE_KEY_PREFIX = "offset-test-";
    
    /**
     * 消息值前缀
     */
    public static final String MESSAGE_VALUE_PREFIX = "Offset Test Message - ";
}
```

### 4. Kafka配置类

`config/KafkaConfig.java`：为每种偏移量策略创建独立的ConsumerFactory和ContainerFactory

```java
package com.action.kafka04offset.config;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * 生产者配置
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Kafka 管理配置 - 自动创建主题
     */
@Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * 默认消费者配置 - earliest 策略
     */
@Bean
public ConsumerFactory<String, String> defaultConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.EARLIEST_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Latest 偏移量策略的消费者配置
     */
@Bean
public ConsumerFactory<String, String> latestConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.LATEST_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_LATEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * None 偏移量策略的消费者配置
     */
@Bean
public ConsumerFactory<String, String> noneConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.NONE_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.OFFSET_NONE);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        // 添加容错配置
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 默认消费者容器工厂 - earliest 策略
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> defaultKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(defaultConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /**
     * Latest 策略的消费者容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> latestKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(latestConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /**
     * None 策略的消费者容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> noneKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(noneConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // 设置错误处理器
        factory.setCommonErrorHandler(new org.springframework.kafka.listener.DefaultErrorHandler());
        
        // 设置重试配置
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setPollTimeout(3000);
        
        return factory;
    }

    /**
     * None 策略错误处理器
     */
    @Bean
    public KafkaListenerErrorHandler noneErrorHandler() {
        return new KafkaListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
                Throwable cause = exception.getCause();
                if (cause instanceof org.apache.kafka.clients.consumer.NoOffsetForPartitionException) {
                    log.warn("None 策略消费者遇到无偏移量异常，这是正常行为: {}", cause.getMessage());
                    return null;
                }
                throw exception;
            }
        };
    }
}
```

### 5. 消费者实现

#### Earliest策略消费者

`consumer/EarliestOffsetConsumer.java`：消费所有历史消息

```java
package com.action.kafka04offset.consumer;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EarliestOffsetConsumer {

    /**
     * 监听 earliest 主题的消息
     * 使用 earliest 偏移量策略，会从最早的消息开始消费
     */
@KafkaListener(
    topics = KafkaConstants.EARLIEST_TOPIC,
    groupId = KafkaConstants.EARLIEST_GROUP,
    containerFactory = "defaultKafkaListenerContainerFactory"
)
    public void handleEarliestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== EARLIEST 偏移量策略消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.EARLIEST_GROUP);
        log.info("================================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听通用测试主题的消息 - 使用 earliest 策略
     */
    @KafkaListener(
        topics = KafkaConstants.TEST_TOPIC,
        groupId = KafkaConstants.EARLIEST_GROUP + "-test",
        containerFactory = "defaultKafkaListenerContainerFactory"
    )
    public void handleTestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== EARLIEST 策略 - 测试主题消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.EARLIEST_GROUP + "-test");
        log.info("=====================================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("测试消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("测试消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
```

#### Latest策略消费者

`consumer/LatestOffsetConsumer.java`：只消费新产生的消息

```java
package com.action.kafka04offset.consumer;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LatestOffsetConsumer {

    /**
     * 监听 latest 主题的消息
     * 使用 latest 偏移量策略，只会消费新产生的消息
     */
@KafkaListener(
    topics = KafkaConstants.LATEST_TOPIC,
    groupId = KafkaConstants.LATEST_GROUP,
    containerFactory = "latestKafkaListenerContainerFactory"
)
    public void handleLatestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== LATEST 偏移量策略消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.LATEST_GROUP);
        log.info("==============================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听通用测试主题的消息 - 使用 latest 策略
     */
    @KafkaListener(
        topics = KafkaConstants.TEST_TOPIC,
        groupId = KafkaConstants.LATEST_GROUP + "-test",
        containerFactory = "latestKafkaListenerContainerFactory"
    )
    public void handleTestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== LATEST 策略 - 测试主题消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.LATEST_GROUP + "-test");
        log.info("====================================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("测试消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("测试消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
```

#### None策略消费者

`consumer/NoneOffsetConsumer.java`：如果没有偏移量会抛出异常

```java
package com.action.kafka04offset.consumer;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NoneOffsetConsumer {

    /**
     * 监听 none 主题的消息
     * 使用 none 偏移量策略，如果没有找到偏移量会抛出异常
     */
@KafkaListener(
    topics = KafkaConstants.NONE_TOPIC,
    groupId = KafkaConstants.NONE_GROUP,
        containerFactory = "noneKafkaListenerContainerFactory",
        errorHandler = "noneErrorHandler",
        autoStartup = "false"
    )
    public void handleNoneMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== NONE 偏移量策略消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.NONE_GROUP);
        log.info("============================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听通用测试主题的消息 - 使用 none 策略
     */
    @KafkaListener(
        topics = KafkaConstants.TEST_TOPIC,
        groupId = KafkaConstants.NONE_GROUP + "-test",
        containerFactory = "noneKafkaListenerContainerFactory",
        errorHandler = "noneErrorHandler",
        autoStartup = "false"
    )
    public void handleTestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== NONE 策略 - 测试主题消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.NONE_GROUP + "-test");
        log.info("==================================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("测试消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("测试消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
```

### 6. 生产者服务

`service/KafkaProducerService.java`：使用CompletableFuture实现异步消息发送

```java
package com.action.kafka04offset.service;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送消息到指定主题
     */
public CompletableFuture<SendResult<String, String>> sendMessage(String topic, String key, String message) {
        log.info("发送消息到主题: {}, 键: {}, 内容: {}", topic, key, message);

    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);
    
    future.whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("消息发送失败 - 主题: {}, 错误: {}", topic, ex.getMessage(), ex);
        } else if (result != null) {
            log.info("消息发送成功 - 主题: {}, 分区: {}, 偏移量: {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
        }
    });
    
    return future;
}

    /**
     * 发送测试消息到 earliest 主题
     */
    public void sendEarliestTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 earliest 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
String key = KafkaConstants.MESSAGE_KEY_PREFIX + "earliest-" + i;
String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Earliest Test " + i + 
               " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.EARLIEST_TOPIC, key, value);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 earliest 主题", messageCount);
    }

    /**
     * 发送测试消息到 latest 主题
     */
    public void sendLatestTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 latest 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "latest-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Latest Test " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.LATEST_TOPIC, key, value);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 latest 主题", messageCount);
    }

    /**
     * 发送测试消息到 none 主题
     */
    public void sendNoneTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 none 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "none-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "None Test " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.NONE_TOPIC, key, value);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 none 主题", messageCount);
    }

    /**
     * 发送测试消息到通用测试主题
     */
    public void sendTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到测试主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "test-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Test Message " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.TEST_TOPIC, key, value);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到测试主题", messageCount);
    }
}
```

### 7. REST控制器

`controller/KafkaOffsetController.java`：提供GET接口测试不同偏移量策略

```java
package com.action.kafka04offset.controller;

import com.action.kafka04offset.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka/offset")
@RequiredArgsConstructor
public class KafkaOffsetController {

    private final KafkaProducerService kafkaProducerService;

    /**
     * 发送测试消息到 earliest 主题
     */
    @GetMapping("/earliest/send")
    public ResponseEntity<Map<String, Object>> sendEarliestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 earliest 主题", count);
        
        try {
            kafkaProducerService.sendEarliestTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 earliest 主题");
            response.put("topic", "kafka-04-offset-earliest");
            response.put("count", count);
            response.put("offsetStrategy", "earliest");
            response.put("description", "earliest 策略：自动将偏移量重置为最早的偏移量，会消费所有历史消息");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 earliest 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到 latest 主题
     */
    @GetMapping("/latest/send")
    public ResponseEntity<Map<String, Object>> sendLatestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 latest 主题", count);
        
        try {
            kafkaProducerService.sendLatestTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 latest 主题");
            response.put("topic", "kafka-04-offset-latest");
            response.put("count", count);
            response.put("offsetStrategy", "latest");
            response.put("description", "latest 策略：自动将偏移量重置为最新偏移量，只消费新产生的消息");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 latest 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到 none 主题
     */
    @GetMapping("/none/send")
    public ResponseEntity<Map<String, Object>> sendNoneMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到 none 主题", count);
        
        try {
            kafkaProducerService.sendNoneTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到 none 主题");
            response.put("topic", "kafka-04-offset-none");
            response.put("count", count);
            response.put("offsetStrategy", "none");
            response.put("description", "none 策略：如果没有为消费者组找到以前的偏移量，则向消费者抛出异常");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送 none 消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送测试消息到通用测试主题
     */
    @GetMapping("/test/send")
    public ResponseEntity<Map<String, Object>> sendTestMessages(
            @RequestParam(defaultValue = "5") int count) {
        
        log.info("收到请求：发送 {} 条消息到测试主题", count);
        
        try {
            kafkaProducerService.sendTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功发送 " + count + " 条消息到测试主题");
            response.put("topic", "kafka-04-offset-test");
            response.put("count", count);
            response.put("description", "通用测试主题：所有消费者都会监听此主题，用于对比不同偏移量策略");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("发送测试消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送消息到所有主题
     */
    @GetMapping("/batch/send")
    public ResponseEntity<Map<String, Object>> sendBatchMessages(
            @RequestParam(defaultValue = "3") int count) {
        
        log.info("收到请求：批量发送消息，每个主题 {} 条", count);
        
        try {
            kafkaProducerService.sendEarliestTestMessages(count);
            kafkaProducerService.sendLatestTestMessages(count);
            kafkaProducerService.sendNoneTestMessages(count);
            kafkaProducerService.sendTestMessages(count);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "成功批量发送消息到所有主题");
            response.put("totalCount", count * 4);
            response.put("topics", new String[]{
                "kafka-04-offset-earliest",
                "kafka-04-offset-latest", 
                "kafka-04-offset-none",
                "kafka-04-offset-test"
            });
            response.put("countPerTopic", count);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("批量发送消息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "批量发送消息失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

}
```

## 偏移量策略说明

### 1. earliest 策略
- **配置值**: `auto-offset-reset: earliest`
- **行为**: 自动将偏移量重置为最早的偏移量
- **特点**: 会消费所有历史消息，包括在消费者启动前已存在的消息
- **适用场景**: 需要处理所有历史数据的场景

### 2. latest 策略
- **配置值**: `auto-offset-reset: latest`
- **行为**: 自动将偏移量重置为最新偏移量
- **特点**: 只消费新产生的消息，忽略历史消息
- **适用场景**: 只关心最新数据的场景

### 3. none 策略
- **配置值**: `auto-offset-reset: none`
- **行为**: 如果没有为消费者组找到以前的偏移量，则向消费者抛出异常
- **特点**: 要求消费者组必须已经存在偏移量，否则抛出异常
- **适用场景**: 需要严格控制偏移量管理的场景

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-04-offset

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送消息到earliest主题
```bash
# 浏览器访问
http://localhost:8080/api/kafka/offset/earliest/send?count=5

# 或使用curl
curl "http://localhost:8080/api/kafka/offset/earliest/send?count=5"
```

#### 发送消息到latest主题
```bash
http://localhost:8080/api/kafka/offset/latest/send?count=5
```

#### 发送消息到none主题
```bash
http://localhost:8080/api/kafka/offset/none/send?count=5
```

#### 发送消息到测试主题（所有消费者都会监听）
```bash
http://localhost:8080/api/kafka/offset/test/send?count=5
```

#### 批量发送消息到所有主题
```bash
http://localhost:8080/api/kafka/offset/batch/send?count=3
```


### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 消息发送日志
```
INFO  - 发送消息到主题: kafka-04-offset-earliest, 键: offset-test-earliest-1, 内容: ...
INFO  - 消息发送成功 - 主题: kafka-04-offset-earliest, 分区: 0, 偏移量: 123
```

#### 消费者消费日志
```
INFO  - === EARLIEST 偏移量策略消费者 ===
INFO  - 主题: kafka-04-offset-earliest
INFO  - 分区: 0
INFO  - 偏移量: 123
INFO  - 消息键: offset-test-earliest-1
INFO  - 消息内容: Offset Test Message - Earliest Test 1 - 2024-01-01 12:00:00
INFO  - 时间戳: 1704067200000
INFO  - 消费者组: kafka-04-earliest-group
```

### 4. 测试场景

#### 场景1：测试earliest策略
1. 启动应用（消费者自动启动）
2. 发送消息：`http://localhost:8080/api/kafka/offset/earliest/send?count=3`
3. 观察消费者是否消费了所有消息
4. 再次发送消息，观察是否继续消费新消息

#### 场景2：测试latest策略
1. 先发送消息：`http://localhost:8080/api/kafka/offset/latest/send?count=3`
2. 启动latest消费者（应用启动时自动启动）
3. 再发送新消息：`http://localhost:8080/api/kafka/offset/latest/send?count=2`
4. 观察消费者是否只消费了新发送的消息

#### 场景3：测试通用主题
1. 发送消息到测试主题：`http://localhost:8080/api/kafka/offset/test/send?count=5`
2. 观察三个不同策略消费者的日志输出
3. 对比不同策略的消费行为差异

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`192.168.56.10:9092`运行
2. **消费者组隔离**: 不同策略使用不同的消费者组，避免相互影响
3. **消息确认**: 使用手动确认模式（MANUAL），确保消息处理完成后再提交偏移量
4. **异常处理**: none策略在找不到偏移量时会抛出异常，需要适当的错误处理
5. **日志级别**: 建议使用DEBUG级别观察详细的消费行为
