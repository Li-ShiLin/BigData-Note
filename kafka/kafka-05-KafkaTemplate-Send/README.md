<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [生产者消息发送](#%E7%94%9F%E4%BA%A7%E8%80%85%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 常量定义](#3-%E5%B8%B8%E9%87%8F%E5%AE%9A%E4%B9%89)
    - [4. Kafka配置类](#4-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [5. 对象消息模型](#5-%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%A8%A1%E5%9E%8B)
      - [5.1 用户消息对象](#51-%E7%94%A8%E6%88%B7%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
      - [5.2 订单消息对象](#52-%E8%AE%A2%E5%8D%95%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
      - [5.3 系统事件消息对象](#53-%E7%B3%BB%E7%BB%9F%E4%BA%8B%E4%BB%B6%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
    - [6. 发送服务实现](#6-%E5%8F%91%E9%80%81%E6%9C%8D%E5%8A%A1%E5%AE%9E%E7%8E%B0)
      - [6.1 对象消息发送方法](#61-%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%96%B9%E6%B3%95)
    - [7. 消费者实现](#7-%E6%B6%88%E8%B4%B9%E8%80%85%E5%AE%9E%E7%8E%B0)
      - [7.1 对象消息消费者](#71-%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E8%80%85)
    - [6. 消费者实现](#6-%E6%B6%88%E8%B4%B9%E8%80%85%E5%AE%9E%E7%8E%B0)
    - [7. 统一响应类](#7-%E7%BB%9F%E4%B8%80%E5%93%8D%E5%BA%94%E7%B1%BB)
    - [8. 全局异常处理器](#8-%E5%85%A8%E5%B1%80%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86%E5%99%A8)
    - [9. REST控制器（改造后）](#9-rest%E6%8E%A7%E5%88%B6%E5%99%A8%E6%94%B9%E9%80%A0%E5%90%8E)
  - [KafkaTemplate Send方法详解](#kafkatemplate-send%E6%96%B9%E6%B3%95%E8%AF%A6%E8%A7%A3)
    - [1. send(String topic, V data)](#1-sendstring-topic-v-data)
    - [2. send(String topic, K key, V data)](#2-sendstring-topic-k-key-v-data)
    - [3. send(String topic, Integer partition, K key, V data)](#3-sendstring-topic-integer-partition-k-key-v-data)
    - [4. send(String topic, Integer partition, Long timestamp, K key, V data)](#4-sendstring-topic-integer-partition-long-timestamp-k-key-v-data)
    - [5. send(ProducerRecord<K, V> record)](#5-sendproducerrecordk-v-record)
    - [6. send(Message<?> message)](#6-sendmessage-message)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [3. 测试API接口](#3-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [测试方法1: 简单消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%951-%E7%AE%80%E5%8D%95%E6%B6%88%E6%81%AF)
      - [测试方法2: 键值消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%952-%E9%94%AE%E5%80%BC%E6%B6%88%E6%81%AF)
      - [测试方法3: 分区消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%953-%E5%88%86%E5%8C%BA%E6%B6%88%E6%81%AF)
      - [测试方法4: 时间戳消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%954-%E6%97%B6%E9%97%B4%E6%88%B3%E6%B6%88%E6%81%AF)
      - [测试方法5: ProducerRecord消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%955-producerrecord%E6%B6%88%E6%81%AF)
      - [测试方法6: Spring Message消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%956-spring-message%E6%B6%88%E6%81%AF)
      - [测试方法7: 默认主题简单消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%957-%E9%BB%98%E8%AE%A4%E4%B8%BB%E9%A2%98%E7%AE%80%E5%8D%95%E6%B6%88%E6%81%AF)
      - [测试方法8: 默认主题键值消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%958-%E9%BB%98%E8%AE%A4%E4%B8%BB%E9%A2%98%E9%94%AE%E5%80%BC%E6%B6%88%E6%81%AF)
      - [测试方法9: 默认主题分区消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%959-%E9%BB%98%E8%AE%A4%E4%B8%BB%E9%A2%98%E5%88%86%E5%8C%BA%E6%B6%88%E6%81%AF)
      - [测试方法10: 默认主题时间戳消息](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%9510-%E9%BB%98%E8%AE%A4%E4%B8%BB%E9%A2%98%E6%97%B6%E9%97%B4%E6%88%B3%E6%B6%88%E6%81%AF)
      - [批量发送所有类型的消息](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%89%80%E6%9C%89%E7%B1%BB%E5%9E%8B%E7%9A%84%E6%B6%88%E6%81%AF)
    - [对象消息发送API接口](#%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81api%E6%8E%A5%E5%8F%A3)
      - [对象消息发送案例1: 用户消息对象](#%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%A1%88%E4%BE%8B1-%E7%94%A8%E6%88%B7%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
      - [对象消息发送案例2: 订单消息对象](#%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%A1%88%E4%BE%8B2-%E8%AE%A2%E5%8D%95%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
      - [对象消息发送案例3: 系统事件消息对象](#%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%A1%88%E4%BE%8B3-%E7%B3%BB%E7%BB%9F%E4%BA%8B%E4%BB%B6%E6%B6%88%E6%81%AF%E5%AF%B9%E8%B1%A1)
      - [批量发送对象消息演示](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%BC%94%E7%A4%BA)
    - [4. 错误处理测试](#4-%E9%94%99%E8%AF%AF%E5%A4%84%E7%90%86%E6%B5%8B%E8%AF%95)
      - [参数校验错误](#%E5%8F%82%E6%95%B0%E6%A0%A1%E9%AA%8C%E9%94%99%E8%AF%AF)
      - [分区号错误](#%E5%88%86%E5%8C%BA%E5%8F%B7%E9%94%99%E8%AF%AF)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [消息发送日志](#%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%97%A5%E5%BF%97)
      - [消费者消费日志](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 生产者消息发送

## 项目作用

本项目演示了SpringBoot中KafkaTemplate的各种send重载方法的使用，包括10种不同的发送方式，以及3种对象消息发送的最佳实践案例，帮助开发者理解KafkaTemplate的灵活性和不同场景下的使用方法。

## 项目结构

```
kafka-05-KafkaTemplate-Send/
├── src/main/java/com/action/kafka05kafkatemplatesend/
│   ├── config/
│   │   └── KafkaConfig.java              # Kafka配置类
│   ├── constants/
│   │   └── KafkaConstants.java           # 常量定义
│   ├── consumer/
│   │   └── KafkaTemplateConsumer.java    # 消费者实现
│   ├── controller/
│   │   └── KafkaTemplateController.java  # REST API控制器
│   ├── model/
│   │   ├── UserMessage.java              # 用户消息对象
│   │   ├── OrderMessage.java             # 订单消息对象
│   │   └── SystemEventMessage.java       # 系统事件消息对象
│   ├── service/
│   │   └── KafkaTemplateSendService.java # 发送服务实现
│   └── Kafka05KafkaTemplateSendApplication.java # 主启动类
├── src/main/resources/
│   └── application.properties            # 配置文件
└── pom.xml                              # Maven配置

kafka-00-common/ (公共模块)
├── src/main/java/com/action/kafka0common/
│   ├── common/
│   │   ├── R.java                       # 统一响应类
│   │   └── Constants.java               # 常量定义
│   ├── exception/
│   │   ├── BusinessException.java       # 业务异常类
│   │   └── GlobalExceptionHandler.java  # 全局异常处理器
│   └── Kafka00CommonApplication.java    # 主启动类
└── pom.xml                              # Maven配置
```

## 核心实现

### 1. 依赖配置

`pom.xml`：引入Kafka相关依赖和公共模块

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

    <!-- 引入公共模块 -->
    <dependency>
        <groupId>com.action</groupId>
        <artifactId>kafka-00-common</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.properties`：Kafka服务器和消费者配置（已优化消费者重平衡问题）

```properties
spring.application.name=kafka-05-KafkaTemplate-Send
# 服务器端口配置
server.port=8095
# Kafka 服务器配置
spring.kafka.bootstrap-servers=192.168.56.10:9092
# 生产者配置
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3
spring.kafka.producer.batch-size=16384
spring.kafka.producer.linger-ms=1
spring.kafka.producer.buffer-memory=33554432
# 消费者配置
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.group-id=kafka-05-template-send-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.auto-commit-interval-ms=1000
spring.kafka.consumer.session-timeout-ms=30000
spring.kafka.consumer.max-poll-records=10
spring.kafka.consumer.max-poll-interval-ms=600000
spring.kafka.consumer.heartbeat-interval-ms=3000
# 监听器配置
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
# 日志配置
logging.level.com.action.kafka05kafkatemplatesend=INFO
logging.level.org.springframework.kafka=WARN
logging.level.org.apache.kafka=WARN
```

**配置优化说明**：

- `max.poll.records=500`: 每次最多拉取500条，兼顾吞吐与处理时延
- `max.poll.interval.ms=300000`: 将轮询最大间隔设置为5分钟，降低重平衡概率
- `heartbeat-interval-ms=3000`: 设置3秒心跳间隔，保持连接活跃
- 日志级别调整为INFO/WARN，减少I/O开销

### 3. 常量定义

`constants/KafkaConstants.java`：统一管理主题名称和常量

```java
package com.action.kafka05kafkatemplatesend.constants;

public class KafkaConstants {

    /**
     * 主题名称
     */
    public static final String TOPIC_SIMPLE = "kafka-05-simple-topic";
    public static final String TOPIC_KEY_VALUE = "kafka-05-key-value-topic";
    public static final String TOPIC_PARTITION = "kafka-05-partition-topic";
    public static final String TOPIC_TIMESTAMP = "kafka-05-timestamp-topic";
    public static final String TOPIC_PRODUCER_RECORD = "kafka-05-producer-record-topic";
    public static final String TOPIC_MESSAGE = "kafka-05-message-topic";

    /**
     * 消费者组名称
     */
    public static final String CONSUMER_GROUP = "kafka-05-template-send-group";

    /**
     * 消息键前缀
     */
    public static final String MESSAGE_KEY_PREFIX = "template-send-";

    /**
     * 消息值前缀
     */
    public static final String MESSAGE_VALUE_PREFIX = "KafkaTemplate Send Test - ";
}
```

### 4. Kafka配置类

`config/KafkaConfig.java`：配置KafkaTemplate和消费者

```java
package com.action.kafka05kafkatemplatesend.config;

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
     * 消费者配置
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-05-template-send-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 消费者容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```

### 5. 对象消息模型

#### 5.1 用户消息对象

`model/UserMessage.java`：用户消息对象，用于演示简单对象消息发送

```java

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserMessage {
    private Long userId;
    private String username;
    private String content;
    private String messageType;
    private LocalDateTime createTime;
    private String status;
}
```

#### 5.2 订单消息对象

`model/OrderMessage.java`：订单消息对象，用于演示复杂对象消息发送

```java

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage {
    private String orderId;
    private Long userId;
    private String status;
    private BigDecimal totalAmount;
    private List<OrderItem> items;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private String remark;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        private String productId;
        private String productName;
        private BigDecimal price;
        private Integer quantity;
        private BigDecimal subtotal;
    }
}
```

#### 5.3 系统事件消息对象

`model/SystemEventMessage.java`：系统事件消息对象，用于演示系统事件消息发送

```java

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemEventMessage {
    private String eventId;
    private String eventType;
    private String level;
    private String description;
    private LocalDateTime eventTime;
    private String serviceName;
    private String hostIp;
    private Long userId;
    private Map<String, Object> attributes;
    private String status;
}
```

### 6. 发送服务实现

`service/KafkaTemplateSendService.java`：实现10种不同的KafkaTemplate send方法，以及3种对象消息发送方法

#### 6.1 对象消息发送方法

```java
/**
 * 发送用户消息对象
 * 演示使用CommonUtils进行对象转换
 */
public CompletableFuture<SendResult<String, String>> sendUserMessageObject(String topic, UserMessage userMessage) {
    log.info("=== 发送用户消息对象 ===");
    log.info("用户消息: {}", userMessage);

    // 使用CommonUtils.convert()将对象转换为JSON字符串
    String userMessageJson = CommonUtils.convert(userMessage, String.class);
    String key = "user-" + userMessage.getUserId();

    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, userMessageJson);

    future.whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("用户消息发送失败 - 用户ID: {}, 错误: {}", userMessage.getUserId(), ex.getMessage(), ex);
        } else if (result != null) {
            log.info("用户消息发送成功 - 用户ID: {}, 分区: {}, 偏移量: {}",
                    userMessage.getUserId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        }
    });

    return future;
}

/**
 * 发送订单消息对象
 * 演示使用CommonUtils进行复杂对象转换
 */
public CompletableFuture<SendResult<String, String>> sendOrderMessageObject(String topic, OrderMessage orderMessage) {
    log.info("=== 发送订单消息对象 ===");
    log.info("订单消息: {}", orderMessage);

    // 使用CommonUtils.convert()将复杂对象转换为JSON字符串
    String orderMessageJson = CommonUtils.convert(orderMessage, String.class);
    String key = "order-" + orderMessage.getOrderId();

    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, orderMessageJson);

    future.whenComplete((result, ex) -> {
        if (ex != null) {
            log.error("订单消息发送失败 - 订单ID: {}, 错误: {}", orderMessage.getOrderId(), ex.getMessage(), ex);
        } else if (result != null) {
            log.info("订单消息发送成功 - 订单ID: {}, 分区: {}, 偏移量: {}",
                    orderMessage.getOrderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        }
    });

    return future;
}
```

### 7. 消费者实现

`consumer/KafkaTemplateConsumer.java`：监听所有测试主题的消息，包括对象消息

#### 7.1 对象消息消费者

```java
/**
 * 监听用户消息对象主题
 */
@KafkaListener(
        topics = "user-message-topic",
        groupId = KafkaConstants.CONSUMER_GROUP,
        containerFactory = "kafkaListenerContainerFactory"
)
public void handleUserMessageObject(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset,
        @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
        ConsumerRecord<String, String> record,
        Acknowledgment acknowledgment) {

    log.info("=== 用户消息对象消费者 ===");
    log.info("主题: {}", topic);
    log.info("分区: {}", partition);
    log.info("偏移量: {}", offset);
    log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
    log.info("原始消息内容: {}", message);

    try {
        // 使用CommonUtils将JSON字符串转换为UserMessage对象
        UserMessage userMessage = CommonUtils.convertString(message, UserMessage.class);
        log.info("解析后的用户消息对象: {}", userMessage);
        log.info("用户ID: {}", userMessage.getUserId());
        log.info("用户名: {}", userMessage.getUsername());
        log.info("消息内容: {}", userMessage.getContent());
        log.info("消息类型: {}", userMessage.getMessageType());
        log.info("状态: {}", userMessage.getStatus());
        log.info("创建时间: {}", userMessage.getCreateTime());
    } catch (Exception e) {
        log.error("解析用户消息对象失败: {}", e.getMessage(), e);
    }

    // 手动确认消息
    if (acknowledgment != null) {
        acknowledgment.acknowledge();
    }
}
```

```java
package com.action.kafka05kafkatemplatesend.service;

import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaTemplateSendService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 测试方法1: send(String topic, V data)
     * 只发送消息值，不指定键
     */
    public CompletableFuture<SendResult<String, String>> sendSimpleMessage(String topic, String message) {
        log.info("=== 测试方法1: send(topic, data) ===");
        log.info("主题: {}, 消息: {}", topic, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("简单消息发送失败 - 主题: {}, 错误: {}", topic, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("简单消息发送成功 - 主题: {}, 分区: {}, 偏移量: {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法2: send(String topic, K key, V data)
     * 发送消息键和消息值
     */
    public CompletableFuture<SendResult<String, String>> sendKeyValueMessage(String topic, String key, String message) {
        log.info("=== 测试方法2: send(topic, key, data) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("键值消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("键值消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法3: send(String topic, Integer partition, K key, V data)
     * 指定分区发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendPartitionMessage(String topic, Integer partition, String key, String message) {
        log.info("=== 测试方法3: send(topic, partition, key, data) ===");
        log.info("主题: {}, 分区: {}, 键: {}, 消息: {}", topic, partition, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, partition, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("分区消息发送失败 - 主题: {}, 分区: {}, 键: {}, 错误: {}", topic, partition, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("分区消息发送成功 - 主题: {}, 分区: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        topic, partition, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法4: send(String topic, Integer partition, Long timestamp, K key, V data)
     * 指定分区和时间戳发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendTimestampMessage(String topic, Integer partition, Long timestamp, String key, String message) {
        log.info("=== 测试方法4: send(topic, partition, timestamp, key, data) ===");
        log.info("主题: {}, 分区: {}, 时间戳: {}, 键: {}, 消息: {}", topic, partition, timestamp, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, partition, timestamp, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("时间戳消息发送失败 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 错误: {}",
                        topic, partition, timestamp, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("时间戳消息发送成功 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        topic, partition, timestamp, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法5: send(ProducerRecord<K, V> record)
     * 使用ProducerRecord发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendProducerRecordMessage(String topic, String key, String message) {
        log.info("=== 测试方法5: send(ProducerRecord) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("ProducerRecord消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("ProducerRecord消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法6: send(Message<?> message)
     * 使用Spring Message发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendSpringMessage(String topic, String key, String message) {
        log.info("=== 测试方法6: send(Message) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        Message<String> springMessage = MessageBuilder
                .withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.CORRELATION_ID, "correlation-" + System.currentTimeMillis())
                .setHeader("custom-header", "custom-value")
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(springMessage);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Spring Message消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("Spring Message消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 批量发送测试消息
     */
    public void sendBatchTestMessages() {
        log.info("开始批量发送测试消息...");

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // 测试方法1: 简单消息
        for (int i = 1; i <= 3; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Simple Message " + i + " - " + timestamp;
            sendSimpleMessage(KafkaConstants.TOPIC_SIMPLE, message);
        }

        // 测试方法2: 键值消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "key-value-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Key-Value Message " + i + " - " + timestamp;
            sendKeyValueMessage(KafkaConstants.TOPIC_KEY_VALUE, key, message);
        }

        // 测试方法3: 分区消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "partition-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Partition Message " + i + " - " + timestamp;
            sendPartitionMessage(KafkaConstants.TOPIC_PARTITION, 0, key, message);
        }

        // 测试方法4: 时间戳消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "timestamp-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Timestamp Message " + i + " - " + timestamp;
            long timestampValue = System.currentTimeMillis() + i * 1000; // 递增时间戳
            sendTimestampMessage(KafkaConstants.TOPIC_TIMESTAMP, 0, timestampValue, key, message);
        }

        // 测试方法5: ProducerRecord消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "producer-record-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "ProducerRecord Message " + i + " - " + timestamp;
            sendProducerRecordMessage(KafkaConstants.TOPIC_PRODUCER_RECORD, key, message);
        }

        // 测试方法6: Spring Message消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "spring-message-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Spring Message " + i + " - " + timestamp;
            sendSpringMessage(KafkaConstants.TOPIC_MESSAGE, key, message);
        }

        log.info("批量发送测试消息完成");
    }
}
```

### 6. 消费者实现

`consumer/KafkaTemplateConsumer.java`：监听所有测试主题的消息

```java
package com.action.kafka05kafkatemplatesend.consumer;

import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
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
public class KafkaTemplateConsumer {

    /**
     * 监听简单消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_SIMPLE,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSimpleMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 简单消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("========================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    // ... 其他消费者方法类似
}
```

### 7. 统一响应类

`kafka-00-common/common/R.java`：统一响应格式

```java
package com.action.kafka0common.common;

import java.io.Serializable;

public class R<T> implements Serializable {

    private String status;
    private String msg;
    private T data;

    public static <T> R<T> success() {
        return new R<>("0000", "操作成功");
    }

    public static <T> R<T> success(T data) {
        return new R<>("0000", "操作成功", data);
    }

    public static <T> R<T> fail(String code, String msg) {
        return new R<>(code, msg);
    }

    // getter/setter方法...
}
```

### 8. 全局异常处理器

`kafka-00-common/exception/GlobalExceptionHandler.java`：统一异常处理

```java
package com.action.kafka0common.exception;

import com.action.kafka0common.common.R;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({MethodArgumentNotValidException.class})
    @ResponseBody
    public R bindExceptionHandler(MethodArgumentNotValidException e) {
        // 参数校验异常处理
        return R.fail("9999", "参数校验失败");
    }

    @ExceptionHandler(value = BusinessException.class)
    @ResponseBody
    public R businessExceptionHandler(BusinessException e) {
        // 业务异常处理
        return R.fail(e.getCode(), e.getMessage());
    }

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public R systemError(Exception e) {
        // 系统异常处理
        return R.fail("9999", "系统异常");
    }
}
```

### 9. REST控制器（改造后）

`controller/KafkaTemplateController.java`：使用统一响应格式和异常处理

```java
package com.action.kafka05kafkatemplatesend.controller;

import com.action.kafka0common.common.R;
import com.action.kafka0common.exception.BusinessException;
import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
import com.action.kafka05kafkatemplatesend.service.KafkaTemplateSendService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka/template")
@RequiredArgsConstructor
public class KafkaTemplateController {

    private final KafkaTemplateSendService kafkaTemplateSendService;

    /**
     * 测试方法1: send(String topic, V data) - 简单消息
     */
    @GetMapping("/send/simple")
    public R<Map<String, Object>> sendSimpleMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条简单消息", count);

        // 参数校验
        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Simple Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendSimpleMessage(KafkaConstants.TOPIC_SIMPLE, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条简单消息");
        data.put("topic", KafkaConstants.TOPIC_SIMPLE);
        data.put("count", count);
        data.put("method", "send(topic, data)");
        data.put("description", "只发送消息值，不指定键");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    // ... 其他接口方法类似，都使用R<T>作为返回类型
}
```

## KafkaTemplate Send方法详解

### 1. send(String topic, V data)

- **用途**: 只发送消息值，不指定键
- **适用场景**: 简单的消息发送，不需要分区控制
- **特点**: 消息键为null，分区由Kafka自动分配

### 2. send(String topic, K key, V data)

- **用途**: 发送消息键和消息值
- **适用场景**: 需要根据键进行分区控制的消息
- **特点**: 相同键的消息会发送到同一分区

### 3. send(String topic, Integer partition, K key, V data)

- **用途**: 指定分区发送消息
- **适用场景**: 需要精确控制消息分区的场景
- **特点**: 可以指定消息发送到特定分区

### 4. send(String topic, Integer partition, Long timestamp, K key, V data)

- **用途**: 指定分区和时间戳发送消息
- **适用场景**: 需要控制消息时间戳的场景
- **特点**: 可以自定义消息的时间戳

### 5. send(ProducerRecord<K, V> record)

- **用途**: 使用ProducerRecord发送消息
- **适用场景**: 需要更精细控制消息属性的场景
- **特点**: 可以设置消息的所有属性

### 6. send(Message<?> message)

- **用途**: 使用Spring Message发送消息
- **适用场景**: 与Spring生态系统集成的场景
- **特点**: 支持Spring的消息头设置

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-05-KafkaTemplate-Send

# 启动应用
mvn spring-boot:run
```

- 查看统一响应格式示例

### 3. 测试API接口

#### 测试方法1: 简单消息

```bash
# 浏览器访问
http://localhost:8095/api/kafka/template/send/simple?count=3

# 或使用curl
curl "http://localhost:8095/api/kafka/template/send/simple?count=3"
```

**响应格式**：

```json
{
  "status": "0000",
  "msg": "操作成功",
  "data": {
    "message": "成功发送 3 条简单消息",
    "topic": "kafka-05-simple-topic",
    "count": 3,
    "method": "send(topic, data)",
    "description": "只发送消息值，不指定键",
    "timestamp": "2024-01-01 12:00:00"
  }
}
```

#### 测试方法2: 键值消息

```bash
http://localhost:8095/api/kafka/template/send/key-value?count=3
```

#### 测试方法3: 分区消息

```bash
http://localhost:8095/api/kafka/template/send/partition?count=3&partition=0
```

#### 测试方法4: 时间戳消息

```bash
http://localhost:8095/api/kafka/template/send/timestamp?count=3&partition=0
```

#### 测试方法5: ProducerRecord消息

```bash
http://localhost:8095/api/kafka/template/send/producer-record?count=3
```

#### 测试方法6: Spring Message消息

```bash
http://localhost:8095/api/kafka/template/send/spring-message?count=3
```

#### 测试方法7: 默认主题简单消息

```bash
http://localhost:8095/api/kafka/template/send/default-simple?count=3
```

**说明**: 使用`sendDefault(data)`方法，发送到默认主题`kafka-05-default-topic`

#### 测试方法8: 默认主题键值消息

```bash
http://localhost:8095/api/kafka/template/send/default-key-value?count=3
```

**说明**: 使用`sendDefault(key, data)`方法，发送到默认主题`kafka-05-default-topic`

#### 测试方法9: 默认主题分区消息

```bash
http://localhost:8095/api/kafka/template/send/default-partition?count=3&partition=0
```

**说明**: 使用`sendDefault(partition, key, data)`方法，发送到默认主题`kafka-05-default-topic`

#### 测试方法10: 默认主题时间戳消息

```bash
http://localhost:8095/api/kafka/template/send/default-timestamp?count=3&partition=0
```

**说明**: 使用`sendDefault(partition, timestamp, key, data)`方法，发送到默认主题`kafka-05-default-topic`

#### 批量发送所有类型的消息

```bash
http://localhost:8095/api/kafka/template/send/batch
```

**说明**: 批量发送所有10种类型的测试消息，每个主题发送3条消息，总共30条消息

### 对象消息发送API接口

#### 对象消息发送案例1: 用户消息对象

```bash
# 发送用户消息对象
curl -X POST "http://localhost:8095/api/kafka/template/send/object/user-message?count=3"

# 响应格式
{
  "status": "0000",
  "msg": "操作成功",
  "data": {
    "message": "成功发送 3 条用户消息对象",
    "topic": "user-message-topic",
    "count": 3,
    "objectType": "UserMessage",
    "description": "使用CommonUtils进行对象转换，发送用户消息对象",
    "timestamp": "2024-01-01 12:00:00"
  }
}
```

#### 对象消息发送案例2: 订单消息对象

```bash
# 发送订单消息对象
curl -X POST "http://localhost:8095/api/kafka/template/send/object/order-message?count=3"

# 响应格式
{
  "status": "0000",
  "msg": "操作成功",
  "data": {
    "message": "成功发送 3 条订单消息对象",
    "topic": "order-message-topic",
    "count": 3,
    "objectType": "OrderMessage",
    "description": "使用CommonUtils进行对象转换，发送复杂订单消息对象",
    "timestamp": "2024-01-01 12:00:00"
  }
}
```

#### 对象消息发送案例3: 系统事件消息对象

```bash
# 发送系统事件消息对象
curl -X POST "http://localhost:8095/api/kafka/template/send/object/system-event-message?count=3"

# 响应格式
{
  "status": "0000",
  "msg": "操作成功",
  "data": {
    "message": "成功发送 3 条系统事件消息对象",
    "topic": "system-event-topic",
    "count": 3,
    "objectType": "SystemEventMessage",
    "description": "使用CommonUtils进行对象转换，发送系统事件消息对象",
    "timestamp": "2024-01-01 12:00:00"
  }
}
```

#### 批量发送对象消息演示

```bash
# 批量发送对象消息演示
curl -X POST "http://localhost:8095/api/kafka/template/send/object/batch"

# 响应格式
{
  "status": "0000",
  "msg": "操作成功",
  "data": {
    "message": "成功批量发送对象消息演示",
    "topics": [
      "user-message-topic",
      "order-message-topic",
      "system-event-topic"
    ],
    "countPerTopic": 3,
    "totalCount": 9,
    "objectTypes": [
      "UserMessage",
      "OrderMessage",
      "SystemEventMessage"
    ],
    "description": "演示使用CommonUtils进行对象转换，发送不同类型的对象消息",
    "timestamp": "2024-01-01 12:00:00"
  }
}
```

### 4. 错误处理测试

#### 参数校验错误

```bash
# 测试消息数量超出范围
curl "http://localhost:8095/api/kafka/template/send/simple?count=101"

# 响应格式
{
  "status": "9998",
  "msg": "消息数量必须在1-100之间",
  "data": null
}
```

#### 分区号错误

```bash
# 测试负数分区号
curl "http://localhost:8095/api/kafka/template/send/partition?count=3&partition=-1"

# 响应格式
{
  "status": "9997",
  "msg": "分区号不能为负数",
  "data": null
}
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 消息发送日志

```
INFO  - === 测试方法1: send(topic, data) ===
INFO  - 主题: kafka-05-simple-topic, 消息: KafkaTemplate Send Test - Simple Message 1
INFO  - 简单消息发送成功 - 主题: kafka-05-simple-topic, 分区: 0, 偏移量: 123
```

#### 消费者消费日志

```
INFO  - === 简单消息消费者 ===
INFO  - 主题: kafka-05-simple-topic
INFO  - 分区: 0
INFO  - 偏移量: 123
INFO  - 消息键: null
INFO  - 消息内容: KafkaTemplate Send Test - Simple Message 1
INFO  - 时间戳: 1704067200000
INFO  - 消费者组: kafka-05-template-send-group
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`192.168.56.10:9092`运行
2. **分区控制**: 指定分区时要注意主题的分区数量
3. **时间戳**: 自定义时间戳会影响消息的排序
4. **消息键**: 相同键的消息会发送到同一分区
5. **异步发送**: 所有send方法都返回CompletableFuture，支持异步处理
6. **错误处理**: 建议在whenComplete中处理发送结果和异常
7. **统一响应**: 所有API都使用统一的R<T>响应格式，便于前端处理
8. **异常处理**: 全局异常处理器会自动捕获并转换异常为统一响应格式
9. **参数校验**: 输入参数会自动进行校验，超出范围会抛出业务异常
10. **消费者重平衡**: 已优化消费者配置，减少重平衡发生，提高系统稳定性
11. **性能优化**: 使用较小的max.poll.records和较长的max.poll.interval.ms避免处理超时
12. **日志优化**: 调整日志级别减少I/O开销，提高消费者处理效率
13. **对象消息发送**: 使用CommonUtils进行对象转换，支持复杂对象的JSON序列化
14. **FastJSON集成**: 使用阿里巴巴FastJSON2提供高效的JSON处理能力
