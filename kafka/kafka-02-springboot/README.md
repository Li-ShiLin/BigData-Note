<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [SpringBoot集成Kafka](#springboot%E9%9B%86%E6%88%90kafka)
    - [目录结构](#%E7%9B%AE%E5%BD%95%E7%BB%93%E6%9E%84)
    - [运行环境](#%E8%BF%90%E8%A1%8C%E7%8E%AF%E5%A2%83)
    - [依赖配置（pom.xml）](#%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AEpomxml)
    - [配置文件（application.yml）](#%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6applicationyml)
    - [代码详解](#%E4%BB%A3%E7%A0%81%E8%AF%A6%E8%A7%A3)
      - [1) 启动类](#1-%E5%90%AF%E5%8A%A8%E7%B1%BB)
      - [2) Topic 常量定义](#2-topic-%E5%B8%B8%E9%87%8F%E5%AE%9A%E4%B9%89)
      - [3) Kafka 管理配置（自动创建 Topic）](#3-kafka-%E7%AE%A1%E7%90%86%E9%85%8D%E7%BD%AE%E8%87%AA%E5%8A%A8%E5%88%9B%E5%BB%BA-topic)
      - [4) 生产者（异步发送 + 同步示例）](#4-%E7%94%9F%E4%BA%A7%E8%80%85%E5%BC%82%E6%AD%A5%E5%8F%91%E9%80%81--%E5%90%8C%E6%AD%A5%E7%A4%BA%E4%BE%8B)
      - [5) 消费者（手动确认提交）](#5-%E6%B6%88%E8%B4%B9%E8%80%85%E6%89%8B%E5%8A%A8%E7%A1%AE%E8%AE%A4%E6%8F%90%E4%BA%A4)
      - [6) REST 控制器（发送消息接口）](#6-rest-%E6%8E%A7%E5%88%B6%E5%99%A8%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E6%8E%A5%E5%8F%A3)
    - [如何运行](#%E5%A6%82%E4%BD%95%E8%BF%90%E8%A1%8C)
    - [常见问题](#%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# SpringBoot集成Kafka

本模块演示使用 Spring Boot + Spring for Apache Kafka 完成最小可用的生产者/消费者示例：

- 启动时自动创建 Topic（通过 `KafkaAdmin` + `TopicBuilder`）
- 提供 REST 接口发送消息
- 消费端示例包含简单消费与携带元数据、手动确认提交偏移量示例

示例中的代码片段与工程源码完全一致（包含注释），便于对照学习。

---

### 目录结构

```
kafka-02-springboot/
├── pom.xml
├── README.md
├── src
│   └── main
│       ├── java/com/action/kafka02springboot/
│       │   ├── Kafka02SpringbootApplication.java
│       │   ├── config/
│       │   │   ├── KafkaAdminConfig.java
│       │   │   └── KafkaConstants.java
│       │   ├── controller/
│       │   │   └── KafkaController.java
│       │   ├── consumer/
│       │   │   └── KafkaConsumer.java
│       │   └── producer/
│       │       └── KafkaProducer.java
│       └── resources/application.yml
└── src/test/java/...（略）
```

---

### 运行环境

- JDK 17（与父工程一致）
- Maven 3.8+
- Kafka 集群（示例默认 `192.168.56.10:9092`，按需修改 `application.yml`）

---

### 依赖配置（pom.xml）

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- 继承父项目 -->
    <parent>
        <groupId>com.action</groupId>
        <artifactId>kafka</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </parent>

    <!-- 子模块信息 -->
    <artifactId>kafka-02-springboot</artifactId>
    <name>kafka-02-springboot</name>
    <description>Kafka Spring Boot 集成演示 - 展示Spring Boot与Kafka的集成使用</description>

    <!-- 依赖配置 -->
    <dependencies>
        <!-- Spring Boot Starter -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter</artifactId>
        </dependency>

        <!-- Spring Boot Web Starter -->
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
        </dependency>

        <!-- 测试依赖 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
        </dependency>
    </dependencies>

    <!-- 构建配置 -->
    <build>
        <plugins>
            <!-- Spring Boot Maven插件 -->
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>

</project>
```

---

### 配置文件（application.yml）

```yaml
server:
  port: 8090

spring:
  kafka:
    # Kafka服务器地址，多个用逗号分隔
    bootstrap-servers: 192.168.56.10:9092

    # 生产者配置
    producer:
      retries: 3 # 失败重试次数
      batch-size: 16384 # 批量大小
      buffer-memory: 33554432 # 缓冲区大小
      acks: 1 # 消息确认机制
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

    # 消费者配置
    consumer:
      group-id: demo-group # 消费者组ID
      auto-offset-reset: earliest # 偏移量重置策略
      enable-auto-commit: false # 是否自动提交偏移量
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

    # 监听器配置
    listener:
      ack-mode: MANUAL_IMMEDIATE # 手动立即提交偏移量

logging:
  level:
    org.apache.kafka.clients.NetworkClient: error
```

要点：

- 使用 `enable-auto-commit: false` + `ack-mode: MANUAL_IMMEDIATE`，消费者中通过 `Acknowledgment.acknowledge()` 手动提交偏移量。
- `bootstrap-servers` 按实际 Kafka 地址修改。

---

### 代码详解

#### 1) 启动类

```java
package com.action.kafka02springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Kafka02SpringbootApplication {

    public static void main(String[] args) {
        SpringApplication.run(Kafka02SpringbootApplication.class, args);
    }

}
```

#### 2) Topic 常量定义

```java
package com.action.kafka02springboot.config;


public class KafkaConstants {
    // Topic名称
    public static final String TOPIC_USERS = "users";
    public static final String TOPIC_ORDERS = "orders";

    // 消费者组
    public static final String CONSUMER_GROUP = "demo-group";

    // Topic配置
    public static final int USERS_TOPIC_PARTITIONS = 3;
    public static final short USERS_TOPIC_REPLICATION_FACTOR = 1;
}
```

#### 3) Kafka 管理配置（自动创建 Topic）

```java
package com.action.kafka02springboot.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(KafkaConstants.TOPIC_USERS)
                        .partitions(KafkaConstants.USERS_TOPIC_PARTITIONS)
                        .replicas(KafkaConstants.USERS_TOPIC_REPLICATION_FACTOR)
                        .build(),
                // 可以添加更多Topic
                TopicBuilder.name(KafkaConstants.TOPIC_ORDERS)
                        .partitions(5)
                        .replicas(1)
                        .build()
        );
    }
}
```

#### 4) 生产者（异步发送 + 同步示例）

```java
package com.action.kafka02springboot.producer;

import com.action.kafka02springboot.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;


@Service
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        log.info("#### → 发送消息: {}", message);

        // 异步发送
        kafkaTemplate.send(KafkaConstants.TOPIC_USERS, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        // 发送成功
                        log.info("消息发送成功: topic={}, partition={}, offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        // 发送失败
                        log.error("消息发送失败: {}", ex.getMessage());
                    }
                });
    }

    // 同步发送示例
    public void sendMessageSync(String message) throws Exception {
        SendResult<String, String> result = kafkaTemplate.send(KafkaConstants.TOPIC_USERS, message)
                .get(3, TimeUnit.SECONDS);
        log.info("同步发送成功: {}", result.getRecordMetadata().offset());
    }
}
```

#### 5) 消费者（手动确认提交）

```java
package com.action.kafka02springboot.consumer;

import com.action.kafka02springboot.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = KafkaConstants.TOPIC_USERS, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consume(String message) {
        log.info("#### → 消费消息: {}", message);
    }

    @KafkaListener(topics = KafkaConstants.TOPIC_USERS, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeWithMetadata(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());

            processMessage(record.value());
            ack.acknowledge();
            log.info("偏移量已提交");
        } catch (Exception e) {
            log.error("消息处理失败: {}", e.getMessage());
        }
    }

    private void processMessage(String message) {
        log.info("处理消息: {}", message);
    }
}
```

#### 6) REST 控制器（发送消息接口）

```java
package com.action.kafka02springboot.controller;

import com.action.kafka02springboot.producer.KafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducer kafkaProducer;

    public KafkaController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        try {
            kafkaProducer.sendMessage(message);
            return ResponseEntity.ok("消息已发送: " + message);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("发送失败: " + e.getMessage());
        }
    }
}
```

---

### 如何运行

1) 启动 Kafka（确保 `192.168.56.10:9092` 可达，或修改 `application.yml`）

2) 运行应用：

```bash
cd kafka-02-springboot
mvn spring-boot:run
```

3) 发送消息（REST）：

```bash
# 使用 curl（POST 表单参数 message）
curl -X POST "http://localhost:8090/api/kafka/send?message=hello-kafka"
```

4) 观察控制台日志（生产者发送成功日志、消费者消费日志、手动提交偏移量日志）。

---

### 常见问题

- 连接失败：请确认 `bootstrap-servers` 地址、端口是否正确，或是否需要配置 SASL/SSL。
- 主题不存在：本示例默认会自动创建主题，如禁用自动创建，请手动在集群中创建 `users` 与 `orders`。
- 消费无日志：请确认消费者组与监听器是否启动，`enable-auto-commit=false` 时需确保调用了 `ack.acknowledge()`。


