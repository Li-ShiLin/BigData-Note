<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kafka发送对象消息](#kafka%E5%8F%91%E9%80%81%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 用户实体类](#3-%E7%94%A8%E6%88%B7%E5%AE%9E%E4%BD%93%E7%B1%BB)
    - [4. Kafka配置类](#4-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [5. 对象消息生产者服务](#5-%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E7%94%9F%E4%BA%A7%E8%80%85%E6%9C%8D%E5%8A%A1)
    - [6. 对象消息消费者](#6-%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E8%80%85)
    - [7. REST控制器](#7-rest%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [Kafka对象消息处理详解](#kafka%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E5%A4%84%E7%90%86%E8%AF%A6%E8%A7%A3)
    - [1. 对象序列化流程](#1-%E5%AF%B9%E8%B1%A1%E5%BA%8F%E5%88%97%E5%8C%96%E6%B5%81%E7%A8%8B)
    - [2. 对象反序列化流程](#2-%E5%AF%B9%E8%B1%A1%E5%8F%8D%E5%BA%8F%E5%88%97%E5%8C%96%E6%B5%81%E7%A8%8B)
    - [3. 消息发送方式](#3-%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81%E6%96%B9%E5%BC%8F)
      - [方式一：无键消息发送](#%E6%96%B9%E5%BC%8F%E4%B8%80%E6%97%A0%E9%94%AE%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81)
      - [方式二：带键消息发送](#%E6%96%B9%E5%BC%8F%E4%BA%8C%E5%B8%A6%E9%94%AE%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81)
      - [方式三：批量消息发送](#%E6%96%B9%E5%BC%8F%E4%B8%89%E6%89%B9%E9%87%8F%E6%B6%88%E6%81%AF%E5%8F%91%E9%80%81)
    - [4. 消息消费方式](#4-%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E6%96%B9%E5%BC%8F)
      - [方式一：字符串消息消费](#%E6%96%B9%E5%BC%8F%E4%B8%80%E5%AD%97%E7%AC%A6%E4%B8%B2%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9)
      - [方式二：对象消息消费](#%E6%96%B9%E5%BC%8F%E4%BA%8C%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送字符串消息测试](#%E5%8F%91%E9%80%81%E5%AD%97%E7%AC%A6%E4%B8%B2%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
      - [发送用户对象消息测试](#%E5%8F%91%E9%80%81%E7%94%A8%E6%88%B7%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
      - [发送带键的用户对象消息测试](#%E5%8F%91%E9%80%81%E5%B8%A6%E9%94%AE%E7%9A%84%E7%94%A8%E6%88%B7%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
      - [批量发送用户对象消息测试](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E7%94%A8%E6%88%B7%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
      - [发送自定义用户对象消息测试](#%E5%8F%91%E9%80%81%E8%87%AA%E5%AE%9A%E4%B9%89%E7%94%A8%E6%88%B7%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [字符串消息消费日志](#%E5%AD%97%E7%AC%A6%E4%B8%B2%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
      - [用户对象消息消费日志](#%E7%94%A8%E6%88%B7%E5%AF%B9%E8%B1%A1%E6%B6%88%E6%81%AF%E6%B6%88%E8%B4%B9%E6%97%A5%E5%BF%97)
      - [异常处理演示日志](#%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86%E6%BC%94%E7%A4%BA%E6%97%A5%E5%BF%97)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kafka发送对象消息

## 项目作用

本项目演示了SpringBoot中Kafka对象消息的发送和监听消费，包括多主题隔离、对象序列化、反序列化、多种消息发送方式，帮助开发者理解Kafka对象消息处理的工作原理和实际应用场景。

## 项目结构

```
kafka-09-object-message/
├── src/main/java/com/action/kafka09objectmessage/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类（多主题配置）
│   ├── model/
│   │   └── User.java                           # 用户实体类
│   ├── service/
│   │   └── ObjectMessageProducerService.java   # 对象消息生产者服务（多主题发送）
│   ├── consumer/
│   │   └── ObjectMessageConsumer.java          # 对象消息消费者（多主题监听）
│   ├── controller/
│   │   └── ObjectMessageController.java        # REST API控制器
│   └── Kafka09ObjectMessageApplication.java    # 主启动类
├── src/main/resources/
│   └── application.yml                         # 配置文件（多主题配置）
├── src/test/java/com/action/kafka09objectmessage/
│   └── Kafka09ObjectMessageApplicationTests.java # 测试类
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
    
    <!-- FastJSON2 -->
    <dependency>
        <groupId>com.alibaba.fastjson2</groupId>
        <artifactId>fastjson2</artifactId>
    </dependency>
    
    <!-- 公共包依赖 -->
    <dependency>
        <groupId>com.action</groupId>
        <artifactId>kafka-00-common</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
    
    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.yml`：Kafka服务器和对象消息配置

```yaml
# ========================================
# Kafka 对象消息演示应用配置
# ========================================

# 应用名称
spring:
  application:
    name: kafka-09-object-message

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
      group-id: object-message-group
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
  port: 9099

# ========================================
# 自定义的配置
# ========================================
kafka:
  topic:
    # 字符串消息主题名称
    string: string-message-topic
    # 用户对象消息主题名称
    user: user-message-topic
    # 通用对象消息主题名称
    object: object-message-topic
  consumer:
    # 消费者组ID（与spring.kafka.consumer.group-id保持一致）
    group: object-message-group
```

### 3. 用户实体类

`model/User.java`：用于演示的对象模型

```java
package com.action.kafka09objectmessage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * 用户实体类
 * 
 * 功能说明：
 * 1) 用于演示 Kafka 对象消息的序列化和反序列化
 * 2) 包含基本的用户信息字段
 * 3) 实现 Serializable 接口，支持序列化
 * 
 * 实现细节：
 * - 使用 Lombok 注解简化代码编写
 * - @Builder: 提供建造者模式，便于对象构建
 * - @AllArgsConstructor: 生成全参构造函数
 * - @NoArgsConstructor: 生成无参构造函数
 * - @Data: 生成 getter、setter、toString、equals、hashCode 方法
 * - 实现 Serializable 接口，确保对象可以被序列化
 * 
 * 字段说明：
 * - id: 用户ID，唯一标识
 * - phone: 手机号码，联系方式
 * - birthDay: 生日，日期类型
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {

    /**
     * 用户ID
     * 唯一标识用户
     */
    private int id;

    /**
     * 手机号码
     * 用户联系方式
     */
    private String phone;

    /**
     * 生日
     * 用户出生日期
     */
    private Date birthDay;
}
```

### 4. Kafka配置类

`config/KafkaConfig.java`：配置Kafka主题创建

```java
package com.action.kafka09objectmessage.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 * 
 * 功能说明：
 * 1) 配置多个 Kafka 主题创建
 * 2) 为不同类型的消息定义不同的 Topic
 * 3) 支持自动创建 Topic，便于演示
 * 
 * 实现细节：
 * - 使用 @Configuration 注解标识为配置类
 * - 通过 @Value 注解读取配置文件中的 Topic 名称
 * - 使用 TopicBuilder 创建多个 Topic 定义
 * - 配置分区数和副本数，适合演示环境
 * 
 * 关键参数说明：
 * - partitions: 分区数，影响并行处理能力
 * - replicas: 副本数，影响可用性（生产环境建议 >= 3）
 */
@Configuration
public class KafkaConfig {

    /**
     * 字符串消息主题名称配置
     * 从 application.yml 读取，默认 string-message-topic
     */
    @Value("${kafka.topic.string:string-message-topic}")
    private String stringTopicName;

    /**
     * 用户对象消息主题名称配置
     * 从 application.yml 读取，默认 user-message-topic
     */
    @Value("${kafka.topic.user:user-message-topic}")
    private String userTopicName;

    /**
     * 通用对象消息主题名称配置
     * 从 application.yml 读取，默认 object-message-topic
     */
    @Value("${kafka.topic.object:object-message-topic}")
    private String objectTopicName;

    /**
     * 字符串消息演示用 Topic Bean
     * 
     * 作用：自动创建用于字符串消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic stringMessageTopic() {
        return TopicBuilder.name(stringTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 用户对象消息演示用 Topic Bean
     * 
     * 作用：自动创建用于用户对象消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic userMessageTopic() {
        return TopicBuilder.name(userTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 通用对象消息演示用 Topic Bean
     * 
     * 作用：自动创建用于通用对象消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic objectMessageTopic() {
        return TopicBuilder.name(objectTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
```

### 5. 对象消息生产者服务

`service/ObjectMessageProducerService.java`：封装KafkaTemplate对象消息发送逻辑

```java
package com.action.kafka09objectmessage.service;

import com.action.kafka0common.common.CommonUtils;
import com.action.kafka09objectmessage.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * 对象消息生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 对象消息发送逻辑
 * 2) 提供多种对象消息发送方式
 * 3) 使用公共包中的 CommonUtils 进行对象序列化
 * 4) 支持异步消息发送
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 使用 CommonUtils.convert() 方法将对象转换为 JSON 字符串
 * - 返回 CompletableFuture 支持异步处理和回调
 * 
 * 关键参数说明：
 * - KafkaTemplate: Spring 提供的 Kafka 操作模板，线程安全
 * - CommonUtils: 公共包中的工具类，用于对象序列化
 * - CompletableFuture: 异步编程模型，支持链式操作和异常处理
 */
@Service
public class ObjectMessageProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 字符串消息 Topic 名称
     * 从配置文件读取，默认 string-message-topic
     */
    @Value("${kafka.topic.string:string-message-topic}")
    private String stringTopicName;

    /**
     * 用户对象消息 Topic 名称
     * 从配置文件读取，默认 user-message-topic
     */
    @Value("${kafka.topic.user:user-message-topic}")
    private String userTopicName;

    /**
     * 通用对象消息 Topic 名称
     * 从配置文件读取，默认 object-message-topic
     */
    @Value("${kafka.topic.object:object-message-topic}")
    private String objectTopicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public ObjectMessageProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送简单字符串消息
     * 
     * 执行流程：
     * 1) 直接发送字符串消息到字符串消息主题
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param message 要发送的字符串消息
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendStringMessage(String message) {
        // 使用 KafkaTemplate 发送字符串消息到字符串消息主题
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(stringTopicName, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送用户对象消息（无键）
     * 
     * 执行流程：
     * 1) 创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 发送 JSON 字符串到 Kafka
     * 
     * @param user 要发送的用户对象
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserMessage(User user) {
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 发送 JSON 字符串到 Kafka
        return kafkaTemplate.send(topicName, userJson)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送带键的用户对象消息
     * 
     * 执行流程：
     * 1) 创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 使用指定的键发送消息到 Kafka
     * 
     * @param key 消息键（用于分区路由）
     * @param user 要发送的用户对象
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserMessageWithKey(String key, User user) {
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 使用指定的键发送 JSON 字符串到 Kafka
        return kafkaTemplate.send(topicName, key, userJson)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 批量发送用户对象消息
     * 
     * 执行流程：
     * 1) 循环创建多个 User 对象
     * 2) 为每个对象生成唯一的键
     * 3) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 4) 批量发送到 Kafka
     * 
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchUserMessages(int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 创建用户对象
            User user = User.builder()
                    .id(i)
                    .phone("1370909090" + i)
                    .birthDay(new Date())
                    .build();
            
            // 生成唯一的键
            String key = "user-" + i;
            
            // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
            String userJson = CommonUtils.convert(user, String.class);
            
            // 发送消息并存储 Future
            futures[i] = kafkaTemplate.send(topicName, key, userJson)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送自定义用户对象消息
     * 
     * 执行流程：
     * 1) 根据参数创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 发送到 Kafka
     * 
     * @param id 用户ID
     * @param phone 手机号码
     * @param key 消息键（可选）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendCustomUserMessage(int id, String phone, String key) {
        // 创建用户对象
        User user = User.builder()
                .id(id)
                .phone(phone)
                .birthDay(new Date())
                .build();
        
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 根据是否有键选择发送方式
        if (key != null && !key.isEmpty()) {
            return kafkaTemplate.send(topicName, key, userJson)
                    .thenApply(recordMetadata -> null);
        } else {
            return kafkaTemplate.send(topicName, userJson)
                    .thenApply(recordMetadata -> null);
        }
    }
}
```

### 6. 对象消息消费者

`consumer/ObjectMessageConsumer.java`：监听Kafka主题，接收对象消息

```java
package com.action.kafka09objectmessage.consumer;

import com.action.kafka0common.common.CommonUtils;
import com.action.kafka09objectmessage.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 对象消息消费者
 * 
 * 功能说明：
 * 1) 监听 Kafka 主题，接收对象消息
 * 2) 使用公共包中的 CommonUtils 进行对象反序列化
 * 3) 支持手动确认消息处理
 * 4) 提供多种消息处理方式
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听指定主题
 * - 使用 @Payload 注解获取消息内容
 * - 使用 @Header 注解获取消息元数据
 * - 使用 Acknowledgment 进行手动消息确认
 * - 使用 CommonUtils.convertString() 将 JSON 字符串转换为对象
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - @Payload: 获取消息体内容
 * - @Header: 获取消息头信息（主题、分区等）
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class ObjectMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(ObjectMessageConsumer.class);

    /**
     * 监听字符串消息主题，接收字符串消息
     * 
     * 执行流程：
     * 1) 监听 string-message-topic 主题
     * 2) 使用 stringGroup 消费者组
     * 3) 接收字符串消息并打印日志
     * 4) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload String event: 消息体内容（字符串）
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"string-message-topic"}, groupId = "stringGroup")
    public void onStringMessage(@Payload String event,
                               @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                               @Payload ConsumerRecord<String, String> record,
                               Acknowledgment ack) {
        try {
            // 打印接收到的字符串消息
            log.info("接收到字符串消息: {}, topic: {}, partition: {}", event, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 手动确认消息处理完成
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理字符串消息时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 监听用户对象消息主题，接收用户对象消息
     * 
     * 执行流程：
     * 1) 监听 user-message-topic 主题
     * 2) 使用 userGroup 消费者组
     * 3) 接收 JSON 字符串并转换为 User 对象
     * 4) 打印用户信息并手动确认消息
     * 
     * 参数说明：
     * - @Payload String userJson: 用户对象的 JSON 字符串
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"user-message-topic"}, groupId = "userGroup")
    public void onUserMessage(String userJson,
                             @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                             @Payload ConsumerRecord<String, String> record,
                             Acknowledgment ack) {
        try {
            // 使用公共包中的 CommonUtils 将 JSON 字符串转换为 User 对象
            User user = CommonUtils.convertString(userJson, User.class);
            
            // 打印接收到的用户对象消息
            log.info("接收到用户对象消息: {}, topic: {}, partition: {}", user, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 手动确认消息处理完成
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理用户对象消息时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 监听用户对象消息主题，接收用户对象消息（带异常处理演示）
     * 
     * 执行流程：
     * 1) 监听 user-message-topic 主题
     * 2) 使用 userExceptionGroup 消费者组
     * 3) 接收 JSON 字符串并转换为 User 对象
     * 4) 模拟业务处理（包含异常情况）
     * 5) 根据处理结果决定是否确认消息
     * 
     * 参数说明：
     * - @Payload String userJson: 用户对象的 JSON 字符串
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"user-message-topic"}, groupId = "userExceptionGroup")
    public void onUserMessageWithExceptionHandling(String userJson,
                                                  @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                                  @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                                  @Payload ConsumerRecord<String, String> record,
                                                  Acknowledgment ack) {
        try {
            // 使用公共包中的 CommonUtils 将 JSON 字符串转换为 User 对象
            User user = CommonUtils.convertString(userJson, User.class);
            
            // 打印接收到的用户对象消息
            log.info("接收到用户对象消息（异常处理演示）: {}, topic: {}, partition: {}", user, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 模拟业务处理
            // 这里可以添加实际的业务逻辑，如数据库操作、外部服务调用等
            processUserBusiness(user);
            
            // 业务处理完成，手动确认消息
            ack.acknowledge();
            log.info("用户对象消息处理完成并已确认: userId={}", user.getId());
            
        } catch (Exception e) {
            log.error("处理用户对象消息时发生业务错误: {}", e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 模拟用户业务处理
     * 
     * 功能说明：
     * 1) 模拟用户相关的业务处理逻辑
     * 2) 包含异常情况演示
     * 3) 用于演示消息处理失败时的行为
     * 
     * @param user 要处理的用户对象
     * @throws Exception 业务处理异常
     */
    private void processUserBusiness(User user) throws Exception {
        // 模拟业务处理逻辑
        log.info("开始处理用户业务: userId={}, phone={}", user.getId(), user.getPhone());
        
        // 模拟处理时间
        Thread.sleep(100);
        
        // 模拟异常情况（当用户ID为特定值时抛出异常）
        if (user.getId() == 999) {
            throw new RuntimeException("模拟业务处理异常：用户ID为999时处理失败");
        }
        
        // 模拟其他业务逻辑
        if (user.getPhone() == null || user.getPhone().isEmpty()) {
            throw new IllegalArgumentException("手机号码不能为空");
        }
        
        log.info("用户业务处理完成: userId={}", user.getId());
    }
}
```

### 7. REST控制器

`controller/ObjectMessageController.java`：提供HTTP接口用于测试对象消息发送

```java
package com.action.kafka09objectmessage.controller;

import com.action.kafka09objectmessage.model.User;
import com.action.kafka09objectmessage.service.ObjectMessageProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 对象消息演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试对象消息发送
 * 2) 演示多种对象消息发送方式
 * 3) 支持通过 URL 参数指定消息内容
 * 4) 提供 RESTful API 接口
 * 
 * 接口说明：
 * - GET /api/send/string: 发送字符串消息
 * - GET /api/send/user: 发送用户对象消息
 * - GET /api/send/user-with-key: 发送带键的用户对象消息
 * - GET /api/send/batch: 批量发送用户对象消息
 * - POST /api/send/custom-user: 发送自定义用户对象消息
 * 
 * 使用示例：
 * curl "http://localhost:9099/api/send/string?message=hello world"
 * curl "http://localhost:9099/api/send/user"
 * curl "http://localhost:9099/api/send/user-with-key?key=user-001"
 * curl "http://localhost:9099/api/send/batch?count=5"
 * curl -X POST "http://localhost:9099/api/send/custom-user" -H "Content-Type: application/json" -d '{"id":123,"phone":"13800138000"}'
 */
@RestController
@RequestMapping("/api/send")
public class ObjectMessageController {

    /**
     * 对象消息生产者服务
     * 负责实际的消息发送逻辑
     */
    private final ObjectMessageProducerService producerService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerService 对象消息生产者服务
     */
    public ObjectMessageController(ObjectMessageProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * 发送字符串消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/string
     * 
     * 参数说明：
     * - message: 要发送的字符串消息（必填）
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param message 要发送的字符串消息
     * @return HTTP 响应实体
     */
    @GetMapping("/string")
    public ResponseEntity<Map<String, Object>> sendStringMessage(@RequestParam("message") String message) {
        try {
            // 调用服务层发送字符串消息
            producerService.sendStringMessage(message);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "字符串消息发送成功");
            response.put("data", message);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "字符串消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/user
     * 
     * 功能说明：
     * - 创建一个默认的用户对象
     * - 将对象序列化为 JSON 字符串
     * - 发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @return HTTP 响应实体
     */
    @GetMapping("/user")
    public ResponseEntity<Map<String, Object>> sendUserMessage() {
        try {
            // 创建默认用户对象
            User user = User.builder()
                    .id(1001)
                    .phone("13709090909")
                    .birthDay(new Date())
                    .build();
            
            // 调用服务层发送用户对象消息
            producerService.sendUserMessage(user);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "用户对象消息发送成功");
            response.put("data", user);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送带键的用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/user-with-key
     * 
     * 参数说明：
     * - key: 消息键（可选），用于分区路由
     * 
     * 功能说明：
     * - 创建一个默认的用户对象
     * - 使用指定的键发送消息
     * - 键用于分区路由和消息去重
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param key 消息键（可选）
     * @return HTTP 响应实体
     */
    @GetMapping("/user-with-key")
    public ResponseEntity<Map<String, Object>> sendUserMessageWithKey(@RequestParam(value = "key", required = false) String key) {
        try {
            // 创建默认用户对象
            User user = User.builder()
                    .id(1002)
                    .phone("13709090908")
                    .birthDay(new Date())
                    .build();
            
            // 如果没有提供键，使用默认值
            if (key == null || key.isEmpty()) {
                key = "default-user-key";
            }
            
            // 调用服务层发送带键的用户对象消息
            producerService.sendUserMessageWithKey(key, user);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "带键的用户对象消息发送成功");
            response.put("data", Map.of("key", key, "user", user));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "带键的用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 批量发送用户对象消息接口
     * 
     * 请求方式：GET
     * 请求路径：/api/send/batch
     * 
     * 参数说明：
     * - count: 要发送的消息数量（可选，默认 5）
     * 
     * 功能说明：
     * - 创建指定数量的用户对象
     * - 为每个对象生成唯一的键
     * - 批量发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param count 要发送的消息数量
     * @return HTTP 响应实体
     */
    @GetMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchUserMessages(@RequestParam(value = "count", defaultValue = "5") int count) {
        try {
            // 限制批量发送数量，避免过载
            if (count <= 0 || count > 100) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "批量发送数量必须在 1-100 之间");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 调用服务层批量发送用户对象消息
            producerService.sendBatchUserMessages(count);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "批量用户对象消息发送成功");
            response.put("data", Map.of("count", count));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "批量用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 发送自定义用户对象消息接口
     * 
     * 请求方式：POST
     * 请求路径：/api/send/custom-user
     * 
     * 请求体说明：
     * - id: 用户ID（必填）
     * - phone: 手机号码（必填）
     * - key: 消息键（可选）
     * 
     * 功能说明：
     * - 根据请求参数创建用户对象
     * - 将对象序列化为 JSON 字符串
     * - 发送到 Kafka 主题
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * @param request 包含用户信息的请求体
     * @return HTTP 响应实体
     */
    @PostMapping("/custom-user")
    public ResponseEntity<Map<String, Object>> sendCustomUserMessage(@RequestBody Map<String, Object> request) {
        try {
            // 验证必需参数
            if (!request.containsKey("id") || !request.containsKey("phone")) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "缺少必需参数: id 和 phone");
                
                return ResponseEntity.badRequest().body(response);
            }
            
            // 提取参数
            int id = Integer.parseInt(request.get("id").toString());
            String phone = request.get("phone").toString();
            String key = request.containsKey("key") ? request.get("key").toString() : null;
            
            // 调用服务层发送自定义用户对象消息
            producerService.sendCustomUserMessage(id, phone, key);
            
            // 返回成功响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "自定义用户对象消息发送成功");
            response.put("data", request);
            
            return ResponseEntity.ok(response);
        } catch (NumberFormatException e) {
            // 返回参数格式错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "参数格式错误: id 必须是数字");
            
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            // 返回错误响应
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "自定义用户对象消息发送失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
```

## Kafka对象消息处理详解

### 1. 对象序列化流程

```
Java对象 → CommonUtils.convert() → JSON字符串 → KafkaTemplate.send() → Kafka主题
```

### 2. 对象反序列化流程

```
Kafka主题 → @KafkaListener → JSON字符串 → CommonUtils.convertString() → Java对象
```

### 3. 消息发送方式

#### 方式一：无键消息发送

```java
// 发送无键的字符串消息
producerService.sendStringMessage("Hello World");

// 发送无键的用户对象消息
User user = User.builder().id(1).phone("13800138000").birthDay(new Date()).build();
producerService.sendUserMessage(user);
```

#### 方式二：带键消息发送

```java
// 发送带键的用户对象消息
producerService.sendUserMessageWithKey("user-001", user);
```

#### 方式三：批量消息发送

```java
// 批量发送用户对象消息
producerService.sendBatchUserMessages(10);
```

### 4. 消息消费方式

#### 方式一：字符串消息消费

```java
@KafkaListener(topics = {"object-message-topic"}, groupId = "helloGroup")
public void onStringMessage(@Payload String event, Acknowledgment ack) {
    // 处理字符串消息
    ack.acknowledge();
}
```

#### 方式二：对象消息消费

```java
@KafkaListener(topics = {"object-message-topic"}, groupId = "helloGroup2")
public void onUserMessage(String userJson, Acknowledgment ack) {
    // 将JSON字符串转换为User对象
    User user = CommonUtils.convertString(userJson, User.class);
    // 处理用户对象
    ack.acknowledge();
}
```

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-09-object-message

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送字符串消息测试

```bash
# 发送字符串消息
curl "http://localhost:9099/api/send/string?message=hello world"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "字符串消息发送成功",
  "data": "hello world"
}
```

#### 发送用户对象消息测试

```bash
# 发送用户对象消息
curl "http://localhost:9099/api/send/user"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "用户对象消息发送成功",
  "data": {
    "id": 1001,
    "phone": "13709090909",
    "birthDay": "2024-01-01T00:00:00.000+00:00"
  }
}
```

#### 发送带键的用户对象消息测试

```bash
# 发送带键的用户对象消息
curl "http://localhost:9099/api/send/user-with-key?key=user-001"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "带键的用户对象消息发送成功",
  "data": {
    "key": "user-001",
    "user": {
      "id": 1002,
      "phone": "13709090908",
      "birthDay": "2024-01-01T00:00:00.000+00:00"
    }
  }
}
```

#### 批量发送用户对象消息测试

```bash
# 批量发送用户对象消息
curl "http://localhost:9099/api/send/batch?count=5"
```

**响应格式**：

```json
{
  "status": "success",
  "message": "批量用户对象消息发送成功",
  "data": {
    "count": 5
  }
}
```

#### 发送自定义用户对象消息测试

```bash
# 发送自定义用户对象消息
curl -X POST "http://localhost:9099/api/send/custom-user" \
  -H "Content-Type: application/json" \
  -d '{"id":123,"phone":"13800138000","key":"custom-key"}'
```

**响应格式**：

```json
{
  "status": "success",
  "message": "自定义用户对象消息发送成功",
  "data": {
    "id": 123,
    "phone": "13800138000",
    "key": "custom-key"
  }
}
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 字符串消息消费日志

```
INFO  - 接收到字符串消息: hello world, topic: object-message-topic, partition: 0
INFO  - 完整消息记录: ConsumerRecord(topic = object-message-topic, partition = 0, offset = 123, CreateTime = 1640995200000, serialized key size = -1, serialized value size = 11, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = hello world)
```

#### 用户对象消息消费日志

```
INFO  - 接收到用户对象消息: User(id=1001, phone=13709090909, birthDay=Mon Jan 01 00:00:00 CST 2024), topic: object-message-topic, partition: 1
INFO  - 完整消息记录: ConsumerRecord(topic = object-message-topic, partition = 1, offset = 124, CreateTime = 1640995200000, serialized key size = -1, serialized value size = 89, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = {"id":1001,"phone":"13709090909","birthDay":"2024-01-01T00:00:00.000+00:00"})
```

#### 异常处理演示日志

```
INFO  - 接收到用户对象消息（异常处理演示）: User(id=999, phone=13709090999, birthDay=Mon Jan 01 00:00:00 CST 2024), topic: object-message-topic, partition: 2
INFO  - 开始处理用户业务: userId=999, phone=13709090999
ERROR - 处理用户对象消息时发生业务错误: 模拟业务处理异常：用户ID为999时处理失败
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9092`运行
2. **主题隔离**: 不同类型的消息使用不同的主题，避免消息类型混淆
3. **对象序列化**: 使用公共包中的CommonUtils进行对象序列化和反序列化
4. **消息确认**: 消费者使用手动确认模式，需要调用ack.acknowledge()
5. **异常处理**: 消息处理失败时不会确认消息，会被重新消费
6. **消费者组**: 不同消费者组可以独立消费消息
7. **分区路由**: 带键的消息会根据键的哈希值路由到特定分区
8. **批量发送**: 批量发送时注意控制数量，避免过载
9. **JSON格式**: 对象序列化为JSON字符串，确保字段类型兼容
10. **线程安全**: KafkaTemplate是线程安全的，可以并发使用
11. **配置管理**: 建议将Topic名称等配置放在配置文件中管理
