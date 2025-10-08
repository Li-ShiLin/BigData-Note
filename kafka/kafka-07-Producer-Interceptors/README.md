<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [生产者拦截器](#%E7%94%9F%E4%BA%A7%E8%80%85%E6%8B%A6%E6%88%AA%E5%99%A8)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 消息修改拦截器](#3-%E6%B6%88%E6%81%AF%E4%BF%AE%E6%94%B9%E6%8B%A6%E6%88%AA%E5%99%A8)
    - [4. 指标统计拦截器](#4-%E6%8C%87%E6%A0%87%E7%BB%9F%E8%AE%A1%E6%8B%A6%E6%88%AA%E5%99%A8)
    - [5. Kafka配置类](#5-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
    - [6. 生产者演示服务](#6-%E7%94%9F%E4%BA%A7%E8%80%85%E6%BC%94%E7%A4%BA%E6%9C%8D%E5%8A%A1)
    - [7. REST控制器](#7-rest%E6%8E%A7%E5%88%B6%E5%99%A8)
  - [Kafka Producer拦截器详解](#kafka-producer%E6%8B%A6%E6%88%AA%E5%99%A8%E8%AF%A6%E8%A7%A3)
    - [1. 拦截器接口说明](#1-%E6%8B%A6%E6%88%AA%E5%99%A8%E6%8E%A5%E5%8F%A3%E8%AF%B4%E6%98%8E)
    - [2. 拦截器执行流程](#2-%E6%8B%A6%E6%88%AA%E5%99%A8%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B)
    - [3. 拦截器配置方式](#3-%E6%8B%A6%E6%88%AA%E5%99%A8%E9%85%8D%E7%BD%AE%E6%96%B9%E5%BC%8F)
      - [方式一：Java配置（推荐）](#%E6%96%B9%E5%BC%8F%E4%B8%80java%E9%85%8D%E7%BD%AE%E6%8E%A8%E8%8D%90)
      - [方式二：属性配置](#%E6%96%B9%E5%BC%8F%E4%BA%8C%E5%B1%9E%E6%80%A7%E9%85%8D%E7%BD%AE)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送消息测试](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [消息修改日志](#%E6%B6%88%E6%81%AF%E4%BF%AE%E6%94%B9%E6%97%A5%E5%BF%97)
      - [发送成功日志](#%E5%8F%91%E9%80%81%E6%88%90%E5%8A%9F%E6%97%A5%E5%BF%97)
      - [统计报告日志（应用关闭时）](#%E7%BB%9F%E8%AE%A1%E6%8A%A5%E5%91%8A%E6%97%A5%E5%BF%97%E5%BA%94%E7%94%A8%E5%85%B3%E9%97%AD%E6%97%B6)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 生产者拦截器

## 项目作用

本项目演示了SpringBoot中Kafka Producer拦截器的自定义实现和使用，包括2种不同类型的拦截器，帮助开发者理解Kafka拦截器的工作原理和实际应用场景。

## 项目结构

```
kafka-07-ProducerInterceptors/
├── src/main/java/com/action/kafka07producerinterceptors/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类
│   ├── interceptor/
│   │   ├── ModifyRecordInterceptor.java        # 消息修改拦截器
│   │   └── MetricsProducerInterceptor.java     # 指标统计拦截器
│   ├── service/
│   │   └── ProducerDemoService.java            # 生产者演示服务
│   ├── controller/
│   │   └── ProducerDemoController.java         # REST API控制器
│   └── Kafka07ProducerInterceptorsApplication.java # 主启动类
├── src/main/resources/
│   └── application.properties                  # 配置文件
└── pom.xml                                    # Maven配置
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
    </dependency>
</dependencies>
```

### 2. 配置文件

`application.properties`：Kafka服务器和拦截器配置

```properties
# ========================================
# Kafka 生产者拦截器演示应用配置
# ========================================

# 应用名称
spring.application.name=kafka-07-ProducerInterceptors

# ========================================
# Kafka 连接配置
# ========================================
# Kafka 服务器地址（多个地址用逗号分隔）
# 默认：localhost:9097
spring.kafka.bootstrap-servers=localhost:9097

# ========================================
# 演示 Topic 配置
# ========================================
# 演示用的 Topic 名称
# 应用启动时会自动创建该 Topic（3个分区，1个副本）
demo.topic.name=demo-interceptor-topic

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
# 拦截器配置（方式一：属性配置）
# ========================================
# 自定义拦截器类列表（按执行顺序配置）
# 注意：如果 Java 配置类中也配置了拦截器，可能会重复
# 建议只使用一种配置方式，本示例同时演示两种方式
spring.kafka.producer.properties.interceptor.classes=com.action.kafka07producerinterceptors.interceptor.ModifyRecordInterceptor,com.action.kafka07producerinterceptors.interceptor.MetricsProducerInterceptor
```

### 3. 消息修改拦截器

`interceptor/ModifyRecordInterceptor.java`：在消息发送前进行定制化处理

```java
package com.action.kafka07producerinterceptors.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 消息修改拦截器 - 演示在消息发送前进行定制化处理
 * 
 * 功能说明：
 * 1) 修改消息内容：为所有字符串消息添加统一前缀标识
 * 2) 添加审计 Header：为消息添加审计标识，便于后续追踪和审计
 * 3) 演示拦截器链的执行顺序和异常处理机制
 * 
 * 实现细节：
 * - 实现 ProducerInterceptor<K, V> 接口，泛型参数指定键值类型
 * - onSend() 方法在消息序列化前被调用，可以修改消息内容
 * - 必须返回新的 ProducerRecord 对象，不能修改原对象
 * - 异常会中断拦截器链，后续拦截器不会执行
 * 
 * 关键参数说明：
 * - ProducerRecord: Kafka 消息记录，包含 topic、partition、key、value、headers 等
 * - RecordHeader: 消息头，用于存储元数据信息
 * - StandardCharsets.UTF_8: 确保字符编码一致性
 */
public class ModifyRecordInterceptor implements ProducerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ModifyRecordInterceptor.class);

    /**
     * 消息发送前的拦截处理
     * 
     * 执行时机：消息被发送到序列化器之前
     * 作用：可以修改消息内容、添加/修改 headers、进行数据转换等
     * 
     * @param record 原始消息记录
     * @return 修改后的消息记录，必须返回新对象
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        try {
            // 1. 获取原始消息值并进行修改
            String newValue = record.value();
            if (newValue != null) {
                // 为消息添加统一前缀，便于识别经过拦截器处理的消息
                newValue = "[modified-by-interceptor] " + newValue;
            }

            // 2. 创建新的 ProducerRecord 对象
            // 注意：必须创建新对象，不能直接修改原 record
            ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                    record.topic(),        // 主题名称
                    record.partition(),    // 分区号（null 表示由分区器决定）
                    record.timestamp(),    // 时间戳（null 表示使用当前时间）
                    record.key(),          // 消息键
                    newValue,              // 修改后的消息值
                    record.headers()       // 原始 headers（会被复制）
            );

            // 3. 添加自定义审计 Header
            // 用于标识消息已被拦截器处理，便于后续审计和追踪
            newRecord.headers().add(new RecordHeader("x-audit", "intercepted".getBytes(StandardCharsets.UTF_8)));
            
            log.debug("Message modified by interceptor: topic={}, key={}, originalValue={}, newValue={}", 
                     record.topic(), record.key(), record.value(), newValue);
            
            return newRecord;
        } catch (Exception ex) {
            // 4. 异常处理：记录错误并重新抛出
            // 拦截器异常会中断整个发送流程，后续拦截器不会执行
            log.error("ModifyRecordInterceptor onSend error for topic={}, key={}", 
                     record.topic(), record.key(), ex);
            throw ex;
        }
    }

    /**
     * 消息发送确认回调
     * 
     * 执行时机：消息发送成功或失败后
     * 作用：可以用于统计、监控、日志记录等
     * 
     * @param metadata 发送成功时的元数据信息（包含 topic、partition、offset 等）
     * @param exception 发送失败时的异常信息（成功时为 null）
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 本拦截器专注于消息修改，统计功能由 MetricsProducerInterceptor 负责
        // 这里可以添加其他处理逻辑，如审计日志记录等
        if (exception != null) {
            log.warn("Message send failed in ModifyRecordInterceptor: {}", exception.getMessage());
        }
    }

    /**
     * 拦截器关闭时的清理工作
     * 
     * 执行时机：Producer 关闭时
     * 作用：释放资源、输出统计信息等
     */
    @Override
    public void close() {
        // 本拦截器无需特殊清理工作
        log.info("ModifyRecordInterceptor closed");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化资源等
     * 
     * @param configs 生产者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器无需特殊配置
        log.info("ModifyRecordInterceptor configured with {} parameters", configs.size());
    }
}
```

### 4. 指标统计拦截器

`interceptor/MetricsProducerInterceptor.java`：收集生产者发送成功与失败的统计信息

```java
package com.action.kafka07producerinterceptors.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标统计拦截器 - 收集生产者发送成功与失败的统计信息
 * 
 * 功能说明：
 * 1) 全局统计：记录总的发送成功和失败次数
 * 2) 分主题统计：按 topic 分别统计成功发送次数
 * 3) 监控告警：在发送失败时记录警告日志
 * 4) 生命周期管理：在拦截器关闭时输出统计报告
 * 
 * 实现细节：
 * - 使用 AtomicLong 确保多线程环境下的计数准确性
 * - 使用 ConcurrentHashMap 存储分主题统计，支持并发访问
 * - 在 onAcknowledgement() 中根据异常情况更新计数器
 * - 在 close() 方法中输出最终统计结果
 * 
 * 关键参数说明：
 * - AtomicLong: 线程安全的原子长整型，用于计数
 * - ConcurrentHashMap: 线程安全的哈希表，存储分主题统计
 * - RecordMetadata: 发送成功时包含 topic、partition、offset 等信息
 */
public class MetricsProducerInterceptor implements ProducerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MetricsProducerInterceptor.class);

    // 全局成功发送计数器（线程安全）
    private final AtomicLong successCount = new AtomicLong(0);
    
    // 全局失败发送计数器（线程安全）
    private final AtomicLong failureCount = new AtomicLong(0);
    
    // 分主题成功发送计数器（线程安全）
    // Key: topic名称, Value: 该topic的成功发送次数
    private final Map<String, AtomicLong> topicSuccess = new ConcurrentHashMap<>();

    /**
     * 消息发送前的拦截处理
     * 
     * 执行时机：消息被发送到序列化器之前
     * 作用：本拦截器专注于统计，不修改消息内容
     * 
     * @param record 消息记录
     * @return 原消息记录（不做修改）
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 本拦截器不修改消息内容，直接返回原记录
        // 可以在这里添加发送前的统计逻辑，如记录发送尝试次数等
        return record;
    }

    /**
     * 消息发送确认回调 - 核心统计逻辑
     * 
     * 执行时机：消息发送成功或失败后
     * 作用：根据发送结果更新各种计数器
     * 
     * @param metadata 发送成功时的元数据信息（包含 topic、partition、offset 等）
     * @param exception 发送失败时的异常信息（成功时为 null）
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null && metadata != null) {
            // 发送成功：更新成功计数器
            successCount.incrementAndGet();
            
            // 更新对应主题的成功计数
            // computeIfAbsent: 如果key不存在则创建新的AtomicLong(0)，然后递增
            topicSuccess.computeIfAbsent(metadata.topic(), k -> new AtomicLong(0)).incrementAndGet();
            
            log.debug("Message sent successfully: topic={}, partition={}, offset={}", 
                     metadata.topic(), metadata.partition(), metadata.offset());
        } else {
            // 发送失败：更新失败计数器
            failureCount.incrementAndGet();
            
            if (exception != null) {
                // 记录失败原因，便于问题排查
                log.warn("Message send failed: topic={}, error={}", 
                        metadata != null ? metadata.topic() : "unknown", 
                        exception.getMessage());
            }
        }
    }

    /**
     * 拦截器关闭时的统计报告输出
     * 
     * 执行时机：Producer 关闭时
     * 作用：输出最终的统计结果，用于监控和运维分析
     */
    @Override
    public void close() {
        // 输出全局统计信息
        long totalSuccess = successCount.get();
        long totalFailure = failureCount.get();
        long totalMessages = totalSuccess + totalFailure;
        
        log.info("=== Producer Metrics Report ===");
        log.info("Total messages: {}, Success: {}, Failure: {}", totalMessages, totalSuccess, totalFailure);
        
        if (totalMessages > 0) {
            double successRate = (double) totalSuccess / totalMessages * 100;
            log.info("Success rate: {:.2f}%", successRate);
        }
        
        // 输出分主题统计信息
        if (!topicSuccess.isEmpty()) {
            log.info("=== Per-Topic Success Count ===");
            topicSuccess.forEach((topic, count) -> 
                log.info("Topic: {}, Success count: {}", topic, count.get())
            );
        }
        
        log.info("=== End of Metrics Report ===");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化统计数据结构等
     * 
     * @param configs 生产者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如统计间隔、输出格式等
        log.info("MetricsProducerInterceptor configured with {} parameters", configs.size());
    }
}
```

### 5. Kafka配置类

`config/KafkaConfig.java`：配置KafkaTemplate和拦截器

```java
package com.action.kafka07producerinterceptors.config;

import com.action.kafka07producerinterceptors.interceptor.MetricsProducerInterceptor;
import com.action.kafka07producerinterceptors.interceptor.ModifyRecordInterceptor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 生产者配置类
 * 
 * 功能说明：
 * 1) 配置生产者基本参数：序列化器、服务器地址等
 * 2) 注册自定义拦截器：按执行顺序配置拦截器链
 * 3) 创建生产者工厂和 KafkaTemplate Bean
 * 4) 自动创建演示用的 Topic
 * 
 * 关键配置说明：
 * - BOOTSTRAP_SERVERS_CONFIG: Kafka 集群地址
 * - KEY_SERIALIZER_CLASS_CONFIG: 键序列化器
 * - VALUE_SERIALIZER_CLASS_CONFIG: 值序列化器
 * - INTERCEPTOR_CLASSES_CONFIG: 拦截器类列表（按顺序执行）
 */
@Configuration
public class KafkaConfig {

    /**
     * Kafka 服务器地址配置
     * 从 application.properties 读取，默认 localhost:9097
     */
    @Value("${spring.kafka.bootstrap-servers:localhost:9097}")
    private String bootstrapServers;

    /**
     * 演示用 Topic 名称配置
     * 从 application.properties 读取，默认 demo-interceptor-topic
     */
    @Value("${demo.topic.name:demo-interceptor-topic}")
    private String demoTopic;

    /**
     * 生产者配置参数
     * 
     * 配置说明：
     * - 基本连接配置：服务器地址、序列化器
     * - 拦截器配置：按顺序注册自定义拦截器
     * - 其他配置：可根据需要添加重试、超时等参数
     * 
     * @return 生产者配置 Map
     */
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        
        // 1. 基本连接配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // 2. 序列化器配置
        // 键和值都使用字符串序列化器，适合演示场景
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // 3. 拦截器配置 - 核心配置
        // 拦截器按列表顺序依次执行 onSend() 方法
        // 如果某个拦截器抛出异常，后续拦截器不会执行
        List<Class<?>> interceptors = new ArrayList<>();
        interceptors.add(ModifyRecordInterceptor.class);    // 第一个：修改消息内容
        interceptors.add(MetricsProducerInterceptor.class); // 第二个：统计发送指标
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        
        // 4. 可选配置（可根据需要添加）
        // props.put(ProducerConfig.RETRIES_CONFIG, 3);                    // 重试次数
        // props.put(ProducerConfig.ACKS_CONFIG, "all");                   // 确认机制
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);     // 请求超时
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);             // 批处理大小
        // props.put(ProducerConfig.LINGER_MS_CONFIG, 5);                  // 等待时间
        
        return props;
    }

    /**
     * 生产者工厂 Bean
     * 
     * 作用：创建 Kafka 生产者实例
     * 实现：使用 DefaultKafkaProducerFactory 和上述配置参数
     * 
     * @return 生产者工厂实例
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * KafkaTemplate Bean
     * 
     * 作用：Spring 提供的 Kafka 操作模板，简化消息发送
     * 特点：线程安全，支持异步发送，集成 Spring 事务管理
     * 
     * @return KafkaTemplate 实例
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * 演示用 Topic Bean
     * 
     * 作用：自动创建用于演示的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(demoTopic)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
```

### 6. 生产者演示服务

`service/ProducerDemoService.java`：封装KafkaTemplate消息发送逻辑

```java
package com.action.kafka07producerinterceptors.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 生产者演示服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供异步消息发送接口
 * 3) 演示拦截器在消息发送过程中的作用
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 返回 CompletableFuture 支持异步处理和回调
 * 
 * 关键参数说明：
 * - KafkaTemplate: Spring 提供的 Kafka 操作模板，线程安全
 * - CompletableFuture: 异步编程模型，支持链式操作和异常处理
 */
@Service
public class ProducerDemoService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 目标 Topic 名称
     * 从配置文件读取，默认 demo-interceptor-topic
     */
    @Value("${demo.topic.name:demo-interceptor-topic}")
    private String topicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public ProducerDemoService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送消息到 Kafka
     * 
     * 执行流程：
     * 1) 调用 KafkaTemplate.send() 发送消息
     * 2) 消息会经过配置的拦截器链处理
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * 拦截器执行顺序：
     * - ModifyRecordInterceptor: 修改消息内容，添加前缀和 Header
     * - MetricsProducerInterceptor: 统计发送成功/失败次数
     * 
     * @param key 消息键（可选，用于分区路由）
     * @param value 消息值（必填，实际的消息内容）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String key, String value) {
        // 使用 KafkaTemplate 发送消息
        // send() 方法返回 SendResult，通过 completable() 转换为 CompletableFuture
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(topicName, key, value)
                .completable()
                .thenApply(recordMetadata -> null);
    }
}
```

### 7. REST控制器

`controller/ProducerDemoController.java`：提供HTTP接口用于测试消息发送

```java
package com.action.kafka07producerinterceptors.controller;

import com.action.kafka07producerinterceptors.service.ProducerDemoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 生产者演示控制器
 * 
 * 功能说明：
 * 1) 提供 HTTP 接口用于测试消息发送
 * 2) 演示拦截器在消息发送过程中的作用
 * 3) 支持通过 URL 参数指定消息键和值
 * 
 * 接口说明：
 * - GET /demo/send?key=xxx&value=yyy: 发送消息到 Kafka
 * - key 参数可选，value 参数必填
 * - 返回简单的成功响应
 * 
 * 使用示例：
 * curl "http://localhost:8080/demo/send?key=k1&value=hello world"
 * curl "http://localhost:8080/demo/send?value=test message"
 */
@RestController
public class ProducerDemoController {

    /**
     * 生产者演示服务
     * 负责实际的消息发送逻辑
     */
    private final ProducerDemoService producerDemoService;

    /**
     * 构造函数注入服务依赖
     * 
     * @param producerDemoService 生产者演示服务
     */
    public ProducerDemoController(ProducerDemoService producerDemoService) {
        this.producerDemoService = producerDemoService;
    }

    /**
     * 发送消息接口
     * 
     * 请求方式：GET
     * 请求路径：/demo/send
     * 
     * 参数说明：
     * - key: 消息键（可选），用于分区路由和消息去重
     * - value: 消息值（必填），实际的消息内容
     * 
     * 响应说明：
     * - 成功：返回 "send ok" 和 HTTP 200
     * - 失败：返回错误信息和相应的 HTTP 状态码
     * 
     * 拦截器处理流程：
     * 1) ModifyRecordInterceptor: 为消息添加前缀 "[modified-by-interceptor] " 和审计 Header
     * 2) MetricsProducerInterceptor: 统计发送成功/失败次数
     * 
     * @param key 消息键（可选）
     * @param value 消息值（必填）
     * @return HTTP 响应实体
     */
    @GetMapping("/demo/send")
    public ResponseEntity<String> send(@RequestParam(value = "key", required = false) String key,
                                       @RequestParam("value") String value) {
        // 调用服务层发送消息
        // 注意：这里使用异步发送，不等待发送结果
        // 实际生产环境中可能需要处理发送异常
        producerDemoService.sendMessage(key, value);
        
        // 返回成功响应
        // 注意：这里只表示消息已提交发送，不代表发送成功
        // 真正的发送结果会在拦截器的 onAcknowledgement() 中处理
        return ResponseEntity.ok("send ok");
    }
}
```

## Kafka Producer拦截器详解

### 1. 拦截器接口说明

Kafka Producer拦截器需要实现 `ProducerInterceptor<K, V>` 接口，包含以下方法：

- **onSend()**: 消息发送前调用，可以修改消息内容
- **onAcknowledgement()**: 消息发送成功或失败后调用，用于统计和监控
- **close()**: 拦截器关闭时调用，用于清理资源
- **configure()**: 拦截器初始化时调用，用于读取配置

### 2. 拦截器执行流程

```
消息发送请求
    ↓
ModifyRecordInterceptor.onSend()  ← 修改消息内容，添加Header
    ↓
MetricsProducerInterceptor.onSend()  ← 不修改消息，直接返回
    ↓
消息序列化
    ↓
发送到Kafka
    ↓
MetricsProducerInterceptor.onAcknowledgement()  ← 统计成功/失败
    ↓
ModifyRecordInterceptor.onAcknowledgement()  ← 记录审计日志
```

### 3. 拦截器配置方式

#### 方式一：Java配置（推荐）

```java
@Bean
public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    // ... 其他配置
    
    List<Class<?>> interceptors = new ArrayList<>();
    interceptors.add(ModifyRecordInterceptor.class);
    interceptors.add(MetricsProducerInterceptor.class);
    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
    
    return props;
}
```

#### 方式二：属性配置

```properties
spring.kafka.producer.properties.interceptor.classes=com.action.kafka07producerinterceptors.interceptor.ModifyRecordInterceptor,com.action.kafka07producerinterceptors.interceptor.MetricsProducerInterceptor
```

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-07-ProducerInterceptors

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送消息测试

```bash
# 发送带键的消息
curl "http://localhost:8080/demo/send?key=k1&value=hello world"

# 发送无键的消息
curl "http://localhost:8080/demo/send?value=test message"
```

**响应格式**：

```
send ok
```

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 消息修改日志

```
INFO  - Message modified by interceptor: topic=demo-interceptor-topic, key=k1, originalValue=hello world, newValue=[modified-by-interceptor] hello world
```

#### 发送成功日志

```
INFO  - Message sent successfully: topic=demo-interceptor-topic, partition=0, offset=123
```

#### 统计报告日志（应用关闭时）

```
INFO  - === Producer Metrics Report ===
INFO  - Total messages: 5, Success: 5, Failure: 0
INFO  - Success rate: 100.00%
INFO  - === Per-Topic Success Count ===
INFO  - Topic: demo-interceptor-topic, Success count: 5
INFO  - === End of Metrics Report ===
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9097`运行
2. **拦截器顺序**: 拦截器按配置顺序执行，异常会中断后续拦截器
3. **线程安全**: 拦截器实例会被多个线程共享，需要确保线程安全
4. **性能影响**: 拦截器会增加消息发送的延迟，需要权衡功能与性能
5. **异常处理**: 拦截器异常会导致消息发送失败，需要谨慎处理
6. **资源清理**: 在close()方法中正确清理资源，避免内存泄漏
7. **配置管理**: 建议使用Java配置方式，便于管理和维护
8. **监控告警**: 可以利用拦截器实现消息发送的监控和告警功能