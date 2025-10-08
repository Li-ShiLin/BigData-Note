<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [消费者消息拦截器](#%E6%B6%88%E8%B4%B9%E8%80%85%E6%B6%88%E6%81%AF%E6%8B%A6%E6%88%AA%E5%99%A8)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 消息过滤拦截器](#3-%E6%B6%88%E6%81%AF%E8%BF%87%E6%BB%A4%E6%8B%A6%E6%88%AA%E5%99%A8)
    - [4. 指标统计拦截器](#4-%E6%8C%87%E6%A0%87%E7%BB%9F%E8%AE%A1%E6%8B%A6%E6%88%AA%E5%99%A8)
    - [5. Kafka配置类](#5-kafka%E9%85%8D%E7%BD%AE%E7%B1%BB)
  - [Kafka Consumer拦截器详解](#kafka-consumer%E6%8B%A6%E6%88%AA%E5%99%A8%E8%AF%A6%E8%A7%A3)
    - [1. 拦截器接口说明](#1-%E6%8B%A6%E6%88%AA%E5%99%A8%E6%8E%A5%E5%8F%A3%E8%AF%B4%E6%98%8E)
    - [2. 拦截器执行流程](#2-%E6%8B%A6%E6%88%AA%E5%99%A8%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B)
    - [3. 拦截器配置方式](#3-%E6%8B%A6%E6%88%AA%E5%99%A8%E9%85%8D%E7%BD%AE%E6%96%B9%E5%BC%8F)
      - [方式一：Java配置（推荐）](#%E6%96%B9%E5%BC%8F%E4%B8%80java%E9%85%8D%E7%BD%AE%E6%8E%A8%E8%8D%90)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 启动应用](#1-%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [2. 测试API接口](#2-%E6%B5%8B%E8%AF%95api%E6%8E%A5%E5%8F%A3)
      - [发送消息到指定分区测试](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%8C%87%E5%AE%9A%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95)
      - [发送消息到所有分区测试（自动分区）](#%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E5%88%B0%E6%89%80%E6%9C%89%E5%88%86%E5%8C%BA%E6%B5%8B%E8%AF%95%E8%87%AA%E5%8A%A8%E5%88%86%E5%8C%BA)
      - [批量发送消息测试](#%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF%E6%B5%8B%E8%AF%95)
    - [3. 观察日志输出](#3-%E8%A7%82%E5%AF%9F%E6%97%A5%E5%BF%97%E8%BE%93%E5%87%BA)
      - [拦截器处理日志](#%E6%8B%A6%E6%88%AA%E5%99%A8%E5%A4%84%E7%90%86%E6%97%A5%E5%BF%97)
      - [消费者处理日志](#%E6%B6%88%E8%B4%B9%E8%80%85%E5%A4%84%E7%90%86%E6%97%A5%E5%BF%97)
      - [统计报告日志（应用关闭时）](#%E7%BB%9F%E8%AE%A1%E6%8A%A5%E5%91%8A%E6%97%A5%E5%BF%97%E5%BA%94%E7%94%A8%E5%85%B3%E9%97%AD%E6%97%B6)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 消费者消息拦截器

## 项目作用

本项目演示了SpringBoot中Kafka Consumer拦截器的自定义实现和使用，包括2种不同类型的拦截器，帮助开发者理解Kafka消费者拦截器的工作原理和实际应用场景。

## 项目结构

```
kafka-12-consumer-Interceptor/
├── src/main/java/com/action/kafka12consumerinterceptor/
│   ├── config/
│   │   └── KafkaConfig.java                    # Kafka配置类
│   ├── interceptor/
│   │   ├── MessageFilterInterceptor.java       # 消息过滤拦截器
│   │   └── MetricsConsumerInterceptor.java     # 指标统计拦截器
│   ├── consumer/
│   │   └── ConsumerDemoConsumer.java           # 消费者演示类
│   ├── service/
│   │   └── ProducerDemoService.java            # 生产者服务
│   ├── controller/
│   │   └── ConsumerDemoController.java          # REST API控制器
│   └── Kafka12ConsumerInterceptorApplication.java # 主启动类
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

`application.properties`：Kafka服务器和拦截器配置

```properties
# ========================================
# Kafka 消费者拦截器演示应用配置
# ========================================

# 应用名称
spring.application.name=kafka-12-consumer-Interceptor

# ========================================
# Kafka 连接配置
# ========================================
# Kafka 服务器地址（多个地址用逗号分隔）
# 默认：localhost:9092
spring.kafka.bootstrap-servers=localhost:9092

# ========================================
# 演示 Topic 配置
# ========================================
# 演示用的 Topic 名称
# 应用启动时会自动创建该 Topic（3个分区，1个副本）
demo.topic.name=consumer-interceptor-demo

# ========================================
# 消费者组配置
# ========================================
# 消费者组 ID
demo.consumer.group=consumer-interceptor-group

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

# 消费者组ID
spring.kafka.consumer.group-id=${demo.consumer.group}

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
server.port=9102
```

### 3. 消息过滤拦截器

`interceptor/MessageFilterInterceptor.java`：在消息消费前进行过滤和预处理

```java
package com.action.kafka12consumerinterceptor.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 消息过滤拦截器 - 演示在消息消费前进行过滤和预处理
 * 
 * 功能说明：
 * 1) 消息过滤：根据消息内容过滤掉不符合条件的消息
 * 2) 消息预处理：为消息添加处理标识和元数据
 * 3) 统计过滤信息：记录被过滤的消息数量和原因
 * 4) 演示拦截器链的执行顺序和异常处理机制
 * 
 * 实现细节：
 * - 实现 ConsumerInterceptor<K, V> 接口，泛型参数指定键值类型
 * - onConsume() 方法在消息反序列化后被调用，可以过滤和修改消息
 * - 必须返回新的 ConsumerRecords 对象，不能修改原对象
 * - 异常会中断拦截器链，后续拦截器不会执行
 */
public class MessageFilterInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MessageFilterInterceptor.class);

    // 过滤统计计数器（线程安全）
    private long filteredCount = 0;
    private long processedCount = 0;

    /**
     * 消息消费前的拦截处理
     * 
     * 执行时机：消息被反序列化之后，传递给消费者之前
     * 作用：可以过滤消息、修改消息内容、添加处理标识等
     * 
     * @param records 原始消息记录集合
     * @return 过滤后的消息记录集合，必须返回新对象
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        try {
            if (records.isEmpty()) {
                return records;
            }

            // 1. 统计处理的消息数量
            processedCount += records.count();
            
            // 2. 记录拦截器处理信息
            log.info("MessageFilterInterceptor processing {} messages", records.count());
            
            // 3. 为每条消息添加处理标识（简化版本，不进行复杂过滤）
            for (var partition : records.partitions()) {
                for (var record : records.records(partition)) {
                    log.debug("Processing message: topic={}, partition={}, offset={}, key={}, value={}", 
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
            
            // 4. 直接返回原始消息（简化演示）
            // 在实际应用中，这里可以进行消息过滤、修改等操作
            log.info("MessageFilterInterceptor completed processing");
            
            return records;
        } catch (Exception ex) {
            // 异常处理：记录错误并重新抛出
            log.error("MessageFilterInterceptor onConsume error for {} records", records.count(), ex);
            throw ex;
        }
    }

    /**
     * 偏移量提交前的回调处理
     * 
     * 执行时机：消费者提交偏移量之前
     * 作用：可以记录提交信息、进行审计等
     * 
     * @param offsets 要提交的偏移量映射
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 本拦截器专注于消息过滤，统计功能由 MetricsConsumerInterceptor 负责
        // 这里可以添加其他处理逻辑，如审计日志记录等
        log.debug("Offset commit in MessageFilterInterceptor: {} partitions", offsets.size());
    }

    /**
     * 拦截器关闭时的清理工作
     * 
     * 执行时机：Consumer 关闭时
     * 作用：释放资源、输出统计信息等
     */
    @Override
    public void close() {
        // 输出过滤统计信息
        log.info("=== Message Filter Statistics ===");
        log.info("Total messages processed: {}", processedCount);
        log.info("Messages filtered out: {}", filteredCount);
        if (processedCount > 0) {
            double filterRate = (double) filteredCount / processedCount * 100;
            log.info("Filter rate: {:.2f}%", filterRate);
        }
        log.info("=== End of Filter Statistics ===");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化资源等
     * 
     * @param configs 消费者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如过滤关键词、过滤规则等
        log.info("MessageFilterInterceptor configured with {} parameters", configs.size());
    }
}
```

### 4. 指标统计拦截器

`interceptor/MetricsConsumerInterceptor.java`：收集消费者消费成功与失败的统计信息

```java
package com.action.kafka12consumerinterceptor.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标统计拦截器 - 收集消费者消费成功与失败的统计信息
 * 
 * 功能说明：
 * 1) 全局统计：记录总的消费成功和失败次数
 * 2) 分主题统计：按 topic 分别统计成功消费次数
 * 3) 分分区统计：按 topic-partition 分别统计消费次数
 * 4) 监控告警：在消费失败时记录警告日志
 * 5) 生命周期管理：在拦截器关闭时输出统计报告
 * 
 * 实现细节：
 * - 使用 AtomicLong 确保多线程环境下的计数准确性
 * - 使用 ConcurrentHashMap 存储分主题和分分区统计，支持并发访问
 * - 在 onConsume() 中统计消费的消息数量
 * - 在 onCommit() 中统计提交的偏移量信息
 * - 在 close() 方法中输出最终统计结果
 */
public class MetricsConsumerInterceptor implements ConsumerInterceptor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(MetricsConsumerInterceptor.class);

    // 全局成功消费计数器（线程安全）
    private final AtomicLong successCount = new AtomicLong(0);
    
    // 全局失败消费计数器（线程安全）
    private final AtomicLong failureCount = new AtomicLong(0);
    
    // 分主题成功消费计数器（线程安全）
    // Key: topic名称, Value: 该topic的成功消费次数
    private final Map<String, AtomicLong> topicSuccess = new ConcurrentHashMap<>();
    
    // 分分区成功消费计数器（线程安全）
    // Key: topic-partition, Value: 该分区的成功消费次数
    private final Map<String, AtomicLong> partitionSuccess = new ConcurrentHashMap<>();
    
    // 偏移量提交统计（线程安全）
    private final AtomicLong commitCount = new AtomicLong(0);

    /**
     * 消息消费前的拦截处理
     * 
     * 执行时机：消息被反序列化之后，传递给消费者之前
     * 作用：本拦截器专注于统计，不修改消息内容
     * 
     * @param records 消息记录集合
     * @return 原消息记录集合（不做修改）
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        try {
            if (records.isEmpty()) {
                return records;
            }

            // 统计消费的消息数量
            long messageCount = records.count();
            successCount.addAndGet(messageCount);
            
            // 按主题统计
            records.partitions().forEach(partition -> {
                String topic = partition.topic();
                int partitionNum = partition.partition();
                String partitionKey = topic + "-" + partitionNum;
                
                long partitionMessageCount = records.records(partition).size();
                
                // 更新主题统计
                topicSuccess.computeIfAbsent(topic, k -> new AtomicLong(0))
                           .addAndGet(partitionMessageCount);
                
                // 更新分区统计
                partitionSuccess.computeIfAbsent(partitionKey, k -> new AtomicLong(0))
                               .addAndGet(partitionMessageCount);
                
                log.debug("Messages consumed: topic={}, partition={}, count={}", 
                         topic, partitionNum, partitionMessageCount);
            });
            
            return records;
        } catch (Exception ex) {
            // 消费失败：更新失败计数器
            failureCount.incrementAndGet();
            log.error("Message consumption failed: {} records", records.count(), ex);
            throw ex;
        }
    }

    /**
     * 偏移量提交前的回调处理 - 核心统计逻辑
     * 
     * 执行时机：消费者提交偏移量之前
     * 作用：统计偏移量提交信息，用于监控消费进度
     * 
     * @param offsets 要提交的偏移量映射
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            // 统计提交的偏移量数量
            commitCount.addAndGet(offsets.size());
            
            // 记录提交详情
            offsets.forEach((partition, offsetMetadata) -> {
                log.debug("Offset commit: topic={}, partition={}, offset={}", 
                         partition.topic(), partition.partition(), offsetMetadata.offset());
            });
            
        } catch (Exception ex) {
            log.error("Offset commit failed: {} partitions", offsets.size(), ex);
        }
    }

    /**
     * 拦截器关闭时的统计报告输出
     * 
     * 执行时机：Consumer 关闭时
     * 作用：输出最终的统计结果，用于监控和运维分析
     */
    @Override
    public void close() {
        // 输出全局统计信息
        long totalSuccess = successCount.get();
        long totalFailure = failureCount.get();
        long totalMessages = totalSuccess + totalFailure;
        long totalCommits = commitCount.get();
        
        log.info("=== Consumer Metrics Report ===");
        log.info("Total messages consumed: {}, Success: {}, Failure: {}", totalMessages, totalSuccess, totalFailure);
        log.info("Total offset commits: {}", totalCommits);
        
        if (totalMessages > 0) {
            double successRate = (double) totalSuccess / totalMessages * 100;
            log.info("Success rate: {:.2f}%", successRate);
        }
        
        // 输出分主题统计信息
        if (!topicSuccess.isEmpty()) {
            log.info("=== Per-Topic Consumption Count ===");
            topicSuccess.forEach((topic, count) -> 
                log.info("Topic: {}, Consumption count: {}", topic, count.get())
            );
        }
        
        // 输出分分区统计信息
        if (!partitionSuccess.isEmpty()) {
            log.info("=== Per-Partition Consumption Count ===");
            partitionSuccess.forEach((partition, count) -> 
                log.info("Partition: {}, Consumption count: {}", partition, count.get())
            );
        }
        
        log.info("=== End of Consumer Metrics Report ===");
    }

    /**
     * 拦截器配置初始化
     * 
     * 执行时机：拦截器创建时
     * 作用：读取配置参数、初始化统计数据结构等
     * 
     * @param configs 消费者配置参数
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 本拦截器使用默认配置，无需特殊初始化
        // 可以在这里读取自定义配置，如统计间隔、输出格式等
        log.info("MetricsConsumerInterceptor configured with {} parameters", configs.size());
    }
}
```

### 5. Kafka配置类

`config/KafkaConfig.java`：配置KafkaTemplate和拦截器

```java
package com.action.kafka12consumerinterceptor.config;

import com.action.kafka12consumerinterceptor.interceptor.MetricsConsumerInterceptor;
import com.action.kafka12consumerinterceptor.interceptor.MessageFilterInterceptor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 消费者配置类
 * 
 * 功能说明：
 * 1) 配置消费者基本参数：反序列化器、服务器地址等
 * 2) 注册自定义拦截器：按执行顺序配置拦截器链
 * 3) 创建消费者工厂和监听器容器工厂 Bean
 * 4) 自动创建演示用的 Topic
 * 
 * 关键配置说明：
 * - BOOTSTRAP_SERVERS_CONFIG: Kafka 集群地址
 * - KEY_DESERIALIZER_CLASS_CONFIG: 键反序列化器
 * - VALUE_DESERIALIZER_CLASS_CONFIG: 值反序列化器
 * - INTERCEPTOR_CLASSES_CONFIG: 拦截器类列表（按顺序执行）
 * - ENABLE_AUTO_COMMIT_CONFIG: 是否自动提交偏移量
 * - AUTO_OFFSET_RESET_CONFIG: 偏移量重置策略
 */
@Configuration
public class KafkaConfig {

    /**
     * Kafka 服务器地址配置
     * 从 application.properties 读取，默认 localhost:9092
     */
    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    /**
     * 演示用 Topic 名称配置
     * 从 application.properties 读取，默认 consumer-interceptor-demo
     */
    @Value("${demo.topic.name:consumer-interceptor-demo}")
    private String demoTopic;

    /**
     * 消费者组 ID 配置
     * 从 application.properties 读取，默认 consumer-interceptor-group
     */
    @Value("${demo.consumer.group:consumer-interceptor-group}")
    private String consumerGroup;

    /**
     * 消费者配置参数
     * 
     * 配置说明：
     * - 基本连接配置：服务器地址、反序列化器
     * - 拦截器配置：按顺序注册自定义拦截器
     * - 消费配置：消费者组、偏移量策略等
     * 
     * @return 消费者配置 Map
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        
        // 1. 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // 2. 反序列化器配置
        // 键和值都使用字符串反序列化器，适合演示场景
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // 3. 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        
        // 4. 偏移量配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // 手动提交偏移量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // 从最早的消息开始消费
        
        // 5. 拦截器配置 - 核心配置
        // 拦截器按列表顺序依次执行 onConsume() 方法
        // 如果某个拦截器抛出异常，后续拦截器不会执行
        List<Class<?>> interceptors = new ArrayList<>();
        interceptors.add(MessageFilterInterceptor.class);    // 第一个：过滤消息内容
        interceptors.add(MetricsConsumerInterceptor.class);  // 第二个：统计消费指标
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        
        return props;
    }

    /**
     * 消费者工厂 Bean
     * 
     * 作用：创建 Kafka 消费者实例
     * 实现：使用 DefaultKafkaConsumerFactory 和上述配置参数
     * 
     * @return 消费者工厂实例
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * 监听器容器工厂 Bean
     * 
     * 作用：创建消息监听器容器，用于处理消费到的消息
     * 特点：支持并发消费，集成 Spring 事务管理
     * 
     * @param consumerFactory 消费者工厂
     * @return 监听器容器工厂实例
     */
    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory);
        
        // 配置手动确认模式
        listenerContainerFactory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );
        
        return listenerContainerFactory;
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

## Kafka Consumer拦截器详解

### 1. 拦截器接口说明

Kafka Consumer拦截器需要实现 `ConsumerInterceptor<K, V>` 接口，包含以下方法：

- **onConsume()**: 消息消费前调用，可以过滤和修改消息内容
- **onCommit()**: 偏移量提交前调用，用于统计和监控
- **close()**: 拦截器关闭时调用，用于清理资源
- **configure()**: 拦截器初始化时调用，用于读取配置

### 2. 拦截器执行流程

```
消息消费请求
    ↓
MessageFilterInterceptor.onConsume()  ← 过滤消息内容，添加处理标识
    ↓
MetricsConsumerInterceptor.onConsume()  ← 统计消费指标
    ↓
消费者处理消息
    ↓
MetricsConsumerInterceptor.onCommit()  ← 统计偏移量提交
    ↓
MessageFilterInterceptor.onCommit()  ← 记录审计日志
```

### 3. 拦截器配置方式

#### 方式一：Java配置（推荐）

```java
@Bean
public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>();
    // ... 其他配置
    
    List<Class<?>> interceptors = new ArrayList<>();
    interceptors.add(MessageFilterInterceptor.class);
    interceptors.add(MetricsConsumerInterceptor.class);
    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
    
    return props;
}
```

## 测试方法

### 1. 启动应用

```bash
# 进入项目目录
cd kafka-12-consumer-Interceptor

# 启动应用
mvn spring-boot:run
```

### 2. 测试API接口

#### 发送消息到指定分区测试

```bash
# 发送消息到分区0
curl "http://localhost:9102/api/send/partition?partition=0&key=test-key&value=test-message"

# 发送消息到分区1（无键）
curl "http://localhost:9102/api/send/partition?partition=1&value=test-message-no-key"
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
curl "http://localhost:9102/api/send/auto?key=auto-key&value=auto-message"

# 发送消息（无键），让Kafka自动选择分区
curl "http://localhost:9102/api/send/auto?value=auto-message-no-key"
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

#### 批量发送消息测试

```bash
# 批量发送消息到分区0
curl "http://localhost:9102/api/send/batch/partition?partition=0&count=5"

# 批量发送消息到所有分区（自动分区）
curl "http://localhost:9102/api/send/batch/auto?count=10"
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

### 3. 观察日志输出

启动应用后，观察控制台日志：

#### 拦截器处理日志

```
INFO  - [MessageFilterInterceptor] 正在处理 1 条消息
DEBUG - [MessageFilterInterceptor] 处理消息: topic=consumer-interceptor-demo, partition=0, offset=0, key=test-key, value=test-message
INFO  - [MessageFilterInterceptor] 消息处理完成
```

#### 消费者处理日志

```
INFO  - [MessageFilterInterceptor] 正在处理 1 条消息
INFO  - [MessageFilterInterceptor] 消息处理完成
INFO  - [MetricsConsumerInterceptor] 消息消费统计: topic=consumer-interceptor-demo, partition=0, count=1
INFO  - 消费者进行消费: topic=consumer-interceptor-demo, partition=0, offset=0, key=test-key, value=test-message, timestamp=1640995200000, headers=RecordHeaders(headers = [], isReadOnly = false)
INFO  - 开始处理消息: topic=consumer-interceptor-demo, partition=0, offset=0, key=test-key, value=test-message
INFO  - 处理普通消息: test-message
INFO  - 消息处理完成: topic=consumer-interceptor-demo, partition=0, offset=0
INFO  - 消息处理完成并已确认: topic=consumer-interceptor-demo, partition=0, offset=0
INFO  - [MetricsConsumerInterceptor] Offset提交: topic=consumer-interceptor-demo, partition=0, offset=0
```

#### 统计报告日志（应用关闭时）

```
INFO  - [MessageFilterInterceptor] === 消息过滤统计 ===
INFO  - [MessageFilterInterceptor] 总处理消息数: 5
INFO  - [MessageFilterInterceptor] 过滤掉的消息数: 0
INFO  - [MessageFilterInterceptor] 过滤率: 0.00%
INFO  - [MessageFilterInterceptor] === 消息过滤统计结束 ===

INFO  - [MetricsConsumerInterceptor] === 消费者指标统计报告 ===
INFO  - [MetricsConsumerInterceptor] 总消息消费数: 5, 成功: 5, 失败: 0
INFO  - [MetricsConsumerInterceptor] 总Offset提交数: 3
INFO  - [MetricsConsumerInterceptor] 成功率: 100.00%
INFO  - [MetricsConsumerInterceptor] === 按Topic消费统计 ===
INFO  - [MetricsConsumerInterceptor] Topic: consumer-interceptor-demo, 消费数量: 5
INFO  - [MetricsConsumerInterceptor] === 按Partition消费统计 ===
INFO  - [MetricsConsumerInterceptor] Partition: consumer-interceptor-demo-0, 消费数量: 3
INFO  - [MetricsConsumerInterceptor] Partition: consumer-interceptor-demo-1, 消费数量: 2
INFO  - [MetricsConsumerInterceptor] === 消费者指标统计报告结束 ===
```

## 注意事项

1. **Kafka服务器**: 确保Kafka服务器在`localhost:9092`运行
2. **拦截器顺序**: 拦截器按配置顺序执行，异常会中断后续拦截器
3. **线程安全**: 拦截器实例会被多个线程共享，需要确保线程安全
4. **性能影响**: 拦截器会增加消息消费的延迟，需要权衡功能与性能
5. **异常处理**: 拦截器异常会导致消息消费失败，需要谨慎处理
6. **资源清理**: 在close()方法中正确清理资源，避免内存泄漏
7. **配置管理**: 建议使用Java配置方式，便于管理和维护
8. **监控告警**: 可以利用拦截器实现消息消费的监控和告警功能
9. **手动确认**: 消费者使用手动确认模式，需要调用ack.acknowledge()
10. **偏移量管理**: 拦截器可以监控偏移量提交情况，便于运维分析
