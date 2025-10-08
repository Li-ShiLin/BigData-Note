<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kafka Hello World](#kafka-hello-world)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [2. 简单 Kafka 生产者](#2-%E7%AE%80%E5%8D%95-kafka-%E7%94%9F%E4%BA%A7%E8%80%85)
      - [生产者配置说明](#%E7%94%9F%E4%BA%A7%E8%80%85%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
    - [3. 简单 Kafka 消费者](#3-%E7%AE%80%E5%8D%95-kafka-%E6%B6%88%E8%B4%B9%E8%80%85)
      - [消费者配置说明](#%E6%B6%88%E8%B4%B9%E8%80%85%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
    - [4. Spring Boot 主启动类](#4-spring-boot-%E4%B8%BB%E5%90%AF%E5%8A%A8%E7%B1%BB)
  - [Kafka 核心概念](#kafka-%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5)
    - [1. 主题 (Topic)](#1-%E4%B8%BB%E9%A2%98-topic)
    - [2. 生产者 (Producer)](#2-%E7%94%9F%E4%BA%A7%E8%80%85-producer)
    - [3. 消费者 (Consumer)](#3-%E6%B6%88%E8%B4%B9%E8%80%85-consumer)
    - [4. 偏移量 (Offset)](#4-%E5%81%8F%E7%A7%BB%E9%87%8F-offset)
  - [运行方法](#%E8%BF%90%E8%A1%8C%E6%96%B9%E6%B3%95)
    - [1. 启动 Kafka 服务器](#1-%E5%90%AF%E5%8A%A8-kafka-%E6%9C%8D%E5%8A%A1%E5%99%A8)
    - [2. 创建主题](#2-%E5%88%9B%E5%BB%BA%E4%B8%BB%E9%A2%98)
    - [3. 运行生产者](#3-%E8%BF%90%E8%A1%8C%E7%94%9F%E4%BA%A7%E8%80%85)
    - [4. 运行消费者](#4-%E8%BF%90%E8%A1%8C%E6%B6%88%E8%B4%B9%E8%80%85)
    - [5. 使用 Spring Boot 运行](#5-%E4%BD%BF%E7%94%A8-spring-boot-%E8%BF%90%E8%A1%8C)
  - [测试步骤](#%E6%B5%8B%E8%AF%95%E6%AD%A5%E9%AA%A4)
    - [1. 基本功能测试](#1-%E5%9F%BA%E6%9C%AC%E5%8A%9F%E8%83%BD%E6%B5%8B%E8%AF%95)
    - [2. 预期输出](#2-%E9%A2%84%E6%9C%9F%E8%BE%93%E5%87%BA)
      - [生产者输出](#%E7%94%9F%E4%BA%A7%E8%80%85%E8%BE%93%E5%87%BA)
      - [消费者输出](#%E6%B6%88%E8%B4%B9%E8%80%85%E8%BE%93%E5%87%BA)
    - [3. 多消费者测试](#3-%E5%A4%9A%E6%B6%88%E8%B4%B9%E8%80%85%E6%B5%8B%E8%AF%95)
  - [常见问题](#%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
    - [1. 连接失败](#1-%E8%BF%9E%E6%8E%A5%E5%A4%B1%E8%B4%A5)
    - [2. 主题不存在](#2-%E4%B8%BB%E9%A2%98%E4%B8%8D%E5%AD%98%E5%9C%A8)
    - [3. 序列化错误](#3-%E5%BA%8F%E5%88%97%E5%8C%96%E9%94%99%E8%AF%AF)
  - [扩展功能](#%E6%89%A9%E5%B1%95%E5%8A%9F%E8%83%BD)
    - [1. 批量发送消息](#1-%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E6%B6%88%E6%81%AF)
    - [2. 异步发送回调](#2-%E5%BC%82%E6%AD%A5%E5%8F%91%E9%80%81%E5%9B%9E%E8%B0%83)
    - [3. 消费者组管理](#3-%E6%B6%88%E8%B4%B9%E8%80%85%E7%BB%84%E7%AE%A1%E7%90%86)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kafka Hello World

## 项目作用

本项目是 Kafka 系列的第一个演示项目，展示了 Kafka 最基础的生产者和消费者功能。通过简单的代码示例，帮助开发者快速理解 Kafka
的基本概念和核心 API 的使用方法。

## 项目结构

```
kafka-01-hello-world/
├── src/main/java/com/action/kafka01helloworld/
│   ├── Kafka01HelloWorldApplication.java    # Spring Boot 主启动类
│   └── simpleKafka/
│       ├── SimpleKafkaProducer.java         # 简单 Kafka 生产者
│       └── SimpleKafkaConsumer.java         # 简单 Kafka 消费者
└── pom.xml                                  # Maven 配置
```

## 核心实现

### 1. 依赖配置

`pom.xml`：引入 Kafka 相关依赖

```xml

<dependencies>
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
```

### 2. 简单 Kafka 生产者

`simpleKafka/SimpleKafkaProducer.java`：使用原生 Kafka 客户端 API 发送消息

```java
package com.action.kafka01helloworld.simpleKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置Kafka服务器地址
        props.put("bootstrap.servers", "192.168.56.10:9092");
        // 设置数据key的序列化处理类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置数据value的序列化处理类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 发送一条消息到名为 "test-topic" 的主题
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key", "Hello, Kafka!");
        producer.send(record);
        // 关闭生产者
        producer.close();
    }
}
```

#### 生产者配置说明

- **bootstrap.servers**: Kafka 集群的服务器地址列表
- **key.serializer**: 消息键的序列化器，将 Java 对象转换为字节数组
- **value.serializer**: 消息值的序列化器，将 Java 对象转换为字节数组
- **ProducerRecord**: 包含主题、键、值的消息记录

### 3. 简单 Kafka 消费者

`simpleKafka/SimpleKafkaConsumer.java`：使用原生 Kafka 客户端 API 消费消息

```java
package com.action.kafka01helloworld.simpleKafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置Kafka服务器地址
        props.put("bootstrap.servers", "192.168.56.10:9092");
        // 设置消费分组名
        props.put("group.id", "test-group");
        // 设置数据key的反序列化处理类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置数据value的反序列化处理类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置自动提交offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        // 创建消费者实例
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 循环拉取消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                records.forEach(record -> {
                    System.out.printf("收到消息: offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                });
            }
        } finally {
            consumer.close();
        }
    }
}
```

#### 消费者配置说明

- **bootstrap.servers**: Kafka 集群的服务器地址列表
- **group.id**: 消费者组 ID，用于标识消费者组
- **key.deserializer**: 消息键的反序列化器，将字节数组转换为 Java 对象
- **value.deserializer**: 消息值的反序列化器，将字节数组转换为 Java 对象
- **enable.auto.commit**: 是否自动提交偏移量
- **auto.commit.interval.ms**: 自动提交偏移量的间隔时间

### 4. Spring Boot 主启动类

`Kafka01HelloWorldApplication.java`：Spring Boot 应用入口

```java
package com.action.kafka01helloworld;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Kafka01HelloWorldApplication {

    public static void main(String[] args) {
        SpringApplication.run(Kafka01HelloWorldApplication.class, args);
    }
}
```

## Kafka 核心概念

### 1. 主题 (Topic)

- **定义**: 消息的分类，类似于数据库中的表
- **特点**: 主题可以被多个消费者组订阅，每个消费者组独立消费
- **分区**: 主题可以分成多个分区，提高并行处理能力

### 2. 生产者 (Producer)

- **作用**: 向 Kafka 主题发送消息
- **特点**: 支持异步发送，高性能
- **配置**: 需要配置序列化器和服务器地址

### 3. 消费者 (Consumer)

- **作用**: 从 Kafka 主题消费消息
- **特点**: 支持消费者组，实现负载均衡
- **配置**: 需要配置反序列化器和消费者组 ID

### 4. 偏移量 (Offset)

- **定义**: 消息在分区中的位置标识
- **作用**: 用于记录消费者的消费进度
- **管理**: 可以自动提交或手动提交

## 运行方法

### 1. 启动 Kafka 服务器

确保 Kafka 服务器在 `192.168.56.10:9092` 运行。

### 2. 创建主题

```bash
# 创建主题
kafka-topics.sh --create --topic test-topic --bootstrap-server 192.168.56.10:9092 --partitions 3 --replication-factor 1

# 查看主题列表
kafka-topics.sh --list --bootstrap-server 192.168.56.10:9092
```

### 3. 运行生产者

```bash
# 进入项目目录
cd kafka-01-hello-world

# 编译项目
mvn compile

# 运行生产者
mvn exec:java -Dexec.mainClass="com.action.kafka01helloworld.simpleKafka.SimpleKafkaProducer"
```

### 4. 运行消费者

```bash
# 运行消费者
mvn exec:java -Dexec.mainClass="com.action.kafka01helloworld.simpleKafka.SimpleKafkaConsumer"
```

### 5. 使用 Spring Boot 运行

```bash
# 启动 Spring Boot 应用
mvn spring-boot:run
```

## 测试步骤

### 1. 基本功能测试

1. **启动消费者**: 先运行消费者程序，等待消息
2. **启动生产者**: 运行生产者程序，发送消息
3. **观察输出**: 消费者应该能收到生产者发送的消息

### 2. 预期输出

#### 生产者输出

```
消息发送成功
```

#### 消费者输出

```
收到消息: offset = 0, key = key, value = Hello, Kafka!
```

### 3. 多消费者测试

1. **启动多个消费者**: 同时运行多个消费者实例
2. **发送多条消息**: 运行生产者发送多条消息
3. **观察负载均衡**: 消息应该被分配到不同的消费者

## 常见问题

### 1. 连接失败

**问题**: 无法连接到 Kafka 服务器

**解决方案**:

- 检查 Kafka 服务器是否启动
- 确认服务器地址和端口是否正确
- 检查网络连接和防火墙设置

### 2. 主题不存在

**问题**: 主题不存在错误

**解决方案**:

- 手动创建主题
- 检查主题名称是否正确
- 确认 Kafka 服务器配置

### 3. 序列化错误

**问题**: 序列化或反序列化失败

**解决方案**:

- 检查序列化器配置
- 确认数据类型匹配
- 使用正确的序列化器类

## 扩展功能

### 1. 批量发送消息

```java
// 批量发送多条消息
for(int i = 0;
i< 10;i++){
ProducerRecord<String, String> record = new ProducerRecord<>(
        "test-topic",
        "key-" + i,
        "Hello, Kafka! Message " + i
);
    producer.

send(record);
}
```

### 2. 异步发送回调

```java
// 异步发送带回调
producer.send(record, (metadata, exception) ->{
        if(exception !=null){
        System.err.

println("发送失败: "+exception.getMessage());
        }else{
        System.out.

println("发送成功: "+metadata.toString());
        }
        });
```

### 3. 消费者组管理

```java
// 使用不同的消费者组
props.put("group.id","my-consumer-group");
```
