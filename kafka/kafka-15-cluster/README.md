<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kafka集群](#kafka%E9%9B%86%E7%BE%A4)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [依赖配置（pom.xml）](#%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AEpomxml)
  - [配置文件（application.properties）](#%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6applicationproperties)
  - [核心代码](#%E6%A0%B8%E5%BF%83%E4%BB%A3%E7%A0%81)
    - [1) 主题配置：3 分区 3 副本（KafkaConfig.java）](#1-%E4%B8%BB%E9%A2%98%E9%85%8D%E7%BD%AE3-%E5%88%86%E5%8C%BA-3-%E5%89%AF%E6%9C%ACkafkaconfigjava)
    - [2) 生产者：批量发送用户消息（ClusterEventProducer.java）](#2-%E7%94%9F%E4%BA%A7%E8%80%85%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81%E7%94%A8%E6%88%B7%E6%B6%88%E6%81%AFclustereventproducerjava)
    - [3) 消费者：并发消费与手动确认（ClusterEventConsumer.java）](#3-%E6%B6%88%E8%B4%B9%E8%80%85%E5%B9%B6%E5%8F%91%E6%B6%88%E8%B4%B9%E4%B8%8E%E6%89%8B%E5%8A%A8%E7%A1%AE%E8%AE%A4clustereventconsumerjava)
    - [4) 控制器：触发批量发送（ClusterDemoController.java）](#4-%E6%8E%A7%E5%88%B6%E5%99%A8%E8%A7%A6%E5%8F%91%E6%89%B9%E9%87%8F%E5%8F%91%E9%80%81clusterdemocontrollerjava)
    - [5) 模型与工具（User.java、JSONUtils.java）](#5-%E6%A8%A1%E5%9E%8B%E4%B8%8E%E5%B7%A5%E5%85%B7userjavajsonutilsjava)
  - [基于 Zookeeper 的集群搭建](#%E5%9F%BA%E4%BA%8E-zookeeper-%E7%9A%84%E9%9B%86%E7%BE%A4%E6%90%AD%E5%BB%BA)
  - [运行与验证](#%E8%BF%90%E8%A1%8C%E4%B8%8E%E9%AA%8C%E8%AF%81)
    - [启动应用](#%E5%90%AF%E5%8A%A8%E5%BA%94%E7%94%A8)
    - [发送测试数据](#%E5%8F%91%E9%80%81%E6%B5%8B%E8%AF%95%E6%95%B0%E6%8D%AE)
  - [观察与预期](#%E8%A7%82%E5%AF%9F%E4%B8%8E%E9%A2%84%E6%9C%9F)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kafka集群

## 项目作用

本模块演示如何在 Kafka 集群环境下（基于 Zookeeper）进行生产与消费，验证分区与副本对可靠性与可用性的影响，并提供可直接运行的示例代码与测试接口。README 中的代码片段与工程内源码完全一致（包括注释）。

## 项目结构

```
kafka-15-cluster/
├── src/main/java/com/action/kafka15cluster/
│   ├── config/
│   │   └── KafkaConfig.java                 # Topic 创建：3 分区 3 副本
│   ├── consumer/
│   │   └── ClusterEventConsumer.java        # 并发消费，手动确认演示
│   ├── controller/
│   │   └── ClusterDemoController.java       # 测试接口：发送批量消息
│   ├── model/
│   │   └── User.java                        # 演示用消息模型
│   ├── producer/
│   │   └── ClusterEventProducer.java        # 批量发送用户消息
│   ├── util/
│   │   └── JSONUtils.java                   # JSON 工具
│   └── Kafka15ClusterApplication.java       # 启动类
├── src/main/resources/
│   └── application.properties               # Kafka 集群连接配置
└── pom.xml
```

## 依赖配置（pom.xml）

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka-test</artifactId>
        <scope>test</scope>
    </dependency>
    <!-- Jackson 由 spring-boot-starter 带入，工具类演示使用 -->
</dependencies>
```

## 配置文件（application.properties）

```properties
spring.application.name=kafka-15-cluster

# ==============================
# Kafka 集群连接配置（ZK 集群示例）
# ==============================
# 请将以下地址替换为你集群的实际 IP:PORT
spring.kafka.bootstrap-servers=192.168.56.10:9091,192.168.56.10:9092,192.168.56.10:9093

# 生产者序列化配置
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# 消费者反序列化配置
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# 从最早的偏移量开始消费，便于演示
spring.kafka.consumer.auto-offset-reset=earliest

# 是否自动提交（演示可开启，生产建议按需配置）
spring.kafka.consumer.enable-auto-commit=true

# 用于演示的主题名称（同时在代码中使用）
kafka.topic.cluster=clusterTopic
```

## 核心代码

### 1) 主题配置：3 分区 3 副本（KafkaConfig.java）

```java
package com.action.kafka15cluster.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 *
 * 功能：
 * 1) 演示在 Kafka 集群下创建 Topic（包含分区与副本）
 * 2) 与 README 中代码片段保持一致
 */
@Configuration
public class KafkaConfig {

    /**
     * 主题名称（与 README、控制器、生产者、消费者一致）
     */
    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopicName;

    /**
     * 创建演示 Topic：3 分区，3 副本
     * 注意：副本数不得超过 broker 节点数
     */
    @Bean
    public NewTopic clusterTopic() {
        return TopicBuilder.name(clusterTopicName)
                .partitions(3)
                .replicas(3)
                .build();
    }
}
```

### 2) 生产者：批量发送用户消息（ClusterEventProducer.java）

```java
package com.action.kafka15cluster.producer;

import com.action.kafka15cluster.model.User;
import com.action.kafka15cluster.util.JSONUtils;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@Component
public class ClusterEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventProducer.class);

    // 加入 spring-kafka 依赖 + 配置后，KafkaTemplate 会自动装配
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopic;

    /**
     * 发送 N 条用户消息到集群 Topic
     */
    public void sendBatchUsers(int count) {
        for (int i = 0; i < count; i++) {
            User user = User.builder().id(1000 + i).phone("1370000" + i).birthDay(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            String key = "k" + i;
            kafkaTemplate.send(clusterTopic, key, userJSON)
                    .whenComplete((SendResult<String, String> result, Throwable ex) -> {
                        if (ex != null) {
                            log.error("发送失败: key={}, error={}", key, ex.getMessage(), ex);
                            return;
                        }
                        RecordMetadata md = result.getRecordMetadata();
                        log.info("发送成功: topic={}, partition={}, offset={}", md.topic(), md.partition(), md.offset());
                    });
        }
    }
}
```

### 3) 消费者：并发消费与手动确认（ClusterEventConsumer.java）

```java
package com.action.kafka15cluster.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class ClusterEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventConsumer.class);

    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopic;

    /**
     * 集群消费演示：并发 3，手动确认
     */
    @KafkaListener(topics = "${kafka.topic.cluster:clusterTopic}", groupId = "clusterGroup", concurrency = "3")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            long tid = Thread.currentThread().getId();
            log.info("{} -> 消费成功: topic={}, partition={}, offset={}, key={}, value={}",
                    tid, record.topic(), record.partition(), record.offset(), record.key(), record.value());
            // 手动确认（若开启 auto-commit=true，本行等价演示；生产建议按需配置）
            ack.acknowledge();
        } catch (Exception e) {
            log.error("消费异常: {}", e.getMessage(), e);
        }
    }
}
```

### 4) 控制器：触发批量发送（ClusterDemoController.java）

```java
package com.action.kafka15cluster.controller;

import com.action.kafka15cluster.producer.ClusterEventProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/cluster")
public class ClusterDemoController {

    @Resource
    private ClusterEventProducer producer;

    /**
     * 触发批量发送，用于观察集群分区与副本效果
     */
    @GetMapping("/sendBatch")
    public Map<String, Object> sendBatch(@RequestParam(defaultValue = "10") int count) {
        producer.sendBatchUsers(count);
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "success");
        resp.put("message", "批量消息已发送");
        resp.put("count", count);
        return resp;
    }
}
```

### 5) 模型与工具（User.java、JSONUtils.java）

```java
package com.action.kafka15cluster.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {

    private int id;

    private String phone;

    private Date birthDay;
}
```

```java
package com.action.kafka15cluster.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONUtils {

    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();

    public static String toJSON(Object object) {
        try {
            return OBJECTMAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toBean(String json, Class<T> clazz) {
        try {
            return OBJECTMAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
```

## 基于 Zookeeper 的集群搭建

基于Zookeeper的集群搭建方式；

```
Kafka集群搭建
基于Zookeeper的集群搭建方式；
1、kafka是一个压缩包，直接解压即可使用，所以就解压三个kafka
2、配置kafka集群：server.properties
3、集群启动并测试；
```

1.解压kafaka压缩包并复制3多份

```bash
# 查看kafaka压缩包
[vagrant@master ~]$ ls

# 解压kafka压缩包
kafka_2.13-3.7.0.tgz
[vagrant@master ~]$ sudo tar -xzf kafka_2.13-3.7.0.tgz
[vagrant@master ~]$ ls
kafka_2.13-3.7.0

# 复制文件并拷贝到/usr/local目录
[vagrant@master ~]$ mv kafka_2.13-3.7.0 kafka_2.13-3.7.0-01
[vagrant@master ~]$ sudo mv kafka_2.13-3.7.0-01 /usr/local

[vagrant@master ~]$ cd /usr/local/
[vagrant@master local]$ ls
kafka_2.13-3.7.0-01

# 拷贝2份kafaka文件
[vagrant@master local]$ sudo cp -rf kafka_2.13-3.7.0-01 kafka_2.13-3.7.0-02
[vagrant@master local]$ sudo cp -rf kafka_2.13-3.7.0-01 kafka_2.13-3.7.0-03
[vagrant@master local]$ ls
kafka_2.13-3.7.0-03 kafka_2.13-3.7.0-01 kafka_2.13-3.7.0-02
```

2.kafka集群配置

```bash
# 配置kafka集群：server.properties
# 1.三台分别配置为：
#    broker.id=1、broker.id=2、broker.id=3
#    该配置项是每个broker的唯一id，取值在0~255之间；
# 2.三台分别配置listener=PAINTEXT:IP:PORT
#    listeners=PLAINTEXT://0.0.0.0:9091
#    listeners=PLAINTEXT://0.0.0.0:9092
#    listeners=PLAINTEXT://0.0.0.0:9093
# 3.三台分别配置advertised.listeners=PAINTEXT:IP:PORT
#    advertised.listeners=PLAINTEXT://192.168.11.128:9091
#    advertised.listeners=PLAINTEXT://192.168.11.128:9092
#    advertised.listeners=PLAINTEXT://192.168.11.128:9093
# 4.配置日志目录。
# 这是极为重要的配置项，kafka所有数据就是写入这个目录下的磁盘文件中的；
#    log.dirs=/tmp/kafka-logs-9091
#    log.dirs=/tmp/kafka-logs-9092
#    log.dirs=/tmp/kafka-logs-9093
# 5.配置zookeeper连接地址
# 如果zookeeper是单机，则：
# zookeeper.connect=localhost:2181
# 如果zookeeper是集群，则：
# zookeeper.connect=localhost:2181,localhost:2182,localhost:2183


# 修改节点1配置
[vagrant@master local]$ cd /usr/local/kafka_2.13-3.7.0-01/config
# 修改配置
[vagrant@master config]$ sudo vim server.properties
# 查看具体配置情况
# sudo cat /usr/local/kafka_2.13-3.7.0-01/config/server.properties | grep -E "^(broker.id|listeners|advertised.listeners|log.dirs|zookeeper.connect)="
[vagrant@master config]$ sudo cat /usr/local/kafka_2.13-3.7.0-01/config/server.properties | grep -E "^(broker.id|listeners|advertised.listeners|log.dirs|zookeeper.connect)="
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9091
advertised.listeners=PLAINTEXT://192.168.56.10:9091
log.dirs=/tmp/kafka-logs-9091
zookeeper.connect=localhost:2181


# 修改节点2配置
[vagrant@master kafka_2.13-3.7.0-02]$ cd /usr/local/kafka_2.13-3.7.0-02/config
# 修改配置
[vagrant@master config]$ sudo vim server.properties
# 查看具体配置情况
[vagrant@master config]$ sudo cat /usr/local/kafka_2.13-3.7.0-02/config/server.properties | grep -E "^(broker.id|listeners|advertised.listeners|log.dirs|zookeeper.connect)="
broker.id=2
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://192.168.56.10:9092
log.dirs=/tmp/kafka-logs-9092
zookeeper.connect=localhost:2181

# 修改节点3配置
[vagrant@master config]$ cd /usr/local/kafka_2.13-3.7.0-03/config
[vagrant@master config]$ sudo vim server.properties
# 查看具体配置情况
# sudo cat /usr/local/kafka_2.13-3.7.0-03/config/server.properties | grep -E "^(broker.id|listeners|advertised.listeners|log.dirs|zookeeper.connect)="
[vagrant@master config]$ sudo cat /usr/local/kafka_2.13-3.7.0-03/config/server.properties | grep -E "^(broker.id|listeners|advertised.listeners|log.dirs|zookeeper.connect)="
broker.id=3
listeners=PLAINTEXT://0.0.0.0:9093
advertised.listeners=PLAINTEXT://192.168.56.10:9093
log.dirs=/tmp/kafka-logs-9093
zookeeper.connect=localhost:2181
```

3.启动测试

```bash
# 启动测试
# 启动Zookeeper，切换到bin目录：./zkServer.sh start
# 启动三个Kafka，切换到bin目录：./kafka-server-start.sh ../config/server.properties
# 查看topic详情：./kafka-topics.sh --bootstrap-server 127.0.0.1:9091 --describe --topic clusterTopic

# 启动zookeeper
[vagrant@master bin]$ pwd
/usr/local/apache-zookeeper-3.9.2-bin/bin
[vagrant@master bin]$ ./zkServer.sh start
[vagrant@master bin]$ ps -ef | grep zookeeper


# 创建logs目录（为所有三个节点创建）
sudo mkdir -p /usr/local/kafka_2.13-3.7.0-01/logs
sudo mkdir -p /usr/local/kafka_2.13-3.7.0-02/logs  
sudo mkdir -p /usr/local/kafka_2.13-3.7.0-03/logs

# 将目录所有者改为vagrant用户
sudo chown -R vagrant:vagrant /usr/local/kafka_2.13-3.7.0-01/logs
sudo chown -R vagrant:vagrant /usr/local/kafka_2.13-3.7.0-02/logs
sudo chown -R vagrant:vagrant /usr/local/kafka_2.13-3.7.0-03/logs


# 查看zookeeper配置文件
[vagrant@master conf]$ pwd
/usr/local/apache-zookeeper-3.9.2-bin/conf
[vagrant@master conf]$ sudo cat zoo.cfg | grep Port
clientPort=2181
#metricsProvider.httpPort=7000
# 控制台访问
admin.serverPort=9089




# 启动三个Kafka
# 启动三个Kafka，切换到bin目录：./kafka-server-start.sh ../config/server.properties
# 启动节点1
cd /usr/local/kafka_2.13-3.7.0-01/bin
./kafka-server-start.sh ../config/server.properties &

# 启动节点2
cd /usr/local/kafka_2.13-3.7.0-02/bin
./kafka-server-start.sh ../config/server.properties &

# 启动节点3
cd /usr/local/kafka_2.13-3.7.0-03/bin
./kafka-server-start.sh ../config/server.properties &
```

## 运行与验证

### 启动应用

```bash
cd kafka-15-cluster
mvn spring-boot:run
```

确保 Kafka 集群可达（`spring.kafka.bootstrap-servers` 指向 9091/9092/9093）。

### 发送测试数据

```bash
curl "http://localhost:9105/api/cluster/sendBatch?count=12"
```

预期效果：
- 日志打印生产成功信息：包含 topic、partition、offset。
- 消费端并发消费日志：线程 ID 不同；同 key 的消息落在同一分区。
- 关闭任一 broker（仍保留 ISR）消息仍可正常生产与消费（副本容错演示）。

## 观察与预期

- 分区分布：可通过 `--describe` 查看 `clusterTopic` 的分区与副本分布。
- 故障容错：停止一个 broker 后，leader 发生迁移，客户端仍可读写（在 ISR 满足时）。
- 顺序性：同 key 的发送保持单分区内顺序，跨分区不保证全局顺序。

## 注意事项

1. 创建 Topic 的副本数不得大于 broker 数量，否则创建失败。
2. 若使用手动确认并关闭自动提交，请配置 `ack.acknowledge()` 与监听容器 `ack-mode`。
3. 生产环境建议设置 `acks=all`、`retries`、`linger.ms`、`batch.size`、`compression.type` 等以优化吞吐与可靠性。

