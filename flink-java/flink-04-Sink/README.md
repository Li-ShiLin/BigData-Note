<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 输出算子（Sink）演示](#flink-%E8%BE%93%E5%87%BA%E7%AE%97%E5%AD%90sink%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event.java - 事件数据模型](#321-eventjava---%E4%BA%8B%E4%BB%B6%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.2 ClickSource.java - 自定义数据源](#322-clicksourcejava---%E8%87%AA%E5%AE%9A%E4%B9%89%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.3 输出到文件](#33-%E8%BE%93%E5%87%BA%E5%88%B0%E6%96%87%E4%BB%B6)
    - [3.4 输出到 Kafka](#34-%E8%BE%93%E5%87%BA%E5%88%B0-kafka)
      - [3.4.1 前置条件：Kafka 环境搭建（KRaft 模式）](#341-%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6kafka-%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BAkraft-%E6%A8%A1%E5%BC%8F)
      - [3.4.2 代码实现与测试](#342-%E4%BB%A3%E7%A0%81%E5%AE%9E%E7%8E%B0%E4%B8%8E%E6%B5%8B%E8%AF%95)
    - [3.5 输出到 Redis](#35-%E8%BE%93%E5%87%BA%E5%88%B0-redis)
      - [3.5.1 前置条件：Redis 环境搭建](#351-%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6redis-%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA)
      - [3.5.2 代码实现与测试](#352-%E4%BB%A3%E7%A0%81%E5%AE%9E%E7%8E%B0%E4%B8%8E%E6%B5%8B%E8%AF%95)
    - [3.6 输出到 Elasticsearch](#36-%E8%BE%93%E5%87%BA%E5%88%B0-elasticsearch)
      - [3.6.1 前置条件：Elasticsearch 环境搭建](#361-%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6elasticsearch-%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA)
      - [3.6.2 代码实现与测试](#362-%E4%BB%A3%E7%A0%81%E5%AE%9E%E7%8E%B0%E4%B8%8E%E6%B5%8B%E8%AF%95)
    - [3.7 输出到 MySQL（JDBC）](#37-%E8%BE%93%E5%87%BA%E5%88%B0-mysqljdbc)
    - [3.8 自定义 Sink 输出](#38-%E8%87%AA%E5%AE%9A%E4%B9%89-sink-%E8%BE%93%E5%87%BA)
      - [3.8.1 前置条件：HBase 环境搭建](#381-%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6hbase-%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA)
        - [3.8.1.1 前置依赖：安装 Hadoop 和 Zookeeper](#3811-%E5%89%8D%E7%BD%AE%E4%BE%9D%E8%B5%96%E5%AE%89%E8%A3%85-hadoop-%E5%92%8C-zookeeper)
        - [3.8.1.2 HBase 安装配置](#3812-hbase-%E5%AE%89%E8%A3%85%E9%85%8D%E7%BD%AE)
      - [3.8.2 代码实现与测试](#382-%E4%BB%A3%E7%A0%81%E5%AE%9E%E7%8E%B0%E4%B8%8E%E6%B5%8B%E8%AF%95)
  - [4. Sink 算子类型对比](#4-sink-%E7%AE%97%E5%AD%90%E7%B1%BB%E5%9E%8B%E5%AF%B9%E6%AF%94)
    - [4.1 官方 Sink 连接器](#41-%E5%AE%98%E6%96%B9-sink-%E8%BF%9E%E6%8E%A5%E5%99%A8)
    - [4.2 Sink 实现方式对比](#42-sink-%E5%AE%9E%E7%8E%B0%E6%96%B9%E5%BC%8F%E5%AF%B9%E6%AF%94)
    - [4.3 状态一致性保证](#43-%E7%8A%B6%E6%80%81%E4%B8%80%E8%87%B4%E6%80%A7%E4%BF%9D%E8%AF%81)
  - [5. 核心概念详解](#5-%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5%E8%AF%A6%E8%A7%A3)
    - [5.1 SinkFunction 接口](#51-sinkfunction-%E6%8E%A5%E5%8F%A3)
    - [5.2 RichSinkFunction 抽象类](#52-richsinkfunction-%E6%8A%BD%E8%B1%A1%E7%B1%BB)
    - [5.3 精确一次语义（Exactly Once）](#53-%E7%B2%BE%E7%A1%AE%E4%B8%80%E6%AC%A1%E8%AF%AD%E4%B9%89exactly-once)
  - [6. 常见问题](#6-%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
    - [6.1 如何选择合适的 Sink？](#61-%E5%A6%82%E4%BD%95%E9%80%89%E6%8B%A9%E5%90%88%E9%80%82%E7%9A%84-sink)
    - [6.2 如何保证数据不丢失？](#62-%E5%A6%82%E4%BD%95%E4%BF%9D%E8%AF%81%E6%95%B0%E6%8D%AE%E4%B8%8D%E4%B8%A2%E5%A4%B1)
    - [6.3 自定义 Sink 的最佳实践](#63-%E8%87%AA%E5%AE%9A%E4%B9%89-sink-%E7%9A%84%E6%9C%80%E4%BD%B3%E5%AE%9E%E8%B7%B5)
    - [6.4 性能优化建议](#64-%E6%80%A7%E8%83%BD%E4%BC%98%E5%8C%96%E5%BB%BA%E8%AE%AE)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 输出算子（Sink）演示

## 1. 项目作用

本项目演示了 Flink 支持的各种输出算子（Sink），包括文件输出、Kafka、Redis、Elasticsearch、MySQL（JDBC）以及自定义
Sink（HBase）等。通过实际代码示例帮助开发者快速掌握 Flink Sink 算子的创建和使用方法。

## 2. 项目结构

```
flink-04-Sink/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源实现
│   └── sink/
│       ├── SinkToFileTest.java                          # 输出到文件示例
│       ├── SinkToKafkaTest.java                        # 输出到 Kafka 示例
│       ├── SinkToRedisTest.java                        # 输出到 Redis 示例
│       ├── SinkToEsTest.java                           # 输出到 Elasticsearch 示例
│       ├── SinkToMySQL.java                            # 输出到 MySQL（JDBC）示例
│       └── SinkCustomtoHBase.java                      # 自定义 Sink 输出示例（HBase）
├── pom.xml                                              # Maven 配置
└── README.md                                            # 本文档
```

## 3. 核心实现

### 3.1 依赖配置

`pom.xml`：引入 Flink 相关依赖和 Sink 连接器

```xml

<dependencies>
    <!-- ==================== Flink 核心依赖 ==================== -->
    <!-- flink-streaming-java: Flink流处理Java API，提供DataStream API -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
    </dependency>

    <!-- flink-clients: Flink客户端依赖，提供作业提交和客户端功能 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients_${scala.binary.version}</artifactId>
    </dependency>

    <!-- ==================== Sink 连接器依赖 ==================== -->
    <!-- Kafka 连接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
    </dependency>

    <!-- Redis 连接器 (Bahir) -->
    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>flink-connector-redis_2.11</artifactId>
    </dependency>

    <!-- Elasticsearch 连接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-elasticsearch6_${scala.binary.version}</artifactId>
    </dependency>

    <!-- JDBC 连接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-jdbc_${scala.binary.version}</artifactId>
    </dependency>

    <!-- MySQL 驱动 -->
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
    </dependency>

    <!-- HBase 客户端 (可选，用于自定义 Sink) -->
    <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-client</artifactId>
    </dependency>

    <!-- ==================== 日志管理依赖 ==================== -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-log4j12</artifactId>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-to-slf4j</artifactId>
    </dependency>
</dependencies>
```

### 3.2 数据模型

#### 3.2.1 Event.java - 事件数据模型

```java
package com.action;

import java.sql.Timestamp;

public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
```

#### 3.2.2 ClickSource.java - 自定义数据源

```java
package com.action;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();    // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔1秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
```

### 3.3 输出到文件

`src/main/java/com/action/sink/SinkToFileTest.java`：使用 `StreamingFileSink` 将数据写入文件系统

**特点说明：**

- **功能**：使用 StreamingFileSink 将数据流写入文件系统
- **支持格式**：行编码（Row-encoded）和批量编码（Bulk-encoded，比如 Parquet）
- **滚动策略**：可以配置文件滚动条件（时间、大小、不活跃时间）
- **状态一致性**：支持精确一次（exactly once）的一致性语义
- **使用场景**：日志收集、数据归档、批量数据输出等

**核心概念：**

1. **StreamingFileSink**：Flink 提供的流式文件系统连接器，继承自 RichSinkFunction
2. **分桶（Buckets）**：数据写入的基本单位，默认基于时间分桶（每小时一个桶）
3. **滚动策略（Rolling Policy）**：控制何时创建新文件的策略
    - 至少包含指定时间的数据
    - 最近指定时间没有收到新数据
    - 文件大小达到指定阈值

```java
package com.action.sink;

import com.action.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 输出到文件示例
 * 使用 StreamingFileSink 将数据写入文件系统
 *
 * 特点说明：
 * - 功能：使用 StreamingFileSink 将数据流写入文件系统
 * - 支持格式：行编码（Row-encoded）和批量编码（Bulk-encoded，比如 Parquet）
 * - 滚动策略：可以配置文件滚动条件（时间、大小、不活跃时间）
 * - 状态一致性：支持精确一次（exactly once）的一致性语义
 * - 使用场景：日志收集、数据归档、批量数据输出等
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 将Event转换成String 写入文件
        stream.map(Event::toString).addSink(fileSink);

        env.execute();
    }
}
```

**滚动策略说明：**

上面的代码设置了在以下 3 种情况下，会滚动分区文件：

- 至少包含 15 分钟的数据
- 最近 5 分钟没有收到新的数据
- 文件大小已达到 1 GB

**注意事项：**

- 旧的方法 `writeAsText()` 和 `writeAsCsv()` 已被弃用，因为不支持并行写入且没有状态一致性保证
- StreamingFileSink 支持分布式文件存储，可以真正实现并行写入
- 需要确保输出目录存在或程序有创建权限

### 3.4 输出到 Kafka

`src/main/java/com/action/sink/SinkToKafkaTest.java`：使用 `FlinkKafkaProducer` 将数据写入 Kafka

**特点说明：**

- **功能**：将数据流写入 Kafka 主题（topic）
- **精确一次语义**：FlinkKafkaProducer 支持端到端的精确一次（exactly once）语义保证
- **两阶段提交**：使用 TwoPhaseCommitSinkFunction 实现事务性保证
- **使用场景**：数据管道、实时数据流、事件流处理等

**核心概念：**

1. **FlinkKafkaProducer**：Flink 提供的 Kafka 生产者连接器
2. **两阶段提交**：提供事务性保证，确保精确一次语义
3. **数据管道**：可以将 Kafka 作为数据源和 Sink，形成完整的数据处理管道

#### 3.4.1 前置条件：Kafka 环境搭建（KRaft 模式）

**说明：** Kafka 3.3+ 版本支持 KRaft 模式，无需 Zookeeper，简化了部署和管理。本示例使用 KRaft 模式在 server01 上搭建 Kafka。

**1. 上传 Kafka 安装包到 server01**

```bash
# 在 Windows 本地执行，上传 kafka 压缩包到 server01
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" \
    "E:\dljd-kafka\ruanjian\kafka_2.13-3.7.0.tgz" \
    vagrant@192.168.56.11:/home/vagrant/
```

**2. 解压 Kafka 到安装目录**

```bash
# 在 server01 上执行
sudo tar -zxvf kafka_2.13-3.7.0.tgz -C /opt/module/
cd /opt/module/kafka_2.13-3.7.0

# 设置目录权限
sudo chown -R vagrant:vagrant /opt/module/kafka_2.13-3.7.0
```

**3. 使用 KRaft 模式启动 Kafka**

```bash
# 进入 bin 目录
cd /opt/module/kafka_2.13-3.7.0/bin

# 步骤1：生成集群 UUID
./kafka-storage.sh random-uuid
# 输出示例：1q86ygN-S3yh2Ao2wwBz9g

# 步骤2：格式化存储目录（使用上一步生成的 UUID）
./kafka-storage.sh format -t 1q86ygN-S3yh2Ao2wwBz9g -c ../config/kraft/server.properties
# 注意：将 1q86ygN-S3yh2Ao2wwBz9g 替换为实际生成的 UUID

# 步骤3：配置外部访问（修改配置文件）
cd /opt/module/kafka_2.13-3.7.0/config/kraft
sudo vim server.properties

# 修改以下配置项：
# listeners=PLAINTEXT://0.0.0.0:9092
# advertised.listeners=PLAINTEXT://server01:9092
# 或者使用 IP 地址：
# advertised.listeners=PLAINTEXT://192.168.56.11:9092

# 说明：
# - listeners=PLAINTEXT://0.0.0.0:9092：监听所有网络接口，允许外部连接
# - advertised.listeners：客户端连接时使用的地址，需要设置为可访问的地址

# 步骤4：启动 Kafka（后台运行）
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-server-start.sh -daemon ../config/kraft/server.properties

# 或者前台运行（方便查看日志）
# ./kafka-server-start.sh ../config/kraft/server.properties &

# 步骤5：验证 Kafka 是否启动成功
ps -ef | grep kafka
# 应该能看到 Kafka 进程

# 查看日志（如果有错误）
tail -f /opt/module/kafka_2.13-3.7.0/logs/kafkaServer.out
```

**4. 创建主题**

```bash
# 进入 bin 目录
cd /opt/module/kafka_2.13-3.7.0/bin

# 创建 clicks 主题
./kafka-topics.sh --create \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --partitions 1 \
    --replication-factor 1

# 或者使用 IP 地址
./kafka-topics.sh --create \
    --topic clicks \
    --bootstrap-server 192.168.56.11:9092 \
    --partitions 1 \
    --replication-factor 1

# 查看主题列表
./kafka-topics.sh --list --bootstrap-server server01:9092

# 查看主题详情
./kafka-topics.sh --describe --topic clicks --bootstrap-server server01:9092
```

**5. 向主题发送数据（测试）**

```bash
# 启动生产者，向 clicks 主题发送数据
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-producer.sh \
    --topic clicks \
    --bootstrap-server server01:9092

# 在终端中输入消息，每行一条，按回车发送
# 例如：
# message1
# message2
# message3
# 按 Ctrl+C 退出
```

**6. 从主题消费数据（测试）**

```bash
# 启动消费者，从 clicks 主题读取数据
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-consumer.sh \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --from-beginning

# 会显示主题中的所有消息
# 按 Ctrl+C 退出
```

**7. 关闭 Kafka**

```bash
# 停止 Kafka 服务
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-server-stop.sh ../config/kraft/server.properties

# 或者使用 kill 命令
ps -ef | grep kafka
kill -9 <进程ID>
```

**8. 重启 Kafka 和验证**

如果之前已经安装并启动过 Kafka，创建了主题，然后停止了服务或重启了 Linux 系统，可以按以下步骤重启 Kafka 并进行验证：

**步骤1：检查 Kafka 进程状态**

```bash
# 检查 Kafka 是否正在运行
ps -ef | grep kafka

# 如果没有看到 Kafka 进程，说明 Kafka 已停止，需要重启
```

**步骤2：启动 Kafka（KRaft 模式）**

```bash
# 进入 Kafka bin 目录
cd /opt/module/kafka_2.13-3.7.0/bin

# 启动 Kafka（后台运行）
./kafka-server-start.sh -daemon ../config/kraft/server.properties

# 或者前台运行（方便查看日志）
# ./kafka-server-start.sh ../config/kraft/server.properties &
```

**注意：** 如果之前已经格式化过存储目录，重启时不需要再次执行 `kafka-storage.sh format` 命令，直接启动即可。

**步骤3：验证 Kafka 是否启动成功**

```bash
# 检查进程
ps -ef | grep kafka
# 应该能看到 Kafka 进程

# 查看日志（如果有错误）
tail -f /opt/module/kafka_2.13-3.7.0/logs/kafkaServer.out

# 或者查看最新的日志文件
tail -f /opt/module/kafka_2.13-3.7.0/logs/server.log
```

**步骤4：验证主题是否存在**

```bash
# 查看所有主题列表
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-topics.sh --list --bootstrap-server server01:9092

# 如果之前创建过 clicks 主题，应该能看到
# 输出示例：
# clicks

# 查看主题详情
./kafka-topics.sh --describe --topic clicks --bootstrap-server server01:9092
```

**步骤5：如果主题不存在，重新创建主题**

```bash
# 如果主题不存在，需要重新创建
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-topics.sh --create \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --partitions 1 \
    --replication-factor 1
```

**步骤6：消费验证 - 测试生产者**

```bash
# 启动生产者，向 clicks 主题发送测试消息
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-producer.sh \
    --topic clicks \
    --bootstrap-server server01:9092

# 在终端中输入消息，每行一条，按回车发送
# 例如：
# test message 1
# test message 2
# test message 3
# 按 Ctrl+C 退出
```

**步骤7：消费验证 - 测试消费者**

```bash
# 启动消费者，从 clicks 主题读取数据
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-consumer.sh \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --from-beginning

# 会显示主题中的所有消息（包括之前发送的消息）
# 输出示例：
# test message 1
# test message 2
# test message 3
# 按 Ctrl+C 退出
```

**步骤8：验证 Flink Sink 写入的数据**

```bash
# 启动消费者，查看 Flink 程序写入的数据
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-consumer.sh \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --from-beginning

# 运行 Flink 程序后，应该能看到：
# Mary,./home,1000
# Bob,./cart,2000
# Alice,./prod?id=100,3000
```

**常见问题：**

**Q1: Kafka 启动失败，提示存储目录未格式化**

**原因：** 存储目录被删除或损坏

**解决方案：**

```bash
# 重新格式化存储目录（注意：这会删除所有数据）
cd /opt/module/kafka_2.13-3.7.0/bin

# 生成新的 UUID
./kafka-storage.sh random-uuid
# 输出示例：1q86ygN-S3yh2Ao2wwBz9g

# 格式化存储目录（使用新生成的 UUID）
./kafka-storage.sh format -t <新生成的UUID> -c ../config/kraft/server.properties

# 然后重新启动 Kafka
./kafka-server-start.sh -daemon ../config/kraft/server.properties
```

**Q2: 无法连接到 Kafka**

**原因：** 网络配置或防火墙问题

**解决方案：**

```bash
# 检查 Kafka 是否监听在正确的端口
netstat -tuln | grep 9092

# 测试连接
telnet server01 9092
# 或
telnet 192.168.56.11 9092

# 检查配置文件中的 advertised.listeners
cat /opt/module/kafka_2.13-3.7.0/config/kraft/server.properties | grep advertised.listeners
```

**Q3: 主题不存在**

**原因：** 主题被删除或从未创建

**解决方案：** 按照步骤5重新创建主题

**KRaft 模式说明：**

- **优势**：无需 Zookeeper，简化部署，提高性能
- **适用版本**：Kafka 3.3+ 版本
- **关键配置**：
    - `listeners`：Kafka 监听的地址和端口
    - `advertised.listeners`：客户端连接时使用的地址（必须可访问）
- **外部访问配置**：
    - `listeners=PLAINTEXT://0.0.0.0:9092`：监听所有网络接口
    - `advertised.listeners=PLAINTEXT://server01:9092`：使用主机名（需要配置 hosts）
    - 或 `advertised.listeners=PLAINTEXT://192.168.56.11:9092`：使用 IP 地址

**客户端 hosts 配置（如果使用主机名）：**

如果 `advertised.listeners` 使用主机名 `server01`，客户端需要配置 hosts 文件以解析该主机名：

```bash
# Windows 系统：编辑 C:\Windows\System32\drivers\etc\hosts
# 添加以下行：
192.168.56.11  server01

# Linux/Mac 系统：编辑 /etc/hosts
# 添加以下行：
192.168.56.11  server01
```

**推荐配置：**

- **开发环境**：使用 IP 地址 `192.168.56.11:9092`，无需配置 hosts
- **生产环境**：使用主机名，便于管理和迁移

#### 3.4.2 代码实现与测试

**（1）添加 Kafka 连接器依赖**

在 `pom.xml` 中添加 Kafka 连接器依赖：

```xml
<!-- Kafka 连接器 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
</dependency>
```

**依赖说明：**

- **flink-connector-kafka**：Flink 官方提供的 Kafka 连接器，支持从 Kafka 读取数据（Source）和向 Kafka 写入数据（Sink）
- **版本要求**：需要与 Flink 版本匹配（本项目使用 Flink 1.13.0）
- **Scala 版本**：`${scala.binary.version}` 在父 POM 中定义为 `2.12`
- **功能特性**：
    - 支持 Kafka 0.10+ 版本
    - 支持精确一次语义（exactly once）
    - 支持两阶段提交（Two-Phase Commit）
    - 支持自动偏移量管理

**（2）编写输出到 Kafka 的示例代码**

```java
package com.action.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 输出到 Kafka 示例
 * 使用 FlinkKafkaProducer 将数据写入 Kafka
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 Kafka 主题（topic）
 * - 精确一次语义：FlinkKafkaProducer 支持端到端的精确一次（exactly once）语义保证
 * - 两阶段提交：使用 TwoPhaseCommitSinkFunction 实现事务性保证
 * - 使用场景：数据管道、实时数据流、事件流处理等
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "server01:9092");

        // 注意：这里需要先创建 input/clicks.csv 文件，或者使用其他数据源
        // DataStreamSource<String> stream = env.readTextFile("input/clicks.csv");

        // 使用 fromElements 作为示例数据源
        DataStreamSource<String> stream = env.fromElements(
                "Mary,./home,1000",
                "Bob,./cart,2000",
                "Alice,./prod?id=100,3000"
        );

        stream
                .addSink(new FlinkKafkaProducer<String>(
                        "clicks",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
```

**（3）运行代码，在 Linux 主机启动一个消费者，查看是否收到数据**

```bash
# 在 server01 上执行
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-consumer.sh --bootstrap-server server01:9092 --topic clicks --from-beginning
```

**完整测试流程：**

**步骤1：在 server01 上启动 Kafka（KRaft 模式）**

```bash
# 在 server01 上执行
cd /opt/module/kafka_2.13-3.7.0/bin
# 生成 UUID 并格式化（首次启动需要）
./kafka-storage.sh random-uuid
./kafka-storage.sh format -t <生成的UUID> -c ../config/kraft/server.properties
# 启动 Kafka
./kafka-server-start.sh -daemon ../config/kraft/server.properties
```

**步骤2：创建主题**

```bash
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-topics.sh --create \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --partitions 1 \
    --replication-factor 1
```

**步骤3：启动 Flink 程序（在本地或客户端）**

```bash
cd flink-04-Sink
mvn compile exec:java -Dexec.mainClass="com.action.sink.SinkToKafkaTest"
```

**步骤4：启动消费者查看数据（在 server01 上）**

```bash
cd /opt/module/kafka_2.13-3.7.0/bin
./kafka-console-consumer.sh \
    --topic clicks \
    --bootstrap-server server01:9092 \
    --from-beginning
```

**步骤5：观察输出**

消费者会显示从 Flink 程序写入的消息：

```
Mary,./home,1000
Bob,./cart,2000
Alice,./prod?id=100,3000
```

**注意事项：**

- FlinkKafkaProducer 继承了抽象类 TwoPhaseCommitSinkFunction，实现了"两阶段提交"的 RichSinkFunction
- 两阶段提交提供了 Flink 向 Kafka 写入数据的事务性保证，能够真正做到精确一次（exactly once）的状态一致性
- 可以同时将 Kafka 作为 Flink 程序的数据源和写入结果的外部系统，形成"数据管道"应用

### 3.5 输出到 Redis

`src/main/java/com/action/sink/SinkToRedisTest.java`：使用 `RedisSink` 将数据写入 Redis

**特点说明：**

- **功能**：将数据流写入 Redis 数据库
- **连接器**：使用 Apache Bahir 提供的 Flink-Redis 连接器
- **数据结构**：支持多种 Redis 数据结构（string、hash、list、set 等）
- **使用场景**：实时缓存、数据存储、会话管理、计数器等

**核心概念：**

1. **RedisSink**：Bahir 项目提供的 Redis Sink 连接器，继承自 RichSinkFunction
2. **RedisMapper**：定义如何将数据转换成可以写入 Redis 的类型
3. **Redis 命令**：支持多种 Redis 命令（HSET、SET、LPUSH 等）

#### 3.5.1 前置条件：Redis 环境搭建

**说明：** Redis 是一个开源的内存数据结构存储系统，可以用作数据库、缓存和消息代理。本示例在 server01 上搭建单节点 Redis
进行测试。

**1. 安装 Redis**

**方式一：使用包管理器安装（推荐）**

```bash
# CentOS/RHEL 系统
sudo yum install -y redis

# 或者 Ubuntu/Debian 系统
sudo apt-get install -y redis-server
```

**方式二：从源码编译安装**

```bash
# 在 server01 上执行

# 步骤1：安装编译工具（必须先安装，否则编译会失败）
# CentOS/RHEL 系统
sudo yum install -y gcc gcc-c++ make

# 或者 Ubuntu/Debian 系统
# sudo apt-get install -y gcc g++ make

# 步骤2：下载 Redis 源码包（下载到用户目录）
cd ~
wget http://download.redis.io/releases/redis-6.2.6.tar.gz

# 步骤3：解压 Redis 到安装目录
sudo tar -zxvf redis-6.2.6.tar.gz -C /opt/module/
cd /opt/module/redis-6.2.6

# 步骤4：设置目录权限
sudo chown -R vagrant:vagrant /opt/module/redis-6.2.6

# 步骤5：安装 jemalloc 开发库（Redis 编译需要）
sudo yum install -y jemalloc-devel

# 步骤6：编译 Redis（在解压后的目录中编译）
make

# 注意：如果安装 jemalloc-devel 失败，可以使用系统的 malloc 编译：
# make MALLOC=libc

# 步骤7：验证编译是否成功
# 检查是否生成了 redis-server 和 redis-cli 可执行文件
ls -lh src/redis-server
ls -lh src/redis-cli

# 如果文件不存在，说明编译失败，需要检查编译错误
# 如果文件存在，说明编译成功，可以继续下一步

# 步骤8：创建配置目录和数据目录
sudo mkdir -p /opt/module/redis-6.2.6/conf
sudo mkdir -p /opt/module/redis-6.2.6/data
sudo mkdir -p /opt/module/redis-6.2.6/logs

# （可选）创建软链接，方便使用 redis-cli 和 redis-server 命令
# 创建软链接后，可以直接使用 redis-cli 和 redis-server 命令，无需完整路径
sudo ln -s /opt/module/redis-6.2.6/src/redis-cli /usr/local/bin/redis-cli
sudo ln -s /opt/module/redis-6.2.6/src/redis-server /usr/local/bin/redis-server

# 验证软链接是否创建成功
which redis-cli
which redis-server

# 创建软链接后，可以直接使用命令（注意使用单引号包裹密码）
# redis-cli -a 'Redis@2024!Secure#Pass' ping
```

**2. 配置 Redis**

```bash
# 在 server01 上执行
cd /opt/module/redis-6.2.6

# 复制配置文件
sudo cp redis.conf conf/redis.conf

# 编辑配置文件
sudo vim conf/redis.conf

# 修改以下配置项：
# daemonize yes                    # 后台运行
# bind 0.0.0.0                    # 允许外部连接（或注释掉 bind 127.0.0.1）
# protected-mode no                # 关闭保护模式（允许外部连接）
# port 6379                        # 端口号（默认 6379）
# dir /opt/module/redis-6.2.6/data       # 数据目录
# logfile /opt/module/redis-6.2.6/logs/redis.log  # 日志文件
# requirepass Redis@2024!Secure#Pass    # 设置 Redis 密码（重要：生产环境必须设置复杂密码）
```

**密码配置说明：**

- **requirepass**：设置 Redis 访问密码，建议使用复杂密码（包含大小写字母、数字、特殊字符）
- **本示例密码**：`Redis@2024!Secure#Pass`（仅用于演示，生产环境请使用更复杂的密码）
- **密码要求**：
    - 长度至少 16 个字符
    - 包含大小写字母、数字和特殊字符
    - 避免使用常见密码或字典词汇
- **安全提示**：配置密码后，所有客户端连接都需要提供密码才能访问 Redis

**3. 启动 Redis**

**重要提示：** 从源码编译安装的 Redis 不会创建 systemd 服务，不能使用 `systemctl` 或 `service` 命令启动。必须使用直接启动的方式。

```bash
# 方式一：使用包管理器安装的 Redis
sudo systemctl start redis
# 或
sudo service redis start

# 方式二：从源码编译安装的 Redis（必须使用这种方式）
# 进入 Redis 安装目录
cd /opt/module/redis-6.2.6

# 前台运行（方便查看日志，按 Ctrl+C 退出）
# /opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf

# 后台运行（推荐）
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes
```

**启动方式说明：**

- **包管理器安装**：会自动创建 systemd 服务，可以使用 `systemctl` 或 `service` 命令管理
- **源码编译安装**：不会创建 systemd 服务，必须使用 `redis-server` 命令直接启动
- **错误提示**：如果看到 `Unit not found` 错误，说明使用的是源码编译安装，需要使用方式二启动

**4. 验证 Redis 是否启动成功**

```bash
# 检查进程
ps -ef | grep redis
# 应该能看到 Redis 进程

# 测试连接（如果配置了密码，使用 -a 参数，注意使用单引号包裹密码）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' ping
# 应该返回：PONG

# 或者指定主机和端口
/opt/module/redis-6.2.6/src/redis-cli -h server01 -p 6379 -a 'Redis@2024!Secure#Pass' ping

# 如果创建了软链接，可以直接使用 redis-cli 命令
# redis-cli -a 'Redis@2024!Secure#Pass' ping
```

**5. 测试 Redis 基本功能**

```bash
# 连接 Redis（从源码编译安装，需要使用完整路径）
/opt/module/redis-6.2.6/src/redis-cli
# 或
/opt/module/redis-6.2.6/src/redis-cli -h server01 -p 6379

# 如果配置了密码，需要先认证（在 redis-cli 内部）
127.0.0.1:6379> AUTH Redis@2024!Secure#Pass
OK

# 测试基本命令
127.0.0.1:6379> set test_key "test_value"
OK
127.0.0.1:6379> get test_key
"test_value"
127.0.0.1:6379> del test_key
(integer) 1
127.0.0.1:6379> exit

# 或者使用 -a 参数在连接时直接提供密码（注意使用单引号）
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass'
```

**6. 配置 Redis 允许外部访问（如果需要）**

```bash
# 编辑配置文件
vim /opt/module/redis-6.2.6/conf/redis.conf

# 修改以下配置：
# bind 0.0.0.0                    # 监听所有网络接口
# protected-mode no                # 关闭保护模式

# 重启 Redis
sudo systemctl restart redis
# 或
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes
```

**7. 关闭 Redis**

```bash
# 方式一：使用 redis-cli 关闭（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' shutdown

# 方式二：使用 systemctl（如果使用包管理器安装）
sudo systemctl stop redis

# 方式三：使用 kill 命令
ps -ef | grep redis
kill -9 <进程ID>
```

**8. 重启 Redis 和验证**

如果之前已经安装并启动过 Redis，然后停止了服务或重启了 Linux 系统，可以按以下步骤重启 Redis 并进行验证：

**步骤1：检查 Redis 进程状态**

```bash
# 检查 Redis 是否正在运行
ps -ef | grep redis

# 如果没有看到 Redis 进程，说明 Redis 已停止，需要重启
```

**步骤2：启动 Redis**

```bash
# 方式一：使用 systemctl（如果使用包管理器安装）
sudo systemctl start redis

# 方式二：从源码编译安装的 Redis
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes
```

**步骤3：验证 Redis 是否启动成功**

```bash
# 检查进程
ps -ef | grep redis
# 应该能看到 Redis 进程

# 测试连接（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' ping
# 应该返回：PONG

# 查看 Redis 信息
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' info server
```

**步骤4：测试 Redis 基本功能**

```bash
# 连接 Redis（从源码编译安装，需要使用完整路径）
/opt/module/redis-6.2.6/src/redis-cli

# 如果配置了密码，需要先认证或使用 -a 参数
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass'

# 测试基本命令
127.0.0.1:6379> keys *
(empty list or set)
127.0.0.1:6379> set test_key "test_value"
OK
127.0.0.1:6379> get test_key
"test_value"
127.0.0.1:6379> del test_key
(integer) 1
127.0.0.1:6379> exit
```

**步骤5：测试 Hash 数据结构（用于验证 Flink Sink）**

```bash
# 连接 Redis（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass'

# 测试 Hash 命令（Flink 程序会使用 HSET 命令）
127.0.0.1:6379> hset clicks user1 url1
(integer) 1
127.0.0.1:6379> hgetall clicks
1) "user1"
2) "url1"
127.0.0.1:6379> hdel clicks user1
(integer) 1
127.0.0.1:6379> exit
```

**常见问题：**

**Q1: Redis 启动失败**

**原因：** 配置文件错误、端口被占用、权限问题等

**解决方案：**

```bash
# 检查端口是否被占用
netstat -tuln | grep 6379

# 查看 Redis 日志
tail -f /opt/module/redis-6.2.6/logs/redis.log
# 或
tail -f /var/log/redis/redis-server.log

# 检查配置文件语法
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --test-memory 1
```

**Q2: 无法从外部连接 Redis**

**原因：** 防火墙、bind 配置、protected-mode 配置等

**解决方案：**

```bash
# 检查防火墙
sudo firewall-cmd --list-all
# 或
sudo iptables -L

# 开放 Redis 端口
sudo firewall-cmd --permanent --add-port=6379/tcp
sudo firewall-cmd --reload

# 检查配置文件
cat /opt/module/redis-6.2.6/conf/redis.conf | grep -E "bind|protected-mode"
```

**Q3: Redis 连接被拒绝**

**原因：** Redis 未启动或网络配置问题

**解决方案：**

```bash
# 检查 Redis 是否启动
ps -ef | grep redis

# 测试本地连接（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' ping

# 测试远程连接
/opt/module/redis-6.2.6/src/redis-cli -h server01 -p 6379 -a 'Redis@2024!Secure#Pass' ping
```

**Q4: 使用 systemctl 启动 Redis 失败，提示 "Unit not found"**

**原因：** 从源码编译安装的 Redis 不会自动创建 systemd 服务文件

**解决方案：**

```bash
# 从源码编译安装的 Redis 不能使用 systemctl 启动
# 必须使用直接启动的方式

# 进入 Redis 安装目录
cd /opt/module/redis-6.2.6

# 后台启动 Redis
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes

# 验证是否启动成功
ps -ef | grep redis
redis-cli -a 'Redis@2024!Secure#Pass' ping
```

**Q5: redis-cli 连接时提示 "event not found" 错误**

**原因：** bash 历史扩展问题，密码中包含 `!` 字符时会被 bash 解释为历史扩展命令

**错误示例：**

```bash
redis-cli -a 'Redis@2024!Secure#Pass' ping
# 错误：-bash: !Secure#Pass: event not found
```

**解决方案：**

```bash
# 方案A：使用单引号包裹密码（推荐）
redis-cli -a 'Redis@2024!Secure#Pass' ping

# 方案B：禁用历史扩展（临时）
set +H
redis-cli -a 'Redis@2024!Secure#Pass' ping
set -H  # 重新启用历史扩展

# 方案C：转义特殊字符
redis-cli -a 'Redis@2024!Secure#Pass' ping
```

**推荐做法：** 使用单引号包裹包含特殊字符的密码，这样可以避免 bash 解释特殊字符。

**Q6: 使用 redis-cli 时提示 "command not found"**

**原因：** 从源码编译安装的 Redis，`redis-cli` 可执行文件不在系统 PATH 中

**解决方案：**

```bash
# 方案A：使用完整路径（推荐）
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' ping

# 方案B：创建软链接（方便使用）
sudo ln -s /opt/module/redis-6.2.6/src/redis-cli /usr/local/bin/redis-cli
sudo ln -s /opt/module/redis-6.2.6/src/redis-server /usr/local/bin/redis-server

# 创建软链接后，可以直接使用 redis-cli 命令
redis-cli -a 'Redis@2024!Secure#Pass' ping

# 方案C：添加到 PATH（临时）
export PATH=$PATH:/opt/module/redis-6.2.6/src
redis-cli -a 'Redis@2024!Secure#Pass' ping

# 方案D：添加到 PATH（永久）
echo 'export PATH=$PATH:/opt/module/redis-6.2.6/src' >> ~/.bashrc
source ~/.bashrc
redis-cli -a 'Redis@2024!Secure#Pass' ping
```

**Q7: 启动 Redis 时提示 "No such file or directory" 或 "command not found"**

**原因：** Redis 编译失败或未编译，`redis-server` 可执行文件不存在

**解决方案：**

```bash
# 步骤1：检查 redis-server 文件是否存在
cd /opt/module/redis-6.2.6
ls -lh src/redis-server

# 如果文件不存在，说明编译失败或未编译，需要重新编译

# 步骤2：检查编译工具是否安装
gcc --version
make --version

# 如果命令不存在，需要先安装编译工具
sudo yum install -y gcc gcc-c++ make

# 步骤3：清理之前的编译文件（如果有）
make clean

# 步骤4：重新编译 Redis
make

# 步骤5：验证编译是否成功
ls -lh src/redis-server
ls -lh src/redis-cli

# 如果文件存在，说明编译成功
# 如果仍然不存在，查看编译错误信息，可能需要安装其他依赖

# 步骤6：编译成功后，再次尝试启动
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes
```

**常见编译错误：**

```bash
# 错误1：缺少编译工具
# 解决：sudo yum install -y gcc gcc-c++ make

# 错误2：缺少 jemalloc（fatal error: jemalloc/jemalloc.h: No such file or directory）
# 这是最常见的编译错误，Redis 默认使用 jemalloc 内存分配器
# 解决方案有两种：

# 方案A：安装 jemalloc 开发库（推荐）
sudo yum install -y jemalloc-devel

# 然后重新编译
make clean
make

# 方案B：使用系统的 malloc（如果无法安装 jemalloc）
make clean
make MALLOC=libc

# 错误3：缺少其他依赖（如 tcl）
# 解决：sudo yum install -y tcl

# 错误4：权限问题
# 解决：确保有编译目录的写权限，或使用 sudo make（不推荐）
```

**如何区分安装方式：**

```bash
# 检查是否有 systemd 服务文件
systemctl list-unit-files | grep redis

# 如果没有任何输出，说明是源码编译安装，需要使用直接启动方式
# 如果有 redis.service，说明是包管理器安装，可以使用 systemctl 管理
```

#### 3.5.2 代码实现与测试

**（1）添加 Redis 连接器依赖**

在 `pom.xml` 中添加 Redis 连接器依赖：

```xml
<!-- Redis 连接器 (Bahir) -->
<dependency>
    <groupId>org.apache.bahir</groupId>
    <artifactId>flink-connector-redis_2.11</artifactId>
    <version>1.0</version>
</dependency>
```

**依赖说明：**

- **flink-connector-redis**：Apache Bahir 项目提供的 Flink-Redis 连接器
- **版本说明**：当前版本为 1.0，支持的 Scala 版本为 2.11
- **功能特性**：
    - 支持多种 Redis 数据结构（string、hash、list、set、sorted set 等）
    - 支持多种 Redis 命令（SET、HSET、LPUSH、SADD 等）
    - 支持连接池配置
    - 支持单节点和集群模式

**（2）编写输出到 Redis 的示例代码**

```java
package com.action.sink;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 输出到 Redis 示例
 * 使用 RedisSink 将数据写入 Redis
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 Redis 数据库
 * - 连接器：使用 Apache Bahir 提供的 Flink-Redis 连接器
 * - 数据结构：支持多种 Redis 数据结构（string、hash、list、set 等）
 * - 使用场景：实时缓存、数据存储、会话管理、计数器等
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建一个到 redis 连接的配置
        // 注意：如果 Redis 配置了密码，需要使用 setPassword() 方法设置密码
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("server01")
                .setPort(6379)
                .setPassword("Redis@2024!Secure#Pass")
                .build();

        env.addSource(new ClickSource())
                .addSink(new RedisSink<Event>(conf, new MyRedisMapper()));

        env.execute();
    }

    /**
     * Redis 映射类
     * 实现 RedisMapper 接口，定义如何将数据转换成可以写入 Redis 的类型
     */
    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public String getKeyFromData(Event e) {
            return e.user;
        }

        @Override
        public String getValueFromData(Event e) {
            return e.url;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }
    }
}
```

**（3）运行代码，在 Redis 中查看是否收到数据**

**完整测试流程：**

**步骤1：在 server01 上启动 Redis**

```bash
# 启动 Redis
sudo systemctl start redis
# 或
/opt/module/redis-6.2.6/src/redis-server /opt/module/redis-6.2.6/conf/redis.conf --daemonize yes

# 验证 Redis 是否启动（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass' ping
# 应该返回：PONG
```

**步骤2：启动 Flink 程序（在本地或客户端）**

```bash
cd flink-04-Sink
mvn compile exec:java -Dexec.mainClass="com.action.sink.SinkToRedisTest"
```

**步骤3：在 Redis 中查看数据（在 server01 上）**

```bash
# 连接 Redis（如果配置了密码，使用 -a 参数）
# 注意：从源码编译安装的 Redis，需要使用完整路径
/opt/module/redis-6.2.6/src/redis-cli -a 'Redis@2024!Secure#Pass'
# 或
/opt/module/redis-6.2.6/src/redis-cli -h server01 -p 6379 -a 'Redis@2024!Secure#Pass'

# 查看所有键
server01:6379> keys *
1) "clicks"

# 查看 clicks 哈希表的所有数据
server01:6379> hgetall clicks
1) "Mary"
2) "./home"
3) "Bob"
4) "./cart"
4) "Alice"
5) "./prod?id=1"

# 查看特定用户的 URL
server01:6379> hget clicks Mary
"./home"

# 退出 Redis CLI
server01:6379> exit
```

**步骤4：观察输出**

Redis 中会保存 Flink 程序写入的数据，以哈希表（hash）的形式存储：

- 哈希表名：`clicks`
- Key：`user`（用户名）
- Value：`url`（URL 地址）

**注意：** 由于使用 HSET 命令，如果同一个 user 出现多次，后面的数据会覆盖前面的数据。

**注意事项：**

- RedisSink 的构造方法需要传入两个参数：
    - FlinkJedisConfigBase：Jedis 的连接配置
    - RedisMapper：Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型
- **密码配置**：如果 Redis 配置了密码，必须在 `FlinkJedisPoolConfig.Builder` 中使用 `setPassword()` 方法设置密码，否则连接会失败
- **连接配置**：建议同时设置 `setHost()`、`setPort()` 和 `setPassword()` 方法，确保连接配置完整
- 保存到 Redis 时调用的命令是 HSET，所以是保存为哈希表（hash），表名为"clicks"
- 保存的数据以 user 为 key，以 url 为 value，每来一条数据就会做一次转换
- 注意：hash 中的 key 重复了，后面的会把前面的覆盖掉，所以发送了多条数据，Redis 中可能只有部分数据

### 3.6 输出到 Elasticsearch

`src/main/java/com/action/sink/SinkToEsTest.java`：使用 `ElasticsearchSink` 将数据写入 Elasticsearch

**特点说明：**

- **功能**：将数据流写入 Elasticsearch 索引
- **连接器**：Flink 官方提供的 Elasticsearch Sink 连接器
- **版本支持**：Flink 1.13 支持 Elasticsearch 6.x 和 7.x
- **使用场景**：日志分析、全文搜索、实时数据索引、监控数据存储等

**核心概念：**

1. **ElasticsearchSink**：Flink 官方提供的 Elasticsearch Sink 连接器
2. **ElasticsearchSinkFunction**：定义如何将数据转换成 Elasticsearch 的 IndexRequest
3. **Builder 模式**：使用 Builder 内部静态类创建 SinkFunction

#### 3.6.1 前置条件：Elasticsearch 环境搭建

**说明：** Elasticsearch 是一个分布式的开源搜索和分析引擎，适用于所有类型的数据。本示例在 server01 上搭建单节点
Elasticsearch 进行测试。

**1. 安装 Java 环境（Elasticsearch 需要 Java 8 或更高版本）**

```bash
# 检查 Java 版本
java -version

# 如果没有安装 Java，需要先安装
# CentOS/RHEL 系统
sudo yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel

# 或者 Ubuntu/Debian 系统
# sudo apt-get install -y openjdk-8-jdk

# 设置 JAVA_HOME 环境变量
# 查找 Java 安装路径
which java
readlink -f $(which java)

# 编辑 ~/.bashrc 或 /etc/profile
echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# 验证 Java 环境
java -version
echo $JAVA_HOME
```

**2. 下载并安装 Elasticsearch**

```bash
# 在 server01 上执行

# 步骤1：下载 Elasticsearch 安装包（以 6.8.23 版本为例，与 Flink 1.13 兼容）
cd ~
sudo wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.8.23.tar.gz

# 步骤2：解压 Elasticsearch 到安装目录
sudo tar -zxvf elasticsearch-6.8.23.tar.gz -C /opt/module/
cd /opt/module/elasticsearch-6.8.23

# 步骤3：设置目录权限
sudo chown -R vagrant:vagrant /opt/module/elasticsearch-6.8.23

# 步骤4：创建数据目录和日志目录
sudo mkdir -p /opt/module/elasticsearch-6.8.23/data
sudo mkdir -p /opt/module/elasticsearch-6.8.23/logs
sudo chown -R vagrant:vagrant /opt/module/elasticsearch-6.8.23/data
sudo chown -R vagrant:vagrant /opt/module/elasticsearch-6.8.23/logs
```

**3. 配置 Elasticsearch**

```bash
# 在 server01 上执行
cd /opt/module/elasticsearch-6.8.23

# 编辑配置文件
sudo vim config/elasticsearch.yml

# 修改以下配置项：
# cluster.name: my-application                    # 集群名称
# node.name: node-1                                # 节点名称
# path.data: /opt/module/elasticsearch-6.8.23/data # 数据目录
# path.logs: /opt/module/elasticsearch-6.8.23/logs # 日志目录
# network.host: 0.0.0.0                            # 监听所有网络接口，允许外部连接
# http.port: 9200                                   # HTTP 端口（默认 9200）
# discovery.type: single-node                      # 单节点模式（开发测试用）
# bootstrap.memory_lock: false                     # 禁用内存锁定（避免权限问题）

# 重要配置说明：
# - network.host: 0.0.0.0 允许外部访问
# - discovery.type: single-node 单节点模式，无需配置 discovery.seed_hosts
# - bootstrap.memory_lock: false 避免需要 root 权限锁定内存
```

**4. 配置系统参数（可选，用于生产环境）**

```bash
# 编辑 /etc/security/limits.conf
sudo vim /etc/security/limits.conf

# 添加以下内容（Elasticsearch 用户需要，如果使用非 root 用户运行）
# vagrant soft nofile 65536
# vagrant hard nofile 65536
# vagrant soft nproc 4096
# vagrant hard nproc 4096

# 编辑 /etc/sysctl.conf
sudo vim /etc/sysctl.conf

# 添加以下内容
# vm.max_map_count=262144

# 应用配置
sudo sysctl -p
```

**5. 启动 Elasticsearch**

```bash
# 进入 Elasticsearch 安装目录
cd /opt/module/elasticsearch-6.8.23

# 前台运行（方便查看日志，按 Ctrl+C 退出）
# bin/elasticsearch

# 后台运行（推荐）
bin/elasticsearch -d

# 或者使用 nohup 后台运行
# nohup bin/elasticsearch > logs/elasticsearch.log 2>&1 &
```

**6. 验证 Elasticsearch 是否启动成功**

```bash
# 检查进程
ps -ef | grep elasticsearch
# 应该能看到 Elasticsearch 进程

# 测试连接（等待几秒钟让 Elasticsearch 完全启动）
curl http://localhost:9200
# 或
curl http://server01:9200
# 或
curl http://192.168.56.11:9200

# 应该返回类似以下内容：
# {
#   "name" : "node-1",
#   "cluster_name" : "my-application",
#   "cluster_uuid" : "...",
#   "version" : {
#     "number" : "6.8.23",
#     ...
#   }
# }

# 查看集群健康状态
curl http://localhost:9200/_cluster/health?pretty

# 应该返回：
# {
#   "cluster_name" : "my-application",
#   "status" : "green",
#   ...
# }
```

**7. 测试 Elasticsearch 基本功能**

```bash
# 创建索引
curl -X PUT "localhost:9200/test_index?pretty"

# 查看索引
curl -X GET "localhost:9200/test_index?pretty"

# 插入文档
curl -X PUT "localhost:9200/test_index/_doc/1?pretty" -H 'Content-Type: application/json' -d'
{
  "user": "test_user",
  "message": "test message"
}
'

# 查询文档
curl -X GET "localhost:9200/test_index/_doc/1?pretty"

# 删除索引
curl -X DELETE "localhost:9200/test_index?pretty"
```

**8. 关闭 Elasticsearch**

```bash
# 方式一：使用 kill 命令
ps -ef | grep elasticsearch
kill -9 <进程ID>

# 方式二：使用 pkill 命令
pkill -f elasticsearch

# 方式三：如果使用 systemd 服务（需要先创建服务文件）
# sudo systemctl stop elasticsearch
```

**9. 重启 Elasticsearch 和验证**

如果之前已经安装并启动过 Elasticsearch，然后停止了服务或重启了 Linux 系统，可以按以下步骤重启 Elasticsearch 并进行验证：

**步骤1：检查 Elasticsearch 进程状态**

```bash
# 检查 Elasticsearch 是否正在运行
ps -ef | grep elasticsearch

# 如果没有看到 Elasticsearch 进程，说明 Elasticsearch 已停止，需要重启
```

**步骤2：启动 Elasticsearch**

```bash
# 进入 Elasticsearch 安装目录
cd /opt/module/elasticsearch-6.8.23

# 后台启动 Elasticsearch
bin/elasticsearch -d

# 或者使用 nohup
# nohup bin/elasticsearch > logs/elasticsearch.log 2>&1 &
```

**步骤3：验证 Elasticsearch 是否启动成功**

```bash
# 等待几秒钟让 Elasticsearch 完全启动
sleep 5

# 检查进程
ps -ef | grep elasticsearch
# 应该能看到 Elasticsearch 进程

# 测试连接
curl http://localhost:9200

# 查看集群健康状态
curl http://localhost:9200/_cluster/health?pretty
# 应该返回 status: "green" 或 "yellow"
```

**步骤4：测试 Elasticsearch 基本功能**

```bash
# 创建测试索引
curl -X PUT "localhost:9200/test_index?pretty"

# 插入测试文档
curl -X PUT "localhost:9200/test_index/_doc/1?pretty" -H 'Content-Type: application/json' -d'
{
  "user": "test_user",
  "message": "test message"
}
'

# 查询测试文档
curl -X GET "localhost:9200/test_index/_doc/1?pretty"

# 删除测试索引
curl -X DELETE "localhost:9200/test_index?pretty"
```

**常见问题：**

**Q1: Elasticsearch 启动失败，提示 "max file descriptors [4096] for elasticsearch process is too low"**

**原因：** 系统文件描述符限制太低

**解决方案：**

```bash
# 编辑 /etc/security/limits.conf
sudo vim /etc/security/limits.conf

# 添加以下内容（将 vagrant 替换为实际运行 Elasticsearch 的用户）
vagrant soft nofile 65536
vagrant hard nofile 65536

# 重新登录或重启系统使配置生效
```

**Q2: Elasticsearch 启动失败，提示 "max virtual memory areas vm.max_map_count [65530] is too low"**

**原因：** 虚拟内存映射数量限制太低

**解决方案：**

```bash
# 编辑 /etc/sysctl.conf
sudo vim /etc/sysctl.conf

# 添加以下内容
vm.max_map_count=262144

# 应用配置
sudo sysctl -p
```

**Q3: 无法从外部连接 Elasticsearch**

**原因：** 防火墙或网络配置问题

**解决方案：**

```bash
# 检查防火墙
sudo firewall-cmd --list-all
# 或
sudo iptables -L

# 开放 Elasticsearch 端口
sudo firewall-cmd --permanent --add-port=9200/tcp
sudo firewall-cmd --reload

# 检查配置文件中的 network.host 设置
cat /opt/module/elasticsearch-6.8.23/config/elasticsearch.yml | grep network.host
# 应该设置为 0.0.0.0 或具体 IP 地址
```

**Q4: Elasticsearch 启动后立即退出**

**原因：** 配置文件错误、权限问题、Java 版本不兼容等

**解决方案：**

```bash
# 查看 Elasticsearch 日志
tail -f /opt/module/elasticsearch-6.8.23/logs/elasticsearch.log

# 检查配置文件语法
/opt/module/elasticsearch-6.8.23/bin/elasticsearch -t

# 检查 Java 版本
java -version
# Elasticsearch 6.8 需要 Java 8 或更高版本

# 检查目录权限
ls -la /opt/module/elasticsearch-6.8.23/
# 确保数据目录和日志目录有写权限
```

**Q5: 使用 systemctl 启动 Elasticsearch 失败，提示 "Unit not found"**

**原因：** 从压缩包安装的 Elasticsearch 不会自动创建 systemd 服务文件

**解决方案：**

```bash
# 从压缩包安装的 Elasticsearch 不能使用 systemctl 启动
# 必须使用直接启动的方式

# 进入 Elasticsearch 安装目录
cd /opt/module/elasticsearch-6.8.23

# 后台启动 Elasticsearch
bin/elasticsearch -d

# 验证是否启动成功
ps -ef | grep elasticsearch
curl http://localhost:9200
```

**版本说明：**

- **Elasticsearch 6.8.23**：与 Flink 1.13 兼容，支持 `flink-connector-elasticsearch6`
- **Elasticsearch 7.x**：需要使用 `flink-connector-elasticsearch7`，且代码中不需要定义 type
- **本示例使用 Elasticsearch 6.8.23**：代码中需要定义 type（如 `type("type")`）

#### 3.6.2 代码实现与测试

**（1）添加 Elasticsearch 连接器依赖**

在 `pom.xml` 中添加 Elasticsearch 连接器依赖：

```xml
<!-- Elasticsearch 连接器 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch6_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

**依赖说明：**

- **flink-connector-elasticsearch6**：Flink 官方提供的 Elasticsearch 6.x 连接器
- **版本要求**：需要与 Flink 版本匹配（本项目使用 Flink 1.13.0）
- **Scala 版本**：`${scala.binary.version}` 在父 POM 中定义为 `2.12`
- **功能特性**：
    - 支持 Elasticsearch 6.x 版本
    - 支持精确一次语义（exactly once）
    - 支持批量写入
    - 支持自动重试机制

**（2）编写输出到 Elasticsearch 的示例代码**

```java
package com.action.sink;

import com.action.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 输出到 Elasticsearch 示例
 * 使用 ElasticsearchSink 将数据写入 Elasticsearch
 *
 * 特点说明：
 * - 功能：将数据流写入 Elasticsearch 索引
 * - 连接器：Flink 官方提供的 Elasticsearch Sink 连接器
 * - 版本支持：Flink 1.13 支持 Elasticsearch 6.x 和 7.x
 * - 使用场景：日志分析、全文搜索、实时数据索引、监控数据存储等
 */
public class SinkToEsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("server01", 9200, "http"));

        // 创建一个ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event element, RuntimeContext ctx, RequestIndexer indexer) {
                HashMap<String, String> data = new HashMap<>();
                data.put(element.user, element.url);

                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")    // Es 6 必须定义 type
                        .source(data);

                indexer.add(request);
            }
        };

        stream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }
}
```

**（3）运行代码，访问 Elasticsearch 查看是否收到数据**

**完整测试流程：**

**步骤1：在 server01 上启动 Elasticsearch**

```bash
# 进入 Elasticsearch 安装目录
cd /opt/module/elasticsearch-6.8.23

# 启动 Elasticsearch（后台运行）
bin/elasticsearch -d

# 等待几秒钟让 Elasticsearch 完全启动
sleep 5

# 验证 Elasticsearch 是否启动成功
curl http://localhost:9200
```

**步骤2：启动 Flink 程序（在本地或客户端）**

```bash
cd flink-04-Sink
mvn compile exec:java -Dexec.mainClass="com.action.sink.SinkToEsTest"
```

**步骤3：在 Elasticsearch 中查看数据（在 server01 上）**

```bash
# 查询 clicks 索引的所有文档
curl -X GET "localhost:9200/clicks/_search?pretty"

# 或者使用浏览器访问
# http://server01:9200/clicks/_search?pretty
```

**查询结果示例：**

```json
{
  "took": 5,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 9,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "clicks",
        "_type": "type",
        "_id": "dAxBYHoB7eAyu-y5suyU",
        "_score": 1.0,
        "_source": {
          "Mary": "./home"
        }
      },
      {
        "_index": "clicks",
        "_type": "type",
        "_id": "eBxBYHoB7eAyu-y5suyV",
        "_score": 1.0,
        "_source": {
          "Bob": "./cart"
        }
      }
      ...
    ]
  }
}
```

**步骤4：观察输出**

Elasticsearch 中会保存 Flink 程序写入的数据，以索引文档的形式存储：

- 索引名：`clicks`
- 类型：`type`（Elasticsearch 6.x 必须定义 type）
- 文档内容：以 `user` 为 key，`url` 为 value 的键值对

**注意事项：**

- ElasticsearchSink 的构造方法是私有（private）的，需要使用 Builder 内部静态类
- Builder 的构造方法需要两个参数：
    - httpHosts：连接到的 Elasticsearch 集群主机列表
    - elasticsearchSinkFunction：用来说明具体处理逻辑、准备数据向 Elasticsearch 发送请求的函数
- 具体的操作需要重写 elasticsearchSinkFunction 中的 process 方法
- Elasticsearch 6.x 版本必须定义 type，7.x 版本可以省略 type
- 如果 Elasticsearch 运行在 `hadoop102` 主机上，需要确保客户端可以访问该主机（配置 hosts 或使用 IP 地址）

### 3.7 输出到 MySQL（JDBC）

`src/main/java/com/action/sink/SinkToMySQL.java`：使用 `JdbcSink` 将数据写入 MySQL 数据库

**特点说明：**

- **功能**：将数据流写入 MySQL 关系型数据库
- **连接器**：Flink 官方提供的 JDBC 连接器
- **批处理**：支持批量写入，提高写入效率
- **使用场景**：结果数据存储、数据归档、报表数据、业务数据存储等

**核心概念：**

1. **JdbcSink**：Flink 官方提供的 JDBC Sink 连接器
2. **批量写入**：通过 JdbcExecutionOptions 配置批量写入参数
3. **连接配置**：通过 JdbcConnectionOptions 配置数据库连接信息

**（1）添加依赖**

```xml

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-jdbc_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
<groupId>mysql</groupId>
<artifactId>mysql-connector-java</artifactId>
<version>5.1.47</version>
</dependency>
```

**（2）启动 MySQL，创建数据库和表**

首先创建数据库：

```sql
CREATE
DATABASE IF NOT EXISTS userbehavior;
USE
userbehavior;
```

然后创建表 clicks：

```sql
CREATE TABLE clicks
(
    user VARCHAR(20)  NOT NULL,
    url  VARCHAR(100) NOT NULL
);
```

**（3）编写输出到 MySQL 的示例代码**

```java
package com.action.sink;

import com.action.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 输出到 MySQL（JDBC）示例
 * 使用 JdbcSink 将数据写入 MySQL 数据库
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 MySQL 关系型数据库
 * - 连接器：Flink 官方提供的 JDBC 连接器
 * - 批处理：支持批量写入，提高写入效率
 * - 使用场景：结果数据存储、数据归档、报表数据、业务数据存储等

 启动 MySQL，创建数据库和表: 
 CREATE DATABASE IF NOT EXISTS userbehavior;
 USE userbehavior;

 CREATE TABLE clicks (
 `user` VARCHAR(20) NOT NULL,
 `url` VARCHAR(100) NOT NULL
 );

 select * from clicks;
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        stream.addSink(
                JdbcSink.sink(
                        "INSERT INTO clicks (user, url) VALUES (?, ?)",
                        (statement, r) -> {
                            statement.setString(1, r.user);
                            statement.setString(2, r.url);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/userbehavior?useSSL=false")
                                // 对于MySQL 5.x，用"com.mysql.jdbc.Driver"
                                // 对于MySQL 8.0+，用"com.mysql.cj.jdbc.Driver"
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )
        );

        env.execute();
    }
}
```

**（4）运行代码，用客户端连接 MySQL，查看是否成功写入数据**

```sql
mysql
>
select *
from clicks;
+------+---------------+
| user |      url      |
+------+---------------+
| Mary | ./home        |
| Alice| ./prod?id=300  |
| Bob  | ./prod?id=3   |
+------+---------------+
3 rows in set (0.00 sec)
```

**注意事项：**

- JdbcSink.sink() 方法需要传入四个参数：
    - SQL 语句：INSERT 语句，使用 ? 作为占位符
    - StatementBuilder：设置 PreparedStatement 的参数
    - JdbcExecutionOptions：批量写入配置（批量大小、批量间隔、重试次数）
    - JdbcConnectionOptions：数据库连接配置（URL、驱动、用户名、密码）
- MySQL 5.x 使用 "com.mysql.jdbc.Driver"，MySQL 8.0+ 使用 "com.mysql.cj.jdbc.Driver"
- 批量写入可以提高写入效率，减少数据库连接开销

### 3.8 自定义 Sink 输出

`src/main/java/com/action/sink/SinkCustomtoHBase.java`：使用 `RichSinkFunction` 自定义实现 HBase Sink

**特点说明：**

- **功能**：自定义实现将数据写入 HBase 的 Sink
- **实现方式**：继承 RichSinkFunction，实现生命周期方法
- **生命周期**：使用 open() 初始化连接，close() 关闭连接
- **使用场景**：Flink 没有提供连接器的外部系统、自定义存储逻辑等

**核心概念：**

1. **SinkFunction**：Flink 提供的通用 Sink 接口
2. **RichSinkFunction**：富函数版本的 SinkFunction，提供生命周期方法
3. **生命周期方法**：
    - open()：任务初始化时调用，可以用于初始化资源（如数据库连接）
    - invoke()：每条数据到来时调用，实现数据写入逻辑
    - close()：任务结束时调用，可以用于清理资源（如关闭连接）

#### 3.8.1 前置条件：HBase 环境搭建

**说明：** HBase 是一个分布式的、面向列的开源数据库，基于 Hadoop 和 HDFS。HBase 需要依赖 Zookeeper 来管理集群状态。本示例在
server01 上搭建单节点 HBase 进行测试（需要先安装 Hadoop 和 Zookeeper）。

##### 3.8.1.1 前置依赖：安装 Hadoop 和 Zookeeper

在生产环境下，HBase 通常依赖已经搭建好的 Hadoop + Zookeeper 集群。下面给出完整的 YARN 部署模式示例，基于以下集群规划：

> 集群规划：`server01（192.168.56.11）` 作为 NameNode/ResourceManager，`server02（192.168.56.12）`、`server03（192.168.56.13）`
> 作为 DataNode/NodeManager。

1.**Hadoop 集群环境搭建**

1）卸载现有 Java 环境（所有节点执行）

```bash
# ~/.bashrc 中删除之前的 JDK 配置
vim ~/.bashrc
# 删除 export JAVA_HOME=... 和 PATH 相关配置，随后重新加载
source ~/.bashrc
unset JAVA_HOME
export PATH=$(echo $PATH | sed 's|:/usr/lib/jvm[^:]*||g')

# 卸载 OpenJDK
rpm -qa | grep java
sudo yum remove -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
sudo rm -rf /usr/lib/jvm/java-1.8.0-openjdk*

# 验证卸载
java -version   # 预期 command not found
which java      # 无输出
```

2）安装 JDK 1.8.0_144（所有节点执行）

```bash
# Windows 上传安装包到各节点
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" \
    "E:\Hadoop-2-sgg\jdk-8u144-linux-x64.tar.gz" vagrant@192.168.56.11:/home/vagrant/
# server02/server03 同理

# 解压至 /opt/module
sudo tar -zxvf /home/vagrant/jdk-8u144-linux-x64.tar.gz -C /opt/module/

# 配置系统环境变量（/etc/profile，全局生效）
sudo vim /etc/profile
export JAVA_HOME=/opt/module/jdk1.8.0_144
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=.:$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH

# 重新加载并验证
source /etc/profile
java -version
which java
```

3）安装 Hadoop 2.7.2（server01 执行，完成后分发）

```bash
# 上传并解压
scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" \
    "E:\Hadoop-2-sgg\hadoop-2.7.2.tar.gz" vagrant@192.168.56.11:/home/vagrant/
sudo tar -zxvf hadoop-2.7.2.tar.gz -C /opt/module/

# 配置环境变量
sudo vim /etc/profile
export HADOOP_HOME=/opt/module/hadoop-2.7.2
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_CLASSPATH=`hadoop classpath`
source /etc/profile
hadoop version
```

4）配置 Hadoop 分布式集群（server01 执行）

```bash
# hadoop-env.sh（写死 JDK 路径）
sudo vim $HADOOP_HOME/etc/hadoop/hadoop-env.sh
export JAVA_HOME=/opt/module/jdk1.8.0_144

# core-site.xml
sudo vim $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://server01:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/module/hadoop-2.7.2/data/tmp</value>
  </property>
  <property>
    <name>hadoop.http.staticuser.user</name>
    <value>vagrant</value>
  </property>
</configuration>

# hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/module/hadoop-2.7.2/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/module/hadoop-2.7.2/data/datanode</value>
  </property>
  <property>
    <name>dfs.permissions</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>server01:50070</value>
  </property>
</configuration>

# yarn-site.xml
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>server01</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.log-aggregation.retain-seconds</name>
    <value>604800</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>server01:8088</value>
  </property>
</configuration>

# mapred-site.xml
sudo cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template \
        $HADOOP_HOME/etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapred.child.env</name>
    <value>LD_LIBRARY_PATH=$HADOOP_HOME/lib/native</value>
  </property>
</configuration>

# slaves
sudo vim $HADOOP_HOME/etc/hadoop/slaves
server02
server03
```

5）分发 Hadoop 并初始化（server01 执行）

```bash
# 准备目录并授权
ssh server02 "sudo mkdir -p /opt/module && sudo chown -R vagrant:vagrant /opt/module"
ssh server03 "sudo mkdir -p /opt/module && sudo chown -R vagrant:vagrant /opt/module"
sudo mkdir -p /opt/module/hadoop-2.7.2/data/{tmp,namenode,datanode}
sudo chown -R vagrant:vagrant /opt/module

# 分发
scp -r /opt/module/hadoop-2.7.2 server02:/opt/module/
scp -r /opt/module/hadoop-2.7.2 server03:/opt/module/
ssh server02 "sudo mkdir -p /opt/module/hadoop-2.7.2/data/{tmp,namenode,datanode}"
ssh server03 "sudo mkdir -p /opt/module/hadoop-2.7.2/data/{tmp,namenode,datanode}"

# 验证
ssh server02 "source /etc/profile && hadoop version"
ssh server03 "source /etc/profile && hadoop version"

# 首次格式化 HDFS（仅执行一次）
hdfs namenode -format
```

6）启动 Hadoop 集群（server01 执行）

```bash
# 1.启动 HDFS（分布式文件系统）
# 启动HDFS（包含NameNode、DataNode、SecondaryNameNode）
start-dfs.sh

# 验证HDFS进程（server01执行jps）
jps
# 预期进程：NameNode、SecondaryNameNode、Jps

# 检查HDFS状态
hdfs dfsadmin -report
# 检查Web UI（如果已启用）
# http://server01:50070

# 验证从节点进程（server02、server03执行jps）
ssh server02 jps  # 预期进程：DataNode、Jps
ssh server03 jps  # 预期进程：DataNode、Jps
# 或者
ssh server02 "source /etc/profile && jps"
ssh server03 "source /etc/profile && jps"


# 2.启动 YARN（资源管理器）
# 启动YARN（包含ResourceManager、NodeManager）
start-yarn.sh
# 验证YARN进程（server01执行jps）
jps
# 预期进程：NameNode、SecondaryNameNode、ResourceManager、Jps
# 验证从节点进程（server02、server03执行jps）
ssh server02 jps  # 预期进程：DataNode、NodeManager、Jps
ssh server03 jps  # 预期进程：DataNode、NodeManager、Jps
# 或者
ssh server02 "source /etc/profile && jps"
ssh server03 "source /etc/profile && jps"


# 关闭命令
# 停止整个集群
stop-yarn.sh && stop-dfs.sh
# 再重新启动
start-dfs.sh && start-yarn.sh
```

7）Web UI 验证

```text
HDFS WebUI： http://192.168.56.11:50070  （DataNodes 应显示 2 个从节点）
YARN WebUI： http://192.168.56.11:8088  （Nodes 应显示 2 个 NodeManager）
```

8）Zookeeper 安装（集群模式，3 节点）

**集群规划：**

- server01（192.168.56.11）：ZooKeeper 节点（可能成为 Leader）
- server02（192.168.56.12）：ZooKeeper 节点（Follower）
- server03（192.168.56.13）：ZooKeeper 节点（Follower）

**版本选择：** ZooKeeper 3.4.14（稳定兼容 HBase 1.4.13 + Hadoop 2.7.2）

```bash
# ============================================
# 步骤 1：下载并上传 ZooKeeper 安装包
# ============================================
# 方式 A（推荐）：仅在 server01 执行，然后通过步骤 3.5 分发到其他节点
# 方式 B：在每个节点都执行此步骤（如果选择各节点独立安装）

# 方式 1：直接 wget 下载（推荐）
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz -P /home/vagrant/

# 方式 2：本地下载后 scp 上传（网络不佳时，Windows 终端执行）
# scp -i "D:\application\VM-Vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "E:\Hadoop-2-sgg\zookeeper-3.4.14.tar.gz" vagrant@192.168.56.11:/home/vagrant/


# ============================================
# 步骤 2：解压并配置目录
# ============================================
# 方式 A（推荐）：仅在 server01 执行，然后通过步骤 3.5 分发到其他节点
# 方式 B：在每个节点（server01、server02、server03）都执行此步骤

# 2.1 解压到统一目录（与 Hadoop/HBase 路径一致）
sudo tar -zxvf /home/vagrant/zookeeper-3.4.14.tar.gz -C /opt/module/
# 修改目录所有者（保持与 Hadoop/HBase 权限一致，避免权限报错）
sudo chown -R vagrant:vagrant /opt/module/zookeeper-3.4.14/

# 2.2 创建必要目录（数据目录 + 日志目录）
# 数据目录（存储 ZooKeeper 集群元数据）
sudo mkdir -p /opt/module/zookeeper-3.4.14/data
# 日志目录（存储运行日志，方便排查问题）
sudo mkdir -p /opt/module/zookeeper-3.4.14/logs
# 设置目录权限（确保 vagrant 用户可以写入）
sudo chown -R vagrant:vagrant /opt/module/zookeeper-3.4.14/data
sudo chown -R vagrant:vagrant /opt/module/zookeeper-3.4.14/logs


# ============================================
# 步骤 3：配置 ZooKeeper 环境变量（所有机器执行）
# ============================================
# 注意：无论使用方式 A（分发）还是方式 B（各节点独立安装），所有节点都需要执行此步骤

# 编辑系统级配置文件 /etc/profile，确保所有用户可访问
sudo vim /etc/profile
# 新增以下配置：
# export ZOOKEEPER_HOME=/opt/module/zookeeper-3.4.14
# export PATH=$PATH:$ZOOKEEPER_HOME/bin:$ZOOKEEPER_HOME/sbin

# 加载配置生效
source /etc/profile
# 验证配置（输出 /opt/module/zookeeper-3.4.14 则正确）
echo $ZOOKEEPER_HOME


# ============================================
# 步骤 3.5：分发 ZooKeeper 到其他节点（仅 server01 执行，可选）
# ============================================
# 如果选择在 server01 上安装后分发，执行以下命令（推荐，效率高）
# 如果选择在每个节点都执行步骤 1-2，可跳过此步骤

# 分发整个 ZooKeeper 目录到 server02 和 server03
scp -r /opt/module/zookeeper-3.4.14 server02:/opt/module/
scp -r /opt/module/zookeeper-3.4.14 server03:/opt/module/

# 验证分发结果（检查目录是否存在）
# 方式 1：使用 ssh 远程执行命令（推荐，一行一个命令）
ssh server02 'ls -ld /opt/module/zookeeper-3.4.14'
ssh server03 'ls -ld /opt/module/zookeeper-3.4.14'

# 方式 2：如果方式 1 遇到问题，可以手动登录到 server02 和 server03 验证
# ssh server02
# ls -ld /opt/module/zookeeper-3.4.14
# exit
# ssh server03
# ls -ld /opt/module/zookeeper-3.4.14
# exit

# 预期输出示例：
# drwxr-xr-x. 16 vagrant vagrant 4096 Nov 17 17:37 /opt/module/zookeeper-3.4.14

# 注意：分发后，server02 和 server03 上也需要执行步骤 3（配置环境变量）


# ============================================
# 步骤 4：核心配置（仅 server01 执行，配置后分发）
# ============================================

# 4.1 复制模板并编辑 zoo.cfg
# 进入配置目录
cd $ZOOKEEPER_HOME/conf/
# 复制模板文件（默认无 zoo.cfg，需从 zoo_sample.cfg 复制）
cp zoo_sample.cfg zoo.cfg
# 编辑配置文件
sudo vim zoo.cfg

# 4.2 修改 zoo.cfg 关键配置（替换原有内容为以下）
# tickTime=2000
# initLimit=10
# syncLimit=5
# dataDir=/opt/module/zookeeper-3.4.14/data
# dataLogDir=/opt/module/zookeeper-3.4.14/logs
# clientPort=2181
# maxClientCnxns=0
# server.1=server01:2888:3888
# server.2=server02:2888:3888
# server.3=server03:2888:3888

# 4.3 创建 myid 文件（所有机器执行，关键！）
# 每个 ZooKeeper 节点需通过 dataDir 下的 myid 文件标识自身 ID，与 zoo.cfg 中 server.X 对应
# 注意：如果目录权限已正确设置（vagrant 用户拥有），可直接使用 echo；否则使用 sudo tee 方式

# server01 执行：
# 方式 1：如果目录所有者是 vagrant（推荐，已在步骤 2.1 中设置）
echo "1" > /opt/module/zookeeper-3.4.14/data/myid
# 方式 2：如果遇到权限问题，使用以下命令
# echo "1" | sudo tee /opt/module/zookeeper-3.4.14/data/myid > /dev/null
# 验证（输出1则正确）
cat /opt/module/zookeeper-3.4.14/data/myid

# server02 执行：
# echo "2" > /opt/module/zookeeper-3.4.14/data/myid
# 或者：echo "2" | sudo tee /opt/module/zookeeper-3.4.14/data/myid > /dev/null
# cat /opt/module/zookeeper-3.4.14/data/myid

# server03 执行：
# echo "3" > /opt/module/zookeeper-3.4.14/data/myid
# 或者：echo "3" | sudo tee /opt/module/zookeeper-3.4.14/data/myid > /dev/null
# cat /opt/module/zookeeper-3.4.14/data/myid


# ============================================
# 步骤 5：分发 ZooKeeper 配置到从节点（仅 server01 执行）
# ============================================

# 分发配置文件（仅同步 zoo.cfg，myid 已单独创建）
scp $ZOOKEEPER_HOME/conf/zoo.cfg server02:$ZOOKEEPER_HOME/conf/
scp $ZOOKEEPER_HOME/conf/zoo.cfg server03:$ZOOKEEPER_HOME/conf/

# 验证分发结果（从节点执行，查看配置是否一致）
ssh server02 "cat $ZOOKEEPER_HOME/conf/zoo.cfg | grep server"
ssh server03 "cat $ZOOKEEPER_HOME/conf/zoo.cfg | grep server"
# 预期输出：server.1=server01:2888:3888 等3行配置


# ============================================
# 步骤 6：启动 ZooKeeper 集群（所有节点执行，顺序无关）
# ============================================

# 6.1 启动命令（每个节点单独执行）
# 启动 ZooKeeper（后台运行）
zkServer.sh start

# 6.2 验证集群状态（所有节点执行）
# 查看节点角色（Leader 或 Follower）
zkServer.sh status
# 预期结果：1 个节点为 Leader，2 个节点为 Follower（奇数节点集群正常状态）
# 示例输出（server01 为 Leader）：
# ZooKeeper JMX enabled by default
# Using config: /opt/module/zookeeper-3.4.14/bin/../conf/zoo.cfg
# Mode: Leader
# 示例输出（server02 为 Follower）：
# ZooKeeper JMX enabled by default
# Using config: /opt/module/zookeeper-3.4.14/bin/../conf/zoo.cfg
# Mode: Follower

# 6.3 验证客户端连接（任意节点执行）
# 连接本地 ZooKeeper 服务
zkCli.sh -server localhost:2181
# 连接成功后，执行以下命令测试
# ls /  # 查看根节点（默认有 zookeeper 节点）
# exit  # 退出客户端
```

（至此，Hadoop + YARN + Zookeeper 集群准备完毕，满足 HBase 安装前提。）

##### 3.8.1.2 HBase 安装配置

**1. 下载并安装 HBase**

```bash
# 在 server01 上执行

# 下载 HBase 安装包
# 主节点 server01 执行下载，版本选择 1.4.13（兼容 Hadoop 2.7.2）：
cd ~
# Apache Archive 调整了目录层级，访问 dist/hbase/1.4.13 才能获取文件
sudo wget https://archive.apache.org/dist/hbase/1.4.13/hbase-1.4.13-bin.tar.gz

# 解压并配置目录权限
# 所有机器统一解压到/opt/module/目录，与 Hadoop 路径保持一致：
# 解压安装包
sudo tar -zxvf /home/vagrant/hbase-1.4.13-bin.tar.gz -C /opt/module/
# 修改目录所有者（与Hadoop权限一致，避免权限问题）
sudo chown -R vagrant:vagrant /opt/module/hbase-1.4.13/
```

**2. 配置 HBase 环境变量（所有机器）**

```bash
# 编辑系统级配置文件/etc/profile，添加 HBase 环境变量
sudo vim /etc/profile
# 新增以下配置
export HBASE_HOME=/opt/module/hbase-1.4.13
export PATH=$PATH:$HBASE_HOME/bin:$HBASE_HOME/sbin

# 查看配置
cat /etc/profile

# 加载配置生效
source /etc/profile
# 验证环境变量（有输出则正确）
echo $HBASE_HOME
```

**3.核心配置（仅 server01 执行，配置后分发）**

```bash
# HBase 配置依赖$HBASE_HOME/conf/目录下 3 个核心文件，需按集群规划修改
# 1. 配置 hbase-env.sh（指定 JDK 和 Hadoop 路径）
sudo vim $HBASE_HOME/conf/hbase-env.sh
# 添加以下配置
# 注释默认的JAVA_HOME，添加实际JDK路径（与Hadoop一致）
export JAVA_HOME=/opt/module/jdk1.8.0_144
# 指定Hadoop配置文件路径（让HBase识别Hadoop集群）
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
# 禁用HBase自带的ZooKeeper（使用独立ZooKeeper，后续配置）
export HBASE_MANAGES_ZK=false
# 解决JDK 8兼容性问题（禁用过时API检查）
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true




# 2. 配置 hbase-site.xml（HBase 核心参数）
sudo vim $HBASE_HOME/conf/hbase-site.xml
# 在<configuration>标签内添加以下内容
<configuration>
    <!-- 指定HBase在HDFS上的存储路径（依赖Hadoop集群） -->
    <property>
        <name>hbase.rootdir</name>
        <value>hdfs://server01:9000/hbase</value>
    </property>
    <!-- 启用分布式模式（必须配置为true，否则是单机模式） -->
    <property>
        <name>hbase.cluster.distributed</name>
        <value>true</value>
    </property>
    <!-- 指定ZooKeeper集群地址（主节点+从节点，默认端口2181） -->
    <property>
        <name>hbase.zookeeper.quorum</name>
        <value>server01,server02,server03</value>
    </property>
    <!-- ZooKeeper数据存储目录（需手动创建） -->
    <property>
        <name>hbase.zookeeper.property.dataDir</name>
        <value>/opt/module/hbase-1.4.13/data/zookeeper</value>
    </property>
    <!-- 关闭权限检查（开发环境方便测试） -->
    <property>
        <name>hbase.security.authorization</name>
        <value>false</value>
    </property>
    <!-- HBase Master WebUI端口（默认16010） -->
    <property>
        <name>hbase.master.info.port</name>
        <value>16010</value>
    </property>
</configuration>



# 3. 配置 regionservers（指定从节点 RegionServer）
sudo vim $HBASE_HOME/conf/regionservers
# 删除默认的localhost，添加从节点主机名（与Hadoop slaves一致）
server02
server03


# 4. 分发 HBase 配置到从节点（server01 执行）
# 将主节点配置好的 HBase 分发到 server02 和 server03，避免重复配置
# 分发HBase目录
scp -r /opt/module/hbase-1.4.13 server02:/opt/module/
scp -r /opt/module/hbase-1.4.13 server03:/opt/module/
# 验证分发结果（从节点执行，有版本输出则正确）
ssh server02 "source /etc/profile; hbase version"
ssh server03 "source /etc/profile; hbase version"


# 5.创建必要目录（所有机器执行）
# 创建HBase数据目录
mkdir -p /opt/module/hbase-1.4.13/data
# 创建ZooKeeper数据目录
mkdir -p /opt/module/hbase-1.4.13/data/zookeeper
# 验证目录权限（确保是vagrant用户所有）
ls -ld /opt/module/hbase-1.4.13/data/
```

**3. 配置环境变量（可选）**

```bash
# 在 /etc/profile 中配置（推荐，所有用户生效）
sudo vim /etc/profile

# 添加以下内容：
export HBASE_HOME=/opt/module/hbase-1.4.13
export PATH=$PATH:$HBASE_HOME/bin

# 查看配置
cat /etc/profile

# 应用配置
source /etc/profile

# 验证环境变量
echo $HBASE_HOME
```

**4. 启动 HBase 集群（仅 server01 执行）**

```bash
# 启动前需确保Hadoop 集群已正常运行（先启动 HDFS 和 YARN）
# 启动顺序（关键）
# 1. 先确认Hadoop集群已启动（若未启动，执行以下命令）
start-dfs.sh && start-yarn.sh
# 2. 启动HBase集群（包含Master、RegionServer、ZooKeeper）
start-hbase.sh
```

**5. 验证 HBase 是否启动成功**

```bash
# 检查进程
jps
# 应该能看到 HMaster 和 HRegionServer 进程

# 验证进程（核心检查）
# 检查进程
jps
# 主节点 server01 执行jps，预期进程
# NameNode、ResourceManager（Hadoop 进程）
# HMaster、QuorumPeerMain（HBase+ZooKeeper 进程）
# 从节点 server02/server03 执行jps，预期进程
# DataNode、NodeManager（Hadoop 进程）
# HRegionServer、QuorumPeerMain（HBase+ZooKeeper 进程）

# WebUI 验证
# 访问 HBase Master WebUI，查看集群状态
http://192.168.56.11:16010


# 或者使用 ps 命令
ps -ef | grep hbase

# 测试 HBase Shell
bin/hbase shell

# 在 HBase Shell 中执行：
hbase(main):001:0> status
# 应该显示 HBase 集群状态

hbase(main):002:0> version
# 应该显示 HBase 版本信息

hbase(main):003:0> exit

```

**6. 测试 HBase 基本功能**

```bash
# 进入 HBase Shell
cd /opt/module/hbase-1.4.13
bin/hbase shell

# 创建测试表
hbase(main):001:0> create 'test_table', 'info'
# 输出：0 row(s) in X.XXXX seconds

# 查看表列表
hbase(main):002:0> list
# 应该能看到 test_table

# 插入数据
hbase(main):003:0> put 'test_table', 'row1', 'info:name', 'test_value'
# 输出：0 row(s) in X.XXXX seconds

# 查询数据
hbase(main):004:0> get 'test_table', 'row1'
# 应该能看到插入的数据

# 扫描表
hbase(main):005:0> scan 'test_table'
# 应该能看到所有数据

# 删除表（测试完成后）
hbase(main):006:0> disable 'test_table'
hbase(main):007:0> drop 'test_table'

# 退出 HBase Shell
hbase(main):008:0> exit
```

**7. 关闭 HBase**

```bash
# 进入 HBase 安装目录
cd /opt/module/hbase-1.4.13

# 停止 HBase
bin/stop-hbase.sh

# 或者使用 kill 命令
ps -ef | grep hbase
kill -9 <进程ID>
```

**8. 重启 HBase 和验证**

如果之前已经安装并启动过 HBase，然后停止了服务或重启了 Linux 系统，可以按以下步骤重启 HBase 并进行验证：

**步骤1：检查 HBase 进程状态**

```bash
# 检查 HBase 是否正在运行
jps | grep -E "HMaster|HRegionServer"
# 或
ps -ef | grep hbase

# 如果没有看到 HBase 进程，说明 HBase 已停止，需要重启
```

**步骤2：确保 Hadoop 和 Zookeeper 已启动**

```bash
# 检查 Hadoop 是否启动
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager"

# 检查 Zookeeper 是否启动
jps | grep QuorumPeerMain
# 或
zkServer.sh status

# 如果未启动，需要先启动
# start-dfs.sh
# start-yarn.sh
# zkServer.sh start
```

**步骤3：启动 HBase**

```bash
# 进入 HBase 安装目录
cd /opt/module/hbase-1.4.13

# 启动 HBase
bin/start-hbase.sh
```

**步骤4：验证 HBase 是否启动成功**

```bash
# 等待几秒钟让 HBase 完全启动
sleep 5

# 检查进程
jps
# 应该能看到 HMaster 和 HRegionServer 进程

# 测试 HBase Shell
cd /opt/module/hbase-1.4.13
bin/hbase shell

# 在 HBase Shell 中执行：
hbase(main):001:0> status
# 应该显示 HBase 集群状态

hbase(main):002:0> version
# 应该显示 HBase 版本信息

hbase(main):003:0> exit
```

**步骤5：测试 HBase 基本功能**

```bash
# 进入 HBase Shell
cd /opt/module/hbase-1.4.13
bin/hbase shell

# 创建测试表
hbase(main):001:0> create 'test_table', 'info'

# 插入测试数据
hbase(main):002:0> put 'test_table', 'row1', 'info:name', 'test_value'

# 查询测试数据
hbase(main):003:0> get 'test_table', 'row1'

# 扫描表
hbase(main):004:0> scan 'test_table'

# 删除测试表
hbase(main):005:0> disable 'test_table'
hbase(main):006:0> drop 'test_table'

# 退出 HBase Shell
hbase(main):007:0> exit
```

**常见问题：**

**Q1: HBase 启动失败，提示 "Could not find or load main class"**

**原因：** Java 环境变量未配置或 HBase 配置文件错误

**解决方案：**

```bash
# 检查 Java 环境
java -version
echo $JAVA_HOME

# 编辑 hbase-env.sh，确保 JAVA_HOME 配置正确
cd /opt/module/hbase-1.4.13
sudo vim conf/hbase-env.sh
# 确保 export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk 已设置
```

**Q2: HBase 启动失败，提示 "Connection refused" 或 "Zookeeper connection failed"**

**原因：** Zookeeper 未启动或配置错误

**解决方案：**

```bash
# 检查 Zookeeper 是否启动
jps | grep QuorumPeerMain
# 或
zkServer.sh status

# 如果未启动，启动 Zookeeper
zkServer.sh start

# 检查 hbase-site.xml 中的 Zookeeper 配置
cat /opt/module/hbase-1.4.13/conf/hbase-site.xml | grep zookeeper.quorum
# 应该显示正确的 Zookeeper 地址，如 server01:2181
```

**Q3: HBase 启动失败，提示 "HDFS not found" 或 "NameNode not found"**

**原因：** Hadoop 未启动或 HDFS 配置错误

**解决方案：**

```bash
# 检查 Hadoop 是否启动
jps | grep -E "NameNode|DataNode"

# 如果未启动，启动 Hadoop
start-dfs.sh

# 检查 hbase-site.xml 中的 HDFS 配置
cat /opt/module/hbase-1.4.13/conf/hbase-site.xml | grep hbase.rootdir
# 应该显示正确的 HDFS 地址，如 hdfs://server01:9000/hbase
```

**Q4: HBase Shell 无法连接**

**原因：** HBase 未启动或端口被占用

**解决方案：**

```bash
# 检查 HBase 进程
jps | grep -E "HMaster|HRegionServer"

# 检查 HBase 日志
tail -f /opt/module/hbase-1.4.13/logs/hbase-*-master-*.log

# 检查端口是否被占用
netstat -tuln | grep 16000  # HMaster 端口
netstat -tuln | grep 16020  # HRegionServer 端口
```

**Q5: 使用 systemctl 启动 HBase 失败，提示 "Unit not found"**

**原因：** 从压缩包安装的 HBase 不会自动创建 systemd 服务文件

**解决方案：**

```bash
# 从压缩包安装的 HBase 不能使用 systemctl 启动
# 必须使用 start-hbase.sh 脚本启动

# 进入 HBase 安装目录
cd /opt/module/hbase-1.4.13

# 启动 HBase
bin/start-hbase.sh

# 验证是否启动成功
jps | grep -E "HMaster|HRegionServer"
```

**版本说明：**

- **HBase 2.2.7**：与 Flink 1.13 兼容，支持 `hbase-client` 2.2.7
- **Zookeeper 要求**：HBase 需要 Zookeeper 3.4.x 或更高版本
- **Hadoop 要求**：HBase 2.2.7 需要 Hadoop 2.7+ 或 3.x
- **本示例使用 HBase 2.2.7**：单机模式，使用外部 Zookeeper

#### 3.8.2 代码实现与测试

**（1）添加 HBase 客户端依赖**

在 `pom.xml` 中添加 HBase 客户端依赖：

```xml
<!-- HBase 客户端 (可选，用于自定义 Sink) -->
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-client</artifactId>
    <version>${hbase.version}</version>
</dependency>
```

**依赖说明：**

- **hbase-client**：HBase 客户端库，用于连接和操作 HBase
- **版本要求**：需要与 HBase 服务器版本匹配（本项目使用 HBase 2.2.7）
- **功能特性**：
    - 支持 HBase 连接管理
    - 支持表操作（创建、删除、查询等）
    - 支持数据操作（Put、Get、Scan、Delete 等）
    - 支持批量操作

**（2）编写输出到 HBase 的示例代码**

```java
package com.action.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 自定义 Sink 输出示例（HBase）
 * 使用 RichSinkFunction 自定义实现 HBase Sink
 * <p>
 * 特点说明：
 * - 功能：自定义实现将数据写入 HBase 的 Sink
 * - 实现方式：继承 RichSinkFunction，实现生命周期方法
 * - 生命周期：使用 open() 初始化连接，close() 关闭连接
 * - 使用场景：Flink 没有提供连接器的外部系统、自定义存储逻辑等
 */
public class SinkCustomtoHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello", "world", "flink", "hbase")
                .addSink(
                        new RichSinkFunction<String>() {
                            public org.apache.hadoop.conf.Configuration configuration; // 管理Hbase的配置信息,这里因为Configuration的重名问题，将类以完整路径导入
                            public Connection connection; // 管理Hbase连接

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                configuration = HBaseConfiguration.create();
                                configuration.set("hbase.zookeeper.quorum", "server01:2181");
                                connection = ConnectionFactory.createConnection(configuration);
                            }

                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                Table table = connection.getTable(TableName.valueOf("test")); // 表名为test
                                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8)); // 指定rowkey

                                put.addColumn("info".getBytes(StandardCharsets.UTF_8) // 指定列名
                                        , value.getBytes(StandardCharsets.UTF_8) // 写入的数据
                                        , "1".getBytes(StandardCharsets.UTF_8)); // 写入的数据
                                table.put(put); // 执行put操作
                                table.close(); // 将表关闭
                            }

                            @Override
                            public void close() throws Exception {
                                super.close();
                                connection.close(); // 关闭连接
                            }
                        }
                );

        env.execute();
    }
}
```

**（3）运行代码，在 HBase 中查看是否收到数据**

**完整测试流程：**

**步骤1：在 server01 上启动 HBase**

```bash
# 确保 Hadoop 和 Zookeeper 已启动
jps | grep -E "NameNode|QuorumPeerMain"

# 如果未启动，先启动
# start-dfs.sh
# zkServer.sh start

# 进入 HBase 安装目录
cd /opt/module/hbase-1.4.13

# 启动 HBase
bin/start-hbase.sh

# 等待几秒钟让 HBase 完全启动
sleep 5

# 验证 HBase 是否启动成功
jps | grep -E "HMaster|HRegionServer"
```

**步骤2：创建 HBase 表（在 server01 上）**

```bash
# 进入 HBase Shell
cd /opt/module/hbase-1.4.13
bin/hbase shell

# 创建 test 表（如果不存在）
hbase(main):001:0> create 'test', 'info'
# 如果表已存在，会提示错误，可以忽略

# 查看表列表
hbase(main):002:0> list
# 应该能看到 test 表

# 退出 HBase Shell
hbase(main):003:0> exit
```

**步骤3：启动 Flink 程序（在本地或客户端）**

```bash
cd flink-04-Sink
mvn compile exec:java -Dexec.mainClass="com.action.sink.SinkCustomtoHBase"
```

**步骤4：在 HBase 中查看数据（在 server01 上）**

```bash
# 进入 HBase Shell
cd /opt/module/hbase-1.4.13
bin/hbase shell

# 扫描 test 表的所有数据
hbase(main):001:0> scan 'test'
# 应该能看到 Flink 程序写入的数据

# 查询特定 rowkey 的数据
hbase(main):002:0> get 'test', 'rowkey'
# 应该能看到最后写入的数据（因为使用相同的 rowkey，后面的数据会覆盖前面的数据）

# 退出 HBase Shell
hbase(main):003:0> exit
```

**步骤5：观察输出**

HBase 中会保存 Flink 程序写入的数据：

- 表名：`test`
- RowKey：`rowkey`（所有数据使用相同的 rowkey）
- 列族：`info`
- 列名：动态生成（根据数据内容）
- 值：`hello` 和 `world`（由于使用相同的 rowkey，最后写入的 `world` 会覆盖 `hello`）

**注意：** 由于代码中使用相同的 rowkey，后面的数据会覆盖前面的数据，所以最终只能看到最后一条数据。

**注意事项：**

- 在实现 SinkFunction 的时候，需要重写的关键方法 invoke()，在这个方法中实现将流里的数据发送出去的逻辑
- 使用 RichSinkFunction 的富函数版本，可以利用生命周期方法管理资源
- 创建 HBase 的连接以及关闭 HBase 的连接需要分别放在 open() 方法和 close() 方法中
- 注意 Configuration 类的重名问题，需要使用完整路径 `org.apache.hadoop.conf.Configuration`
- **Zookeeper 配置**：代码中使用 `hadoop102:2181`，如果 HBase 运行在 `server01` 上，可以改为 `server01:2181`，需要确保与
  `hbase-site.xml` 中的配置一致
- **表创建**：运行 Flink 程序前，需要先在 HBase 中创建 `test` 表，否则会报错
- **RowKey 设计**：实际应用中应该使用不同的 rowkey，避免数据覆盖

## 4. Sink 算子类型对比

### 4.1 官方 Sink 连接器

| Sink 类型           | 连接器                | 精确一次语义  | 使用场景        |
|-------------------|--------------------|---------|-------------|
| **文件系统**          | StreamingFileSink  | ✅ 支持    | 日志收集、数据归档   |
| **Kafka**         | FlinkKafkaProducer | ✅ 支持    | 数据管道、实时数据流  |
| **Elasticsearch** | ElasticsearchSink  | ✅ 支持    | 日志分析、全文搜索   |
| **JDBC**          | JdbcSink           | ⚠️ 有限支持 | 结果数据存储、报表数据 |
| **Redis**         | RedisSink (Bahir)  | ❌ 不支持   | 实时缓存、数据存储   |

### 4.2 Sink 实现方式对比

| 实现方式         | 接口/类             | 生命周期方法            | 使用场景             |
|--------------|------------------|-------------------|------------------|
| **普通 Sink**  | SinkFunction     | 无                 | 简单的数据输出          |
| **富函数 Sink** | RichSinkFunction | open/close        | 需要管理资源（连接、缓存等）   |
| **自定义 Sink** | RichSinkFunction | open/invoke/close | Flink 没有提供连接器的系统 |

### 4.3 状态一致性保证

| Sink 类型                | 状态一致性 | 实现机制                    |
|------------------------|-------|-------------------------|
| **StreamingFileSink**  | 精确一次  | Checkpoint 机制           |
| **FlinkKafkaProducer** | 精确一次  | 两阶段提交（Two-Phase Commit） |
| **ElasticsearchSink**  | 精确一次  | Checkpoint 机制           |
| **JdbcSink**           | 至少一次  | 批量写入 + 重试机制             |
| **RedisSink**          | 至少一次  | 无事务保证                   |
| **自定义 Sink**           | 取决于实现 | 需要自行实现                  |

## 5. 核心概念详解

### 5.1 SinkFunction 接口

**接口定义：**

```java
public interface SinkFunction<T> extends Function, Serializable {
    void invoke(T value, Context context) throws Exception;
}
```

**核心方法：**

- `invoke(T value, Context context)`：每条数据记录到来时都会调用，用来将指定的值写入到外部系统中

**实现要点：**

1. 每条数据都会调用一次 invoke 方法
2. 需要处理异常情况
3. 对于需要连接的外部系统，建议使用 RichSinkFunction

### 5.2 RichSinkFunction 抽象类

**类定义：**

```java
public abstract class RichSinkFunction<T> extends AbstractRichFunction implements SinkFunction<T> {
    public abstract void open(Configuration parameters) throws Exception;

    public abstract void invoke(T value, Context context) throws Exception;

    public abstract void close() throws Exception;
}
```

**核心方法：**

- `open(Configuration parameters)`：任务初始化时调用，可以用于初始化资源
- `invoke(T value, Context context)`：每条数据到来时调用，实现数据写入逻辑
- `close()`：任务结束时调用，可以用于清理资源

**实现要点：**

1. 可以在 open() 中初始化外部连接（数据库、HBase 等）
2. 可以在 close() 中关闭连接，释放资源
3. 可以访问运行时上下文（RuntimeContext）

### 5.3 精确一次语义（Exactly Once）

**概念说明：**

精确一次语义是指每条数据只会被处理一次，不会丢失也不会重复。这对于保证数据处理的正确性非常重要。

**实现机制：**

1. **Checkpoint 机制**：Flink 定期创建检查点，保存状态快照
2. **两阶段提交**：对于支持事务的外部系统，使用两阶段提交协议
3. **幂等性写入**：对于不支持事务的系统，通过幂等性写入保证精确一次

**支持的 Sink：**

- StreamingFileSink：通过 Checkpoint 机制保证
- FlinkKafkaProducer：通过两阶段提交保证
- ElasticsearchSink：通过 Checkpoint 机制保证

## 6. 常见问题

### 6.1 如何选择合适的 Sink？

**选择建议：**

- **文件系统**：适合日志收集、数据归档、批量数据输出
- **Kafka**：适合数据管道、实时数据流、事件流处理
- **Redis**：适合实时缓存、会话管理、计数器
- **Elasticsearch**：适合日志分析、全文搜索、实时数据索引
- **MySQL**：适合结果数据存储、报表数据、业务数据存储
- **自定义 Sink**：适合 Flink 没有提供连接器的系统

### 6.2 如何保证数据不丢失？

**解决方案：**

1. **启用 Checkpoint**：在 Flink 配置中启用 Checkpoint
2. **使用支持精确一次的 Sink**：如 StreamingFileSink、FlinkKafkaProducer
3. **配置重试机制**：对于不支持精确一次的 Sink，配置重试次数
4. **幂等性写入**：确保写入操作是幂等的，重复写入不会产生副作用

### 6.3 自定义 Sink 的最佳实践

**实践建议：**

1. **使用 RichSinkFunction**：需要管理资源时使用富函数版本
2. **在 open() 中初始化连接**：避免每条数据都创建连接
3. **在 close() 中关闭连接**：确保资源正确释放
4. **处理异常情况**：在 invoke() 中正确处理异常
5. **考虑批量写入**：对于支持批量写入的系统，使用批量写入提高效率

### 6.4 性能优化建议

**优化建议：**

1. **批量写入**：对于支持批量写入的 Sink（如 JdbcSink），配置批量大小
2. **异步写入**：对于支持异步写入的 Sink，使用异步模式
3. **连接池**：对于需要连接的外部系统，使用连接池管理连接
4. **并行度调整**：根据外部系统的处理能力调整 Sink 的并行度

## 7. 参考资料

- [Flink 官方文档 - 数据输出（Sink）](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sinks/)
- [Flink 官方文档 - 文件 Sink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/filesystem/)
- [Flink 官方文档 - Kafka 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/kafka/)
- [Flink 官方文档 - Elasticsearch 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/elasticsearch/)
- [Flink 官方文档 - JDBC 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/jdbc/)
- [Flink 官方文档 - 自定义 Sink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sinks/#自定义-sink)

