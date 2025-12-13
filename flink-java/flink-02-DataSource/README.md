<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 数据源读取演示](#flink-%E6%95%B0%E6%8D%AE%E6%BA%90%E8%AF%BB%E5%8F%96%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event.java - 事件数据模型](#321-eventjava---%E4%BA%8B%E4%BB%B6%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.2 ClickSource.java - 自定义数据源](#322-clicksourcejava---%E8%87%AA%E5%AE%9A%E4%B9%89%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.3 从文件读取数据源](#33-%E4%BB%8E%E6%96%87%E4%BB%B6%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.4 从集合读取数据源](#34-%E4%BB%8E%E9%9B%86%E5%90%88%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.5 从元素读取数据源](#35-%E4%BB%8E%E5%85%83%E7%B4%A0%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.6 从 Socket 读取数据源](#36-%E4%BB%8E-socket-%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.7 从 CSV 文件读取并解析数据源](#37-%E4%BB%8E-csv-%E6%96%87%E4%BB%B6%E8%AF%BB%E5%8F%96%E5%B9%B6%E8%A7%A3%E6%9E%90%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.8 从 HTTP API 读取数据源](#38-%E4%BB%8E-http-api-%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.9 从 JDBC 数据库读取数据源](#39-%E4%BB%8E-jdbc-%E6%95%B0%E6%8D%AE%E5%BA%93%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.10 自定义数据源](#310-%E8%87%AA%E5%AE%9A%E4%B9%89%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.11 自定义并行数据源](#311-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%B9%B6%E8%A1%8C%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.12 从 Kafka 读取数据源](#312-%E4%BB%8E-kafka-%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E6%BA%90)
      - [3.12.1 前置条件：Kafka 环境搭建（KRaft 模式）](#3121-%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6kafka-%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BAkraft-%E6%A8%A1%E5%BC%8F)
      - [3.12.2 FlinkKafkaConsumer 代码实现](#3122-flinkkafkaconsumer-%E4%BB%A3%E7%A0%81%E5%AE%9E%E7%8E%B0)
    - [3.13 综合数据源示例](#313-%E7%BB%BC%E5%90%88%E6%95%B0%E6%8D%AE%E6%BA%90%E7%A4%BA%E4%BE%8B)
  - [4. 数据源类型对比](#4-%E6%95%B0%E6%8D%AE%E6%BA%90%E7%B1%BB%E5%9E%8B%E5%AF%B9%E6%AF%94)
    - [4.1 有界数据源 vs 无界数据源](#41-%E6%9C%89%E7%95%8C%E6%95%B0%E6%8D%AE%E6%BA%90-vs-%E6%97%A0%E7%95%8C%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [4.2 数据源 API 对比](#42-%E6%95%B0%E6%8D%AE%E6%BA%90-api-%E5%AF%B9%E6%AF%94)
    - [4.3 自定义数据源接口对比](#43-%E8%87%AA%E5%AE%9A%E4%B9%89%E6%95%B0%E6%8D%AE%E6%BA%90%E6%8E%A5%E5%8F%A3%E5%AF%B9%E6%AF%94)
  - [5. 测试数据](#5-%E6%B5%8B%E8%AF%95%E6%95%B0%E6%8D%AE)
    - [5.1 words.txt](#51-wordstxt)
    - [5.2 clicks.csv](#52-clickscsv)
  - [6. 核心概念详解](#6-%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5%E8%AF%A6%E8%A7%A3)
    - [6.1 SourceFunction 接口](#61-sourcefunction-%E6%8E%A5%E5%8F%A3)
    - [6.2 ParallelSourceFunction 接口](#62-parallelsourcefunction-%E6%8E%A5%E5%8F%A3)
    - [6.3 数据源生命周期](#63-%E6%95%B0%E6%8D%AE%E6%BA%90%E7%94%9F%E5%91%BD%E5%91%A8%E6%9C%9F)
    - [6.4 文件路径问题](#64-%E6%96%87%E4%BB%B6%E8%B7%AF%E5%BE%84%E9%97%AE%E9%A2%98)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 数据源读取演示

## 1. 项目作用

本项目演示了 Flink 支持的各种数据源读取方式，包括从文件、集合、元素、Socket、自定义数据源、Kafka、HTTP API、JDBC 数据库和 CSV
文件等读取数据。通过实际代码示例帮助开发者快速掌握 Flink 数据源的创建和使用方法。

## 2. 项目结构

```
flink-02-DataSource/
├── input/
│   ├── words.txt                                        # 文本测试数据
│   └── clicks.csv                                       # CSV 测试数据
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源实现
│   └── source/
│       ├── SourceFromFile.java                          # 从文件读取
│       ├── SourceFromCollection.java                    # 从集合读取
│       ├── SourceFromElements.java                      # 从元素读取
│       ├── SourceFromSocket.java                        # 从 Socket 读取
│       ├── SourceFromCsvFile.java                       # 从 CSV 文件读取并解析
│       ├── SourceFromHttpApi.java                       # 从 HTTP API 读取
│       ├── SourceFromJdbc.java                          # 从 JDBC 数据库读取
│       ├── SourceCustomTest.java                        # 自定义数据源
│       ├── SourceCustomParallelTest.java                # 自定义并行数据源
│       ├── SourceKafkaTest.java                         # 从 Kafka 读取
│       └── SourceTest.java                              # 综合示例
├── pom.xml                                              # Maven 配置
└── README.md                                            # 本文档
```

## 3. 核心实现

### 3.1 依赖配置

`pom.xml`：引入 Flink 相关依赖

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

    <!-- flink-connector-kafka: Kafka连接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
    </dependency>

    <!-- ==================== 数据库驱动依赖（可选）=================== -->
    <!-- MySQL JDBC 驱动：用于 SourceFromJdbc 示例 -->
    <!-- 如果不需要使用 JDBC 数据源，可以注释掉此依赖 -->
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.33</version>
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

### 3.3 从文件读取数据源

`src/main/java/com/action/source/SourceFromFile.java`：使用 `readTextFile()` 方法从本地文件系统读取数据

```java
package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件读取数据源示例
 * 使用 readTextFile() 方法从本地文件系统读取数据
 *
 * 特点说明：
 * - 适用场景：读取本地文件系统中的文本文件
 * - 数据特点：有界数据流（文件读取完毕后自动结束）
 * - 路径说明：文件路径相对于项目根目录
 * - 支持格式：文本文件（按行读取）
 *
 * 运行方式：
 * # 方法1: 使用 Maven 运行
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromFile"
 *
 * # 方法2: 在 IDE 中直接运行
 * # 右键 SourceFromFile.java -> Run 'SourceFromFile.main()'
 *
 * 预期输出：
 * file> hello world
 * file> hello flink
 * file> hello java
 */
public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        // 注意：文件路径相对于项目根目录
        DataStreamSource<String> stream = env.readTextFile("flink-02-DataSource/input/words.txt");

        stream.print("file");

        env.execute();
    }
}

/**
 * 常见问题：
 *
 * Q3: 文件路径错误
 * 原因：文件路径不正确
 * 解决方案：
 * - 使用相对路径：flink-02-DataSource/input/words.txt（相对于项目根目录）
 * - 使用绝对路径：/home/user/project/flink-02-DataSource/input/words.txt
 * - 检查文件是否存在
 *
 * 注意事项：
 * - 从 IDE 运行：工作目录通常是项目根目录
 * - 打包运行：需要指定正确的文件路径
 * - 集群运行：文件需要在集群所有节点可访问
 */
```

### 3.4 从集合读取数据源

`src/main/java/com/action/source/SourceFromCollection.java`：使用 `fromCollection()` 方法从 Java 集合中读取数据

```java
package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 从集合读取数据源示例
 * 使用 fromCollection() 方法从 Java 集合中读取数据
 *
 * 特点说明：
 * - 适用场景：从内存中的 Java 集合创建数据流
 * - 支持类型：支持任意类型的集合（List、Set 等）
 * - 数据特点：有界数据流
 * - 使用场景：测试、小批量数据处理
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromCollection"
 *
 * 预期输出：
 * nums> 2
 * nums> 5
 * events> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * events> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class SourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从整数集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        // 2. 从 Event 对象集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream = env.fromCollection(events);

        numStream.print("nums");
        stream.print("events");

        env.execute();
    }
}
```

### 3.5 从元素读取数据源

`src/main/java/com/action/source/SourceFromElements.java`：使用 `fromElements()` 方法直接从元素创建数据流

```java
package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从元素读取数据源示例
 * 使用 fromElements() 方法直接从元素创建数据流
 *
 * 特点说明：
 * - 适用场景：快速创建包含少量元素的数据流
 * - 使用方式：直接传入多个元素作为参数
 * - 数据特点：有界数据流
 * - 优势：代码简洁，适合测试和演示
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromElements"
 *
 * 预期输出：
 * elements> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * elements> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * elements> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class SourceFromElements {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        // 可以直接传入多个元素，Flink 会自动创建数据流
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        stream.print("elements");

        env.execute();
    }
}
```

### 3.6 从 Socket 读取数据源

`src/main/java/com/action/source/SourceFromSocket.java`：使用 `socketTextStream()` 方法从网络 Socket 读取文本流

```java
package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从 Socket 读取数据源示例
 * 使用 socketTextStream() 方法从网络 Socket 读取文本流
 *
 * 特点说明：
 * - 适用场景：从网络 Socket 接收实时数据流
 * - 数据特点：无界数据流（持续接收数据）
 * - 使用场景：实时数据测试、流处理演示
 * - 注意事项：需要外部程序持续发送数据
 *
 * 测试步骤：
 * 1. 启动 Socket 服务器（在 Linux 服务器上）
 *    # 安装 nc 工具（如果没有）
 *    sudo yum -y install nc
 *    # 或
 *    sudo apt-get install netcat-openbsd
 *    # 启动 nc 监听 7777 端口
 *    nc -lk 7777
 *
 * 2. 运行 Flink 程序
 *    cd flink-02-DataSource
 *    mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromSocket"
 *
 * 3. 在 nc 终端中输入数据
 *    hello
 *    world
 *    flink
 *
 * 4. 观察输出
 *    socket> hello
 *    socket> world
 *    socket> flink
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromSocket"
 */
public class SourceFromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 Socket 文本流读取
        // 参数：host（主机地址），port（端口号）
        // 这会创建一个无界流，持续接收数据
        DataStreamSource<String> stream = env.socketTextStream("server01", 7777);

        stream.print("socket");

        env.execute();
    }
}

/**
 * 注意事项：
 *
 * 1. Socket 连接问题
 * 连接失败：
 * // 错误示例：无法连接到主机
 * env.socketTextStream("localhost", 7777);
 *
 * 解决方案：
 * - 确保目标主机可访问
 * - 确保端口已开放
 * - 使用 nc -lk 7777 监听端口
 * - 检查防火墙设置
 *
 * 测试连接：
 * # 测试连接
 * telnet server01 7777
 * # 或
 * nc -zv server01 7777
 *
 * 2. 无界流处理
 * 特点：
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 * - Socket 断开连接后，Flink 会重连或抛出异常
 *
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 *
 * 常见问题：
 *
 * Q1: Socket 连接失败
 * 原因：网络或权限问题
 * 解决方案：
 * # 检查端口是否被占用
 * netstat -tuln | grep 7777
 * # 测试连接
 * telnet server01 7777
 * # 使用 netcat 测试
 * nc -zv server01 7777
 */
```

### 3.7 从 CSV 文件读取并解析数据源

`src/main/java/com/action/source/SourceFromCsvFile.java`：使用 `readTextFile()` 方法读取 CSV 文件，然后使用 `map()`
转换算子解析数据

```java
package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从 CSV 文件读取并解析数据源示例
 * 使用 readTextFile() 方法读取 CSV 文件，然后使用 map() 转换算子解析数据
 *
 * 特点说明：
 * - 适用场景：读取 CSV 格式的数据文件并解析为结构化数据
 * - 数据特点：有界数据流（文件读取完毕后自动结束）
 * - 实现方式：使用 readTextFile() 读取文件，使用 map() 解析 CSV 行
 * - 使用场景：数据导入、批量数据处理、数据迁移等
 *
 * CSV 文件格式（clicks.csv）：
 * Mary, ./home, 1000
 * Alice, ./cart, 2000
 * Bob, ./prod?id=100, 3000
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromCsvFile"
 *
 * 预期输出：
 * csv> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * csv> Event{user='Alice', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * csv> Event{user='Bob', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class SourceFromCsvFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 CSV 文件读取数据
        DataStreamSource<String> textStream = env.readTextFile("flink-02-DataSource/input/clicks.csv");

        // 解析 CSV 行并转换为 Event 对象
        textStream.filter(line -> {
                    // 过滤空行
                    return line != null && !line.trim().isEmpty();
                })
                .map(line -> {
                    // 按逗号分割 CSV 行
                    String[] fields = line.split(",\\s*");  // 使用正则表达式处理空格

                    // 验证字段数量
                    if (fields.length < 3) {
                        System.err.println("CSV 行格式错误，字段数量不足: " + line);
                        return null;
                    }

                    try {
                        // 解析字段
                        String user = fields[0].trim();
                        String url = fields[1].trim();
                        long timestamp = Long.parseLong(fields[2].trim());

                        // 创建 Event 对象
                        return new Event(user, url, timestamp);
                    } catch (Exception e) {
                        System.err.println("CSV 行解析失败: " + line + ", 错误: " + e.getMessage());
                        return null;
                    }
                })
                .filter(event -> event != null)  // 过滤掉解析失败的行
                .print("csv");

        env.execute();
    }
}

/**
 * 注意事项：
 *
 * 1. CSV 解析方式
 * - 简单分割：使用 split(",") 按逗号分割
 * - 处理空格：使用 split(",\\s*") 正则表达式
 * - 处理引号：使用专门的 CSV 解析库（如 OpenCSV、Apache Commons CSV）
 *
 * 2. 使用 CSV 解析库（推荐）
 * 对于复杂的 CSV 文件（包含引号、换行等），建议使用专门的解析库：
 *
 * 添加依赖（pom.xml）：
 * <dependency>
 *     <groupId>com.opencsv</groupId>
 *     <artifactId>opencsv</artifactId>
 *     <version>5.7.1</version>
 * </dependency>
 *
 * 使用示例：
 * import com.opencsv.CSVReader;
 * import java.io.StringReader;
 *
 * textStream.map(line -> {
 *     CSVReader reader = new CSVReader(new StringReader(line));
 *     String[] fields = reader.readNext();
 *     reader.close();
 *
 *     String user = fields[0];
 *     String url = fields[1];
 *     long timestamp = Long.parseLong(fields[2]);
 *
 *     return new Event(user, url, timestamp);
 * })
 *
 * 3. 错误处理
 * - 空行处理：使用 filter() 过滤空行，避免解析错误
 * - 格式错误：验证字段数量，捕获异常并记录日志
 * - 字段缺失：返回 null 并使用 filter() 过滤掉解析失败的行
 *
 * 代码实现：
 * // 1. 过滤空行
 * textStream.filter(line -> line != null && !line.trim().isEmpty())
 *
 * // 2. 验证字段数量
 * if (fields.length < 3) {
 *     System.err.println("CSV 行格式错误，字段数量不足: " + line);
 *     return null;
 * }
 *
 * // 3. 异常处理
 * try {
 *     // 解析字段...
 * } catch (Exception e) {
 *     System.err.println("CSV 行解析失败: " + line + ", 错误: " + e.getMessage());
 *     return null;
 * }
 *
 * // 4. 过滤解析失败的行
 * .filter(event -> event != null)
 *
 * 4. 性能优化
 * - 使用并行度提高读取速度
 * - 对于大文件，考虑使用 Flink 的文件系统连接器
 * - 使用批处理模式处理历史数据
 *
 * 5. 文件路径
 * - 相对路径：相对于项目根目录
 * - 绝对路径：完整的文件系统路径
 * - HDFS 路径：hdfs://namenode:port/path/to/file.csv
 *
 * 常见问题：
 *
 * Q1: CSV 解析错误
 * 原因：CSV 格式不规范（包含引号、换行、特殊字符等）
 * 解决方案：
 * - 使用专门的 CSV 解析库
 * - 预处理 CSV 文件，规范化格式
 * - 添加异常处理，跳过格式错误的行
 *
 * Q2: 字段解析错误
 * 原因：字段类型不匹配或字段缺失
 * 解决方案：
 * - 添加字段验证和类型转换
 * - 使用 try-catch 捕获解析异常
 * - 记录错误日志，便于排查问题
 *
 * Q3: 文件读取性能问题
 * 原因：文件过大或读取方式不当
 * 解决方案：
 * - 增加并行度：env.setParallelism(4)
 * - 使用 Flink 的文件系统连接器
 * - 考虑将大文件分割成多个小文件
 */
```

### 3.8 从 HTTP API 读取数据源

`src/main/java/com/action/source/SourceFromHttpApi.java`：使用自定义 SourceFunction 从 HTTP REST API 读取数据

```java
package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * 从 HTTP API 读取数据源示例
 * 使用自定义 SourceFunction 从 HTTP REST API 读取数据
 *
 * 特点说明：
 * - 适用场景：从 REST API 获取实时数据流
 * - 数据特点：可以是无界流（轮询）或有界流（单次请求）
 * - 实现方式：实现 SourceFunction<String> 接口，使用 HttpURLConnection 发送 HTTP 请求
 * - 使用场景：实时数据监控、API 数据采集、第三方数据集成等
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromHttpApi"
 *
 * 预期输出：
 * http> {"id":1,"name":"Product 1","price":100.0}
 * http> {"id":2,"name":"Product 2","price":200.0}
 * ...
 */
public class SourceFromHttpApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 HTTP API 读取数据
        DataStreamSource<String> stream = env.addSource(new HttpApiSource(
                "https://jsonplaceholder.typicode.com/posts",  // API 地址
                5000  // 轮询间隔（毫秒），设置为 0 表示只请求一次
        ));

        stream.print("http");

        env.execute();
    }

    /**
     * HTTP API 数据源实现
     */
    public static class HttpApiSource implements SourceFunction<String> {
        private String apiUrl;
        private long interval;
        private boolean running = true;

        public HttpApiSource(String apiUrl, long interval) {
            this.apiUrl = apiUrl;
            this.interval = interval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                try {
                    // 创建 HTTP 连接
                    URL url = new URL(apiUrl);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    connection.setConnectTimeout(5000);
                    connection.setReadTimeout(5000);

                    // 读取响应
                    int responseCode = connection.getResponseCode();
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(connection.getInputStream())
                        );
                        String line;
                        while ((line = reader.readLine()) != null) {
                            // 发送数据到 Flink 流
                            ctx.collect(line);
                        }
                        reader.close();
                    } else {
                        System.err.println("HTTP 请求失败，响应码: " + responseCode);
                    }

                    connection.disconnect();

                    // 如果间隔为 0，只请求一次后退出
                    if (interval == 0) {
                        break;
                    }

                    // 等待指定时间后再次请求
                    Thread.sleep(interval);
                } catch (Exception e) {
                    System.err.println("HTTP 请求异常: " + e.getMessage());
                    // 发生异常时等待一段时间后重试
                    Thread.sleep(interval);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

/**
 * 注意事项：
 *
 * 1. HTTP 请求配置
 * - 设置连接超时和读取超时，避免长时间阻塞
 * - 根据 API 要求设置请求头（如 Authorization、Content-Type 等）
 * - 处理 HTTP 错误响应码
 *
 * 2. 轮询策略
 * - 单次请求：设置 interval = 0，请求一次后退出
 * - 轮询请求：设置 interval > 0，每隔指定时间请求一次
 * - 注意控制请求频率，避免对 API 服务器造成压力
 *
 * 3. 异常处理
 * - 网络异常：捕获并重试
 * - HTTP 错误：记录错误码并处理
 * - 数据解析错误：根据实际情况处理
 *
 * 4. 认证方式
 * - API Key：在请求头中添加 "X-API-Key: your-api-key"
 * - Bearer Token：在请求头中添加 "Authorization: Bearer your-token"
 * - Basic Auth：使用 Authenticator 或手动设置请求头
 *
 * 示例：添加认证
 * connection.setRequestProperty("Authorization", "Bearer your-token");
 * connection.setRequestProperty("X-API-Key", "your-api-key");
 *
 * 常见问题：
 *
 * Q1: HTTP 连接超时
 * 原因：网络延迟或服务器响应慢
 * 解决方案：
 * - 增加连接超时时间：connection.setConnectTimeout(10000);
 * - 增加读取超时时间：connection.setReadTimeout(10000);
 * - 检查网络连接和服务器状态
 *
 * Q2: API 限流
 * 原因：请求频率过高
 * 解决方案：
 * - 增加轮询间隔：设置更大的 interval 值
 * - 实现请求限流机制
 * - 使用指数退避策略重试
 */
```

### 3.9 从 JDBC 数据库读取数据源

`src/main/java/com/action/source/SourceFromJdbc.java`：使用自定义 SourceFunction 从关系型数据库读取数据

```java
package com.action.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从 JDBC 数据库读取数据源示例
 * 使用自定义 SourceFunction 从关系型数据库读取数据
 *
 * 特点说明：
 * - 适用场景：从 MySQL、PostgreSQL、Oracle 等关系型数据库读取数据
 * - 数据特点：可以是无界流（轮询）或有界流（单次查询）
 * - 实现方式：实现 SourceFunction<String> 接口，使用 JDBC 连接数据库
 * - 使用场景：数据库数据同步、实时数据监控、数据迁移等
 *
 * 前置条件：
 * 1. 安装并启动数据库（MySQL、PostgreSQL 等）
 * 2. 创建数据库和测试表
 * 3. 插入测试数据
 * 4. 确保 pom.xml 中已添加数据库驱动依赖（MySQL 驱动已添加）
 *
 * MySQL 数据库建表建库完整步骤：
 *
 * 步骤1：登录 MySQL
 * <pre>{@code
mysql -u root -p
}</pre>
 *
 * 步骤2：创建数据库
 * <pre>{@code
CREATE DATABASE IF NOT EXISTS testdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
}</pre>
 *
 * 步骤3：使用数据库
 * <pre>{@code
USE testdb;
}</pre>
 *
 * 步骤4：创建表
 * <pre>{@code
CREATE TABLE IF NOT EXISTS events (
id INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
user_name VARCHAR(50) NOT NULL COMMENT '用户名',
url VARCHAR(200) NOT NULL COMMENT 'URL地址',
timestamp BIGINT NOT NULL COMMENT '时间戳',
INDEX idx_user_name (user_name),
INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件表';
}</pre>
 *
 * 步骤5：插入测试数据
 * <pre>{@code
INSERT INTO events (user_name, url, timestamp) VALUES
('Mary', './home', 1000),
('Bob', './cart', 2000),
('Alice', './prod?id=1', 3000),
('Mary', './home', 4000),
('Bob', './prod?id=2', 5000);
}</pre>
 *
 * 步骤6：验证数据
 * <pre>{@code
SELECT * FROM events;
}</pre>
 *
 * 步骤7：查看表结构
 * <pre>{@code
DESC events;
}</pre>
 * 或
 * <pre>{@code
SHOW CREATE TABLE events;
}</pre>
 *
 * 方式二：完整 SQL 脚本
 * 将以下 SQL 脚本一次性执行，避免 "No database selected" 错误：
 * <pre>{@code
-- 创建数据库
CREATE DATABASE IF NOT EXISTS testdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 使用数据库（重要：必须执行）
USE testdb;

-- 创建表
CREATE TABLE IF NOT EXISTS events (
id INT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
user_name VARCHAR(50) NOT NULL COMMENT '用户名',
url VARCHAR(200) NOT NULL COMMENT 'URL地址',
timestamp BIGINT NOT NULL COMMENT '时间戳',
INDEX idx_user_name (user_name),
INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件表';

-- 插入测试数据
INSERT INTO events (user_name, url, timestamp) VALUES
('Mary', './home', 1000),
('Bob', './cart', 2000),
('Alice', './prod?id=1', 3000),
('Mary', './home', 4000),
('Bob', './prod?id=2', 5000);

-- 验证数据
SELECT * FROM events;
}</pre>
 *
 * 运行方式：
 * <pre>{@code
cd flink-02-DataSource
mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromJdbc"
}</pre>
 *
 * 预期输出：
 * <pre>{@code
jdbc> Mary,./home,1000
jdbc> Bob,./cart,2000
jdbc> Alice,./prod?id=1,3000
}</pre>
 */
public class SourceFromJdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 JDBC 数据库读取数据
        DataStreamSource<String> stream = env.addSource(new JdbcSource(
                "jdbc:mysql://localhost:3306/testdb",  // 数据库连接 URL
                "root",                                 // 用户名
                "password",                             // 密码
                "SELECT user_name, url, timestamp FROM events",  // SQL 查询语句
                0  // 轮询间隔（毫秒），设置为 0 表示只查询一次
        ));

        stream.print("jdbc");

        env.execute();
    }

    /**
     * JDBC 数据源实现
     */
    public static class JdbcSource implements SourceFunction<String> {
        private String jdbcUrl;
        private String username;
        private String password;
        private String sql;
        private long interval;
        private boolean running = true;

        public JdbcSource(String jdbcUrl, String username, String password, String sql, long interval) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.sql = sql;
            this.interval = interval;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;

            try {
                // 加载数据库驱动（MySQL）
                Class.forName("com.mysql.cj.jdbc.Driver");

                // 建立数据库连接
                connection = DriverManager.getConnection(jdbcUrl, username, password);

                while (running) {
                    // 执行 SQL 查询
                    statement = connection.prepareStatement(sql);
                    resultSet = statement.executeQuery();

                    // 读取查询结果
                    while (resultSet.next()) {
                        // 构建输出字符串（根据实际表结构调整）
                        String user = resultSet.getString("user_name");
                        String url = resultSet.getString("url");
                        long timestamp = resultSet.getLong("timestamp");

                        String output = user + "," + url + "," + timestamp;
                        ctx.collect(output);
                    }

                    // 关闭结果集和语句
                    if (resultSet != null) {
                        resultSet.close();
                        resultSet = null;
                    }
                    if (statement != null) {
                        statement.close();
                        statement = null;
                    }

                    // 如果间隔为 0，只查询一次后退出
                    if (interval == 0) {
                        break;
                    }

                    // 等待指定时间后再次查询
                    Thread.sleep(interval);
                }
            } catch (Exception e) {
                System.err.println("JDBC 查询异常: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // 关闭资源
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

/**
 * 注意事项：
 *
 * 1. 数据库驱动依赖
 * 本项目已在 pom.xml 中添加了 MySQL 驱动依赖，如果使用其他数据库，需要添加对应的驱动依赖：
 *
 * MySQL（已添加）:
 * <pre>{@code
<dependency>
<groupId>mysql</groupId>
<artifactId>mysql-connector-java</artifactId>
<version>8.0.33</version>
</dependency>
}</pre>
 *
 * 注意：添加依赖后需要重新编译项目：
 * <pre>{@code
mvn clean compile
}</pre>
 *
 * PostgreSQL:
 * <pre>{@code
<dependency>
<groupId>org.postgresql</groupId>
<artifactId>postgresql</artifactId>
<version>42.6.0</version>
</dependency>
}</pre>
 *
 * 2. 数据库连接配置
 * - MySQL: jdbc:mysql://host:port/database
 * - PostgreSQL: jdbc:postgresql://host:port/database
 * - Oracle: jdbc:oracle:thin:@host:port:database
 *
 * 3. 连接池使用
 * 生产环境建议使用连接池（如 HikariCP、Druid）：
 * - 提高连接复用率
 * - 减少连接创建开销
 * - 更好的连接管理
 *
 * 4. SQL 查询优化
 * - 使用索引字段进行查询
 * - 限制查询结果数量（LIMIT）
 * - 使用增量查询（WHERE timestamp > ?）
 *
 * 5. 增量数据读取
 * 对于增量数据读取，可以使用时间戳或自增 ID：
 *
 * <pre>{@code
// 记录上次读取的最大 ID
long lastId = 0;

// 增量查询
String sql = "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT 1000";
statement.setLong(1, lastId);

// 更新 lastId
while (resultSet.next()) {
long currentId = resultSet.getLong("id");
if (currentId > lastId) {
lastId = currentId;
}
// 处理数据...
}
}</pre>
 *
 * 常见问题：
 *
 * Q1: 数据库连接失败
 * 原因：数据库未启动、连接信息错误或网络不通
 * 解决方案：
 * - 检查数据库是否启动：mysql -u root -p
 * - 验证连接信息：jdbc:mysql://localhost:3306/testdb
 * - 检查网络连接：telnet localhost 3306
 * - 检查防火墙设置
 *
 * Q2: 驱动类未找到（ClassNotFoundException）
 * 原因：缺少数据库驱动依赖或未重新编译项目
 * 解决方案：
 * - 检查 pom.xml 中是否已添加对应的数据库驱动依赖（MySQL 驱动已添加）
 * - 确保驱动版本与数据库版本兼容
 * - 重新编译项目：mvn clean compile
 * - 如果使用 IDE，刷新 Maven 项目并重新构建
 * - 检查 classpath 中是否包含驱动 jar 包
 */
```

### 3.10 自定义数据源

`src/main/java/com/action/source/SourceCustomTest.java`：使用自定义的 `SourceFunction` 实现类创建数据源

```java
package com.action.source;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源示例
 * 使用自定义的 SourceFunction 实现类创建数据源
 *
 * ClickSource 实现了 SourceFunction<Event> 接口
 * 会持续生成随机的点击事件数据
 *
 * 特点说明：
 * - 适用场景：需要自定义数据生成逻辑的场景
 * - 实现方式：实现 SourceFunction<T> 接口
 * - 数据特点：可以是无界流（持续生成）或有界流（生成完成后结束）
 * - 优势：灵活控制数据生成逻辑
 *
 * ClickSource 实现要点：
 * 1. 实现接口：SourceFunction<Event>
 * 2. 核心方法：
 *    - run()：数据生成逻辑，通过 ctx.collect() 发送数据
 *    - cancel()：取消数据生成，设置 running = false
 * 3. 控制机制：使用 running 标志位控制数据生成循环
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceCustomTest"
 *
 * 预期输出：
 * SourceCustom> Event{user='Bob', url='./cart', timestamp=2024-11-12 00:16:23.456}
 * SourceCustom> Event{user='Mary', url='./home', timestamp=2024-11-12 00:16:24.456}
 * SourceCustom> Event{user='Alice', url='./prod?id=1', timestamp=2024-11-12 00:16:25.456}
 * ...
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 有了自定义的 source function，调用 addSource 方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();
    }
}

/**
 * 注意事项：
 *
 * 1. 自定义数据源实现要点
 * - 必须实现 run() 和 cancel() 方法
 * - 使用 running 标志位控制循环
 * - 通过 ctx.collect() 发送数据
 * - 注意线程安全（如果使用共享变量）
 *
 * 2. 常见错误
 * - 忘记实现 cancel() 方法
 * - 没有设置 running 标志位
 * - 在 run() 方法中使用阻塞操作导致无法取消
 *
 * 3. 无界流处理
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 *
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 * - 调用 cancel() 方法（自定义数据源）
 *
 * 常见问题：
 *
 * Q4: 自定义数据源无法停止
 * 原因：未正确实现 cancel() 方法
 * 解决方案：
 * @Override
 * public void cancel() {
 *     running = false;  // 必须设置标志位
 * }
 */
```

### 3.11 自定义并行数据源

`src/main/java/com/action/source/SourceCustomParallelTest.java`：使用 `ParallelSourceFunction` 接口实现可并行的数据源

```java
package com.action.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 自定义并行数据源示例
 * 使用 ParallelSourceFunction 接口实现可并行的数据源
 *
 * 与 SourceFunction 的区别：
 * - SourceFunction 只能单并行度运行
 * - ParallelSourceFunction 可以设置多个并行度
 *
 * 特点说明：
 * - 适用场景：需要高吞吐量的数据生成场景
 * - 实现方式：实现 ParallelSourceFunction<T> 接口
 * - 并行度：可以设置多个并行度，提高数据生成速度
 * - 与 SourceFunction 的区别：
 *   - SourceFunction：只能单并行度运行
 *   - ParallelSourceFunction：可以设置多个并行度
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceCustomParallelTest"
 *
 * 预期输出：
 * 1> -1234567890
 * 2> 987654321
 * 1> -456789012
 * 2> 123456789
 * ...
 */
public class SourceCustomParallelTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为 2，会启动 2 个并行任务同时生成数据
        env.addSource(new CustomSource()).setParallelism(2).print();

        env.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

/**
 * 注意事项：
 *
 * 1. 并行度设置
 * 默认并行度：
 * // Flink 默认并行度等于 CPU 核心数
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * // 设置并行度
 * env.setParallelism(4);
 *
 * 并行度影响：
 * - 数据会被分发到多个 subtask
 * - 影响性能和数据分布
 * - 相同 key 的数据始终发送到同一个 subtask
 *
 * 2. 自定义数据源实现要点
 * - 必须实现 run() 和 cancel() 方法
 * - 使用 running 标志位控制循环
 * - 通过 ctx.collect() 发送数据
 * - 注意线程安全（如果使用共享变量）
 *
 * 3. 常见错误
 * - 忘记实现 cancel() 方法
 * - 没有设置 running 标志位
 * - 在 run() 方法中使用阻塞操作导致无法取消
 *
 * 4. 无界流处理
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 *
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 * - 调用 cancel() 方法（自定义数据源）
 *
 * 常见问题：
 *
 * Q1: 并行度设置无效
 * 原因：某些数据源不支持并行
 * 解决方案：
 * - SourceFunction 只能单并行度
 * - 使用 ParallelSourceFunction 支持并行
 * - 检查数据源类型
 *
 * Q2: 自定义数据源无法停止
 * 原因：未正确实现 cancel() 方法
 * 解决方案：
 * @Override
 * public void cancel() {
 *     running = false;  // 必须设置标志位
 * }
 */
```

### 3.12 从 Kafka 读取数据源

#### 3.12.1 前置条件：Kafka 环境搭建（KRaft 模式）

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

#### 3.12.2 FlinkKafkaConsumer 代码实现

`src/main/java/com/action/source/SourceKafkaTest.java`：使用 `FlinkKafkaConsumer` 从 Kafka 主题读取数据

```java
package com.action.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从 Kafka 读取数据源示例
 * 使用 FlinkKafkaConsumer 从 Kafka 主题读取数据
 *
 * 前置条件：
 * 1. 在 server01 上使用 KRaft 模式启动 Kafka 集群
 * 2. 创建主题：./kafka-topics.sh --create --topic clicks --bootstrap-server server01:9092 --partitions 1 --replication-factor 1
 * 3. 向主题发送数据：./kafka-console-producer.sh --topic clicks --bootstrap-server server01:9092
 *
 * 特点说明：
 * - 适用场景：从 Kafka 主题读取实时数据流
 * - 数据特点：无界数据流
 * - 配置要点：
 *   - bootstrap.servers：Kafka 服务器地址
 *   - group.id：消费者组 ID
 *   - key.deserializer / value.deserializer：反序列化器
 *   - auto.offset.reset：偏移量重置策略（latest/earliest）
 *
 * 运行方式：
 *
 * 完整测试流程：
 *
 * 步骤1：在 server01 上启动 Kafka（KRaft 模式）
 * # 在 server01 上执行
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * # 生成 UUID 并格式化（首次启动需要）
 * ./kafka-storage.sh random-uuid
 * ./kafka-storage.sh format -t <生成的UUID> -c ../config/kraft/server.properties
 * # 启动 Kafka
 * ./kafka-server-start.sh -daemon ../config/kraft/server.properties
 *
 * 步骤2：创建主题
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-topics.sh --create \
 *     --topic clicks \
 *     --bootstrap-server server01:9092 \
 *     --partitions 1 \
 *     --replication-factor 1
 *
 * 步骤3：启动 Flink 程序（在本地或客户端）
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceKafkaTest"
 *
 * 步骤4：向主题发送数据（在 server01 上）
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-console-producer.sh \
 *     --topic clicks \
 *     --bootstrap-server server01:9092
 * # 输入消息，每行一条
 * # message1
 * # message2
 * # message3
 *
 * 步骤5：观察 Flink 程序输出
 * Flink 程序会实时打印从 Kafka 接收到的消息：
 * Kafka> message1
 * Kafka> message2
 * Kafka> message3
 */
public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置 Kafka 连接属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "server01:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 创建 Kafka 消费者
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",                    // 主题名称
                new SimpleStringSchema(),    // 反序列化器
                properties                   // Kafka 配置
        ));

        stream.print("Kafka");

        env.execute();
    }
}

/**
 * 注意事项：
 *
 * 1. Kafka 配置问题
 * 常见配置项：
 * - bootstrap.servers：Kafka 服务器地址
 * - group.id：消费者组 ID
 * - key.deserializer / value.deserializer：反序列化器
 * - auto.offset.reset：偏移量重置策略（latest/earliest）
 *
 * 注意事项：
 * - 确保 Kafka 集群已启动
 * - 确保主题已创建
 * - 确保网络连接正常
 *
 * 2. 无界流处理
 * 特点：
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 *
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 *
 * 常见问题：
 *
 * Q2: Kafka 连接失败
 * 原因：Kafka 未启动、配置错误或网络不通
 * 解决方案：
 * # 检查 Kafka 是否启动
 * ps -ef | grep kafka
 * # 检查主题是否存在
 * cd /opt/module/kafka_2.13-3.7.0/bin
 * ./kafka-topics.sh --list --bootstrap-server server01:9092
 * # 测试网络连接
 * telnet server01 9092
 * # 或
 * telnet 192.168.56.11 9092
 * # 检查配置文件中的 advertised.listeners 是否正确
 * cat /opt/module/kafka_2.13-3.7.0/config/kraft/server.properties | grep advertised.listeners
 * # 确保 advertised.listeners 配置为可访问的地址
 * # 如果使用主机名，确保客户端能解析该主机名
 * # 如果使用 IP，确保 IP 地址正确
 */
```

### 3.13 综合数据源示例

`src/main/java/com/action/source/SourceTest.java`：演示 Flink 支持的多种数据源读取方式

```java
package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 综合数据源示例
 * 演示 Flink 支持的多种数据源读取方式
 *
 * 特点说明：
 * - 综合演示：展示多种数据源创建方式
 * - 灵活切换：通过注释/取消注释切换不同的数据源
 * - 学习参考：适合初学者理解各种数据源的使用方法
 *
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceTest"
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("flink-02-DataSource/input/clicks.csv");

        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4. 从 Socket 文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("server01", 7777);

        // 打印输出（根据需要取消注释）
        // stream1.print("1");
        // numStream.print("nums");
        // stream2.print("2");
        // stream3.print("3");
        stream4.print("4");

        env.execute();
    }
}
```

## 4. 数据源类型对比

### 4.1 有界数据源 vs 无界数据源

| 数据源类型                          | 有界/无界 | 特点                   | 适用场景                    |
|--------------------------------|-------|----------------------|-------------------------|
| **文件**                         | 有界    | 读取完毕后自动结束            | 批量数据处理、历史数据分析           |
| **CSV 文件**                     | 有界    | 读取并解析 CSV 格式数据       | 数据导入、批量数据处理、数据迁移        |
| **集合**                         | 有界    | 集合数据读取完毕后结束          | 测试、小批量数据处理              |
| **元素**                         | 有界    | 元素读取完毕后结束            | 测试、演示                   |
| **Socket**                     | 无界    | 持续接收数据，需要手动停止        | 实时数据测试                  |
| **HTTP API**                   | 可配置   | 从 REST API 获取数据，支持轮询 | 实时数据监控、API 数据采集、第三方数据集成 |
| **JDBC 数据库**                   | 可配置   | 从关系型数据库读取数据，支持轮询     | 数据库数据同步、实时数据监控、数据迁移     |
| **自定义 SourceFunction**         | 可配置   | 根据实现逻辑决定             | 灵活的数据生成                 |
| **自定义 ParallelSourceFunction** | 可配置   | 支持并行，提高吞吐量           | 高吞吐量数据生成                |
| **Kafka**                      | 无界    | 持续从 Kafka 主题读取       | 实时数据处理                  |

### 4.2 数据源 API 对比

| API 方法                              | 返回类型                       | 数据特点       | 并行度  |
|-------------------------------------|----------------------------|------------|------|
| `readTextFile()`                    | `DataStreamSource<String>` | 有界         | 支持并行 |
| `readTextFile() + map()`            | `DataStreamSource<T>`      | 有界（CSV 解析） | 支持并行 |
| `fromCollection()`                  | `DataStreamSource<T>`      | 有界         | 支持并行 |
| `fromElements()`                    | `DataStreamSource<T>`      | 有界         | 支持并行 |
| `socketTextStream()`                | `DataStreamSource<String>` | 无界         | 单并行度 |
| `addSource(HttpApiSource)`          | `DataStreamSource<String>` | 可配置        | 单并行度 |
| `addSource(JdbcSource)`             | `DataStreamSource<String>` | 可配置        | 单并行度 |
| `addSource(SourceFunction)`         | `DataStreamSource<T>`      | 可配置        | 单并行度 |
| `addSource(ParallelSourceFunction)` | `DataStreamSource<T>`      | 可配置        | 支持并行 |
| `addSource(FlinkKafkaConsumer)`     | `DataStreamSource<T>`      | 无界         | 支持并行 |

### 4.3 自定义数据源接口对比

| 接口                              | 并行度  | 使用场景            | 实现复杂度 |
|---------------------------------|------|-----------------|-------|
| `SourceFunction<T>`             | 单并行度 | 简单数据生成          | 简单    |
| `ParallelSourceFunction<T>`     | 多并行度 | 高吞吐量数据生成        | 中等    |
| `RichSourceFunction<T>`         | 单并行度 | 需要访问运行时上下文      | 中等    |
| `RichParallelSourceFunction<T>` | 多并行度 | 需要访问运行时上下文且支持并行 | 复杂    |

## 5. 测试数据

### 5.1 words.txt

```
hello world
hello flink
hello java
```

### 5.2 clicks.csv

```
Mary, ./home, 1000
Alice, ./cart, 2000
Bob, ./prod?id=100, 3000
Bob, ./cart, 4000
Bob, ./home, 5000
Mary, ./home, 6000
Bob, ./home, 7000
Bob, ./home, 8000
Bob, ./prod?id=10, 9000
Mary, ./home, 11000
Bob, ./cart, 13000
Bob, ./cart, 15000
```

## 6. 核心概念详解

### 6.1 SourceFunction 接口

**接口定义：**

```java
public interface SourceFunction<T> extends Function, Serializable {
    void run(SourceContext<T> ctx) throws Exception;

    void cancel();
}
```

**核心方法：**

- `run(SourceContext<T> ctx)`：数据生成逻辑，通过 `ctx.collect()` 发送数据
- `cancel()`：取消数据生成，通常设置标志位停止循环

**实现要点：**

1. 使用 `running` 标志位控制数据生成循环
2. 在 `run()` 方法中通过 `ctx.collect()` 发送数据
3. 在 `cancel()` 方法中设置 `running = false` 停止生成

### 6.2 ParallelSourceFunction 接口

**接口定义：**

```java
public interface ParallelSourceFunction<T> extends SourceFunction<T> {
    // 继承自 SourceFunction，无额外方法
}
```

**与 SourceFunction 的区别：**

- `SourceFunction`：只能单并行度运行
- `ParallelSourceFunction`：可以设置多个并行度，提高数据生成速度

**使用场景：**

- 需要高吞吐量的数据生成
- 数据生成逻辑可以并行执行

### 6.3 数据源生命周期

1. **创建阶段**：调用 `env.readTextFile()` 或 `env.addSource()` 创建数据源
2. **执行阶段**：调用 `env.execute()` 启动任务，数据源开始生成数据
3. **运行阶段**：数据源持续生成数据（无界流）或生成完成后结束（有界流）
4. **停止阶段**：调用 `cancel()` 方法或数据生成完成后停止

### 6.4 文件路径问题

**相对路径 vs 绝对路径：**

```java
// 相对路径：相对于项目根目录
env.readTextFile("flink-02-DataSource/input/words.txt")

// 绝对路径：完整路径
env.

readTextFile("/home/user/project/flink-02-DataSource/input/words.txt")
```

**注意事项：**

- 从 IDE 运行：工作目录通常是项目根目录
- 打包运行：需要指定正确的文件路径
- 集群运行：文件需要在集群所有节点可访问

## 7. 参考资料

- [Flink 官方文档 - 数据源（Source）](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/)
- [Flink 官方文档 - 内置数据源](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#内置数据源)
- [Flink 官方文档 - Kafka 连接器](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/kafka/)
- [Flink 官方文档 - 自定义数据源](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#自定义数据源)
- [Flink 官方文档 - 数据源函数](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/sources/#数据源函数)

