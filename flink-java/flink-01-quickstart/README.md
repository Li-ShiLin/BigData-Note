<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 快速入门演示](#flink-%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8%E6%BC%94%E7%A4%BA)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心实现](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [1. 依赖配置](#1-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
      - [Flink 核心依赖](#flink-%E6%A0%B8%E5%BF%83%E4%BE%9D%E8%B5%96)
      - [日志管理依赖](#%E6%97%A5%E5%BF%97%E7%AE%A1%E7%90%86%E4%BE%9D%E8%B5%96)
      - [测试依赖](#%E6%B5%8B%E8%AF%95%E4%BE%9D%E8%B5%96)
    - [2. 配置文件](#2-%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)
    - [3. 测试数据](#3-%E6%B5%8B%E8%AF%95%E6%95%B0%E6%8D%AE)
    - [4. 批处理 WordCount（DataSet API）](#4-%E6%89%B9%E5%A4%84%E7%90%86-wordcountdataset-api)
      - [核心概念](#%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5)
      - [核心实现详解](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0%E8%AF%A6%E8%A7%A3)
      - [执行结果解析](#%E6%89%A7%E8%A1%8C%E7%BB%93%E6%9E%9C%E8%A7%A3%E6%9E%90)
      - [运行方式](#%E8%BF%90%E8%A1%8C%E6%96%B9%E5%BC%8F)
    - [5. 有界流 WordCount（DataStream API）](#5-%E6%9C%89%E7%95%8C%E6%B5%81-wordcountdatastream-api)
      - [核心概念](#%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5-1)
      - [核心实现详解](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0%E8%AF%A6%E8%A7%A3-1)
      - [与批处理的区别](#%E4%B8%8E%E6%89%B9%E5%A4%84%E7%90%86%E7%9A%84%E5%8C%BA%E5%88%AB)
      - [运行方式](#%E8%BF%90%E8%A1%8C%E6%96%B9%E5%BC%8F-1)
    - [6. 无界流 WordCount（DataStream API）](#6-%E6%97%A0%E7%95%8C%E6%B5%81-wordcountdatastream-api)
      - [核心概念](#%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5-2)
      - [核心实现详解](#%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0%E8%AF%A6%E8%A7%A3-2)
      - [测试步骤](#%E6%B5%8B%E8%AF%95%E6%AD%A5%E9%AA%A4)
      - [运行方式](#%E8%BF%90%E8%A1%8C%E6%96%B9%E5%BC%8F-2)
  - [Flink 处理模式对比](#flink-%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E5%AF%B9%E6%AF%94)
    - [1. 三种模式对比](#1-%E4%B8%89%E7%A7%8D%E6%A8%A1%E5%BC%8F%E5%AF%B9%E6%AF%94)
    - [2. DataSet API vs DataStream API](#2-dataset-api-vs-datastream-api)
      - [相似点](#%E7%9B%B8%E4%BC%BC%E7%82%B9)
      - [不同点](#%E4%B8%8D%E5%90%8C%E7%82%B9)
    - [3. 核心操作对比](#3-%E6%A0%B8%E5%BF%83%E6%93%8D%E4%BD%9C%E5%AF%B9%E6%AF%94)
      - [数据读取](#%E6%95%B0%E6%8D%AE%E8%AF%BB%E5%8F%96)
      - [分组操作](#%E5%88%86%E7%BB%84%E6%93%8D%E4%BD%9C)
  - [测试方法](#%E6%B5%8B%E8%AF%95%E6%96%B9%E6%B3%95)
    - [1. 环境准备](#1-%E7%8E%AF%E5%A2%83%E5%87%86%E5%A4%87)
    - [2. 运行批处理 WordCount](#2-%E8%BF%90%E8%A1%8C%E6%89%B9%E5%A4%84%E7%90%86-wordcount)
    - [3. 运行有界流 WordCount](#3-%E8%BF%90%E8%A1%8C%E6%9C%89%E7%95%8C%E6%B5%81-wordcount)
    - [4. 运行无界流 WordCount](#4-%E8%BF%90%E8%A1%8C%E6%97%A0%E7%95%8C%E6%B5%81-wordcount)
  - [核心概念详解](#%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5%E8%AF%A6%E8%A7%A3)
    - [1. Lambda 表达式与泛型擦除](#1-lambda-%E8%A1%A8%E8%BE%BE%E5%BC%8F%E4%B8%8E%E6%B3%9B%E5%9E%8B%E6%93%A6%E9%99%A4)
    - [2. KeyBy 与 GroupBy](#2-keyby-%E4%B8%8E-groupby)
    - [3. Sum 聚合操作](#3-sum-%E8%81%9A%E5%90%88%E6%93%8D%E4%BD%9C)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)
    - [1. 文件路径问题](#1-%E6%96%87%E4%BB%B6%E8%B7%AF%E5%BE%84%E9%97%AE%E9%A2%98)
    - [2. Socket 连接问题](#2-socket-%E8%BF%9E%E6%8E%A5%E9%97%AE%E9%A2%98)
    - [3. 程序不结束问题](#3-%E7%A8%8B%E5%BA%8F%E4%B8%8D%E7%BB%93%E6%9D%9F%E9%97%AE%E9%A2%98)
    - [4. 并行度设置](#4-%E5%B9%B6%E8%A1%8C%E5%BA%A6%E8%AE%BE%E7%BD%AE)
    - [5. 依赖版本](#5-%E4%BE%9D%E8%B5%96%E7%89%88%E6%9C%AC)
    - [6. 类型信息](#6-%E7%B1%BB%E5%9E%8B%E4%BF%A1%E6%81%AF)
    - [7. 输出格式说明](#7-%E8%BE%93%E5%87%BA%E6%A0%BC%E5%BC%8F%E8%AF%B4%E6%98%8E)
    - [8. Maven 配置](#8-maven-%E9%85%8D%E7%BD%AE)
  - [快速参考](#%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [代码位置](#%E4%BB%A3%E7%A0%81%E4%BD%8D%E7%BD%AE)
    - [运行命令](#%E8%BF%90%E8%A1%8C%E5%91%BD%E4%BB%A4)
    - [数据源](#%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [关键 API](#%E5%85%B3%E9%94%AE-api)
  - [学习建议](#%E5%AD%A6%E4%B9%A0%E5%BB%BA%E8%AE%AE)
    - [1. 入门路径](#1-%E5%85%A5%E9%97%A8%E8%B7%AF%E5%BE%84)
    - [2. 实践建议](#2-%E5%AE%9E%E8%B7%B5%E5%BB%BA%E8%AE%AE)
    - [3. 进阶学习](#3-%E8%BF%9B%E9%98%B6%E5%AD%A6%E4%B9%A0)
  - [常见问题](#%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
    - [Q1: 程序报错 "Could not find or load main class"](#q1-%E7%A8%8B%E5%BA%8F%E6%8A%A5%E9%94%99-could-not-find-or-load-main-class)
    - [Q2: Socket 连接失败](#q2-socket-%E8%BF%9E%E6%8E%A5%E5%A4%B1%E8%B4%A5)
    - [Q3: 泛型擦除错误](#q3-%E6%B3%9B%E5%9E%8B%E6%93%A6%E9%99%A4%E9%94%99%E8%AF%AF)
    - [Q4: 输出乱码](#q4-%E8%BE%93%E5%87%BA%E4%B9%B1%E7%A0%81)
    - [Q5: 程序不结束](#q5-%E7%A8%8B%E5%BA%8F%E4%B8%8D%E7%BB%93%E6%9D%9F)
  - [总结](#%E6%80%BB%E7%BB%93)
  - [参考资料](#%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 快速入门演示

## 项目作用

本项目演示了 Flink 的三种核心数据处理模式：批处理（DataSet API）、有界流处理（DataStream API）、无界流处理（DataStream API），通过经典的 WordCount 示例帮助开发者快速理解 Flink 的核心概念和 API 使用方法。

## 项目结构

```
flink-01-quickstart/
├── input/
│   └── words.txt                                        # 测试数据文件
├── src/main/java/com/action/
│   ├── App.java                                         # Hello World 程序
│   └── wc/
│       ├── BatchWordCount.java                          # 批处理WordCount
│       ├── BoundedStreamWordCount.java                  # 有界流WordCount
│       └── StreamWordCount.java                         # 无界流WordCount
├── src/main/resources/
│   └── log4j.properties                                 # 日志配置
├── pom.xml                                              # Maven 配置
└── README.md                                            # 本文档
```

## 核心实现

### 1. 依赖配置

`pom.xml`：引入 Flink 相关依赖

```xml
<dependencies>
    <!-- ==================== Flink 核心依赖 ==================== -->
    
    <!-- flink-java: Flink核心Java API，提供批处理DataSet API -->
    <!-- 作用：提供 DataSet API，用于批处理数据处理 -->
    <!-- 版本：通过 ${flink.version} 定义，本项目使用 1.13.0 -->
    <!-- 使用场景：批处理 WordCount、历史数据分析等 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
    </dependency>

    <!-- flink-streaming-java: Flink流处理Java API，提供DataStream API -->
    <!-- 作用：提供 DataStream API，用于流处理数据处理 -->
    <!-- 注意：依赖 Scala 版本，因为 Flink 底层使用 Akka 进行分布式通信，而 Akka 是用 Scala 开发的 -->
    <!-- 版本：flink-streaming-java_${scala.binary.version} 最终解析为 flink-streaming-java_2.12:1.13.0 -->
    <!-- 使用场景：流处理 WordCount、实时数据处理等 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
    </dependency>

    <!-- flink-clients: Flink客户端依赖，提供作业提交和客户端功能 -->
    <!-- 作用：提供作业提交、客户端功能和本地执行环境 -->
    <!-- 使用场景：本地运行 Flink 程序、作业提交到集群等 -->
    <!-- 版本：与 flink-streaming-java 一致，使用相同的 Scala 版本 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients_${scala.binary.version}</artifactId>
    </dependency>

    <!-- ==================== 日志管理依赖 ==================== -->
    
    <!-- slf4j-api: 日志门面接口 -->
    <!-- 作用：提供统一的日志接口，屏蔽底层日志实现 -->
    <!-- 版本：1.7.30，通过 ${slf4j.version} 统一管理 -->
    <!-- 优势：灵活切换日志框架（log4j、logback 等） -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
    </dependency>
    
    <!-- slf4j-log4j12: slf4j到log4j的桥接器 -->
    <!-- 作用：将 slf4j 日志调用转换为 log4j 实现 -->
    <!-- 版本：1.7.30，与 slf4j-api 版本保持一致 -->
    <!-- 说明：使用 slf4j API 编写日志，实际使用 log4j 处理日志 -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-log4j12</artifactId>
    </dependency>
    
    <!-- log4j-to-slf4j: log4j到slf4j的桥接器，避免日志冲突 -->
    <!-- 作用：将旧的 log4j 日志调用转换到 slf4j，统一日志框架 -->
    <!-- 版本：2.14.0，通过 ${log4j-to-slf4j.version} 管理 -->
    <!-- 优势：避免多个日志框架同时工作导致的冲突和重复日志 -->
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-to-slf4j</artifactId>
    </dependency>
    
    <!-- ==================== 测试依赖 ==================== -->
    
    <!-- JUnit 4: 单元测试框架 -->
    <!-- 作用：提供单元测试功能，用于编写和运行测试用例 -->
    <!-- 版本：4.13.2，只在测试时使用（scope: test） -->
    <!-- 使用场景：编写测试用例验证代码功能 -->
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

**依赖详细说明：**

#### Flink 核心依赖

| 依赖 | 作用 | 主要 API | 使用场景 | 注意事项 |
|-----|------|---------|---------|---------|
| **flink-java** | 批处理 API | DataSet API | 批处理 WordCount、离线分析 | DataSet API 在 Flink 1.12+ 已废弃 |
| **flink-streaming-java_2.12** | 流处理 API | DataStream API | 流处理 WordCount、实时分析 | 依赖 Scala 2.12 版本 |
| **flink-clients_2.12** | 客户端功能 | 本地执行环境 | 本地运行、作业提交 | 必须与 streaming 版本匹配 |

#### 日志管理依赖

| 依赖 | 作用 | 版本 | 说明 |
|-----|------|------|------|
| **slf4j-api** | 日志门面 | 1.7.30 | 统一日志接口，屏蔽实现细节 |
| **slf4j-log4j12** | slf4j→log4j 桥接 | 1.7.30 | 将 slf4j 调用转为 log4j 处理 |
| **log4j-to-slf4j** | log4j→slf4j 桥接 | 2.14.0 | 避免日志冲突，统一日志框架 |

#### 测试依赖

| 依赖 | 作用 | 版本 | 说明 |
|-----|------|------|------|
| **junit** | 单元测试框架 | 4.13.2 | 编写和运行测试用例，仅在测试阶段使用 |

**依赖关系说明：**

```
flink-java (批处理)
    └── 用于批处理数据处理
    └── 提供 DataSet API

flink-streaming-java_2.12 (流处理)
    └── 依赖 Scala 2.12
    └── 用于流处理数据处理
    └── 提供 DataStream API

flink-clients_2.12 (客户端)
    └── 依赖 Scala 2.12
    └── 提供本地执行环境
    └── 提供作业提交功能

slf4j-api (日志门面)
    ├── slf4j-log4j12 (slf4j→log4j)
    ├── log4j-to-slf4j (log4j→slf4j)
    └── 统一日志管理系统

junit (测试框架)
    └── 仅在测试阶段使用
    └── scope: test
```

**版本说明：**

- **Flink 版本**: 1.13.0 (通过 `${flink.version}` 定义)
- **Scala 版本**: 2.12 (通过 `${scala.binary.version}` 定义)
- **Java 版本**: 1.8 (通过 `${java.version}` 定义)
- **日志框架版本**: 
  - SLF4J: 1.7.30
  - Log4j-to-SLF4J: 2.14.0
- **测试框架版本**: JUnit 4.13.2

**为什么需要这些依赖：**

1. **Flink 核心依赖**：
   - `flink-java`: 用于批处理程序（虽然已废弃，但本项目用于演示）
   - `flink-streaming-java_2.12`: 用于流处理程序（主要使用）
   - `flink-clients_2.12`: 提供本地运行和作业提交功能

2. **日志依赖**：
   - 使用 SLF4J 作为日志门面，可以灵活切换底层日志实现
   - 使用 Log4j 作为实际日志输出框架
   - 通过桥接器避免日志冲突，确保日志正常输出

3. **测试依赖**：
   - JUnit 用于编写单元测试
   - scope 设置为 test，只在测试阶段使用

**依赖注意事项：**

1. **Scala 版本必须匹配**：`flink-streaming-java` 和 `flink-clients` 必须使用相同的 Scala 版本（2.12）
2. **日志框架统一**：必须同时引入 `slf4j-api` 和 `slf4j-log4j12`，以及 `log4j-to-slf4j` 来避免冲突
3. **版本管理**：所有版本通过父 POM 的 `<properties>` 统一管理，子模块无需指定版本
4. **DataSet API 废弃**：Flink 1.12+ 已废弃 DataSet API，推荐使用 DataStream API 的批处理模式

### 2. 配置文件

`src/main/resources/log4j.properties`：日志配置

```properties
# 设置日志级别
log4j.rootLogger=INFO, console

# 控制台输出配置
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n

# 设置Flink相关日志级别
log4j.logger.org.apache.flink=INFO
log4j.logger.akka=INFO
log4j.logger.org.apache.kafka=INFO
log4j.logger.org.apache.hadoop=INFO
log4j.logger.org.apache.zookeeper=INFO
```

### 3. 测试数据

`input/words.txt`：测试数据文件

```
hello flink
hello world
hello java
flink is great
java is awesome
hello flink java
```

### 4. 批处理 WordCount（DataSet API）

`src/main/java/com/action/wc/BatchWordCount.java`：使用 DataSet API 实现批处理 WordCount

#### 核心概念

**DataSet API**：Flink 的批处理 API，用于处理有界数据集（批量数据）

**特点：**
- 处理有界数据流（数据集有明确的开始和结束）
- 适用于一次性数据分析任务
- 性能优化更侧重于吞吐量
- Flink 1.12+ 版本中，DataSet API 已标记为废弃，推荐使用 DataStream API 的批处理模式

#### 核心实现详解

```java
package com.action.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink批处理WordCount程序
 * 使用DataSet API实现单词频次统计
 * 
 * 执行流程：
 * 1. 创建批处理执行环境
 * 2. 从文件读取数据
 * 3. 将每一行文本拆分成单词，转换为(word, 1)元组
 * 4. 按单词分组
 * 5. 对每个分组求和
 * 6. 打印结果
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        
        // ==================== 步骤1: 创建批处理执行环境 ====================
        // ExecutionEnvironment 是 Flink 批处理的入口，getExecutionEnvironment() 会返回本地或集群环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // ==================== 步骤2: 从文件读取数据 ====================
        // DataSource<String> 表示数据源是一个字符串数据集
        // 从本地文件读取数据，返回每行文本
        DataSource<String> lineDS = env.readTextFile("flink-01-quickstart/input/words.txt");

        // ==================== 步骤3: 转换数据格式 ====================
        // flatMap 操作：一行文本 -> 多个单词
        // 输入：String（一行文本）
        // 输出：Tuple2<String, Long>（单词, 1）
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    // 将一行文本按空格切分，转换成(word, 1)的二元组
                    // 例如："hello flink" -> ("hello", 1), ("flink", 1)
                    Arrays.stream(line.split(" "))
                            .forEach(word -> out.collect(Tuple2.of(word, 1L)));
                })
                // 当 Lambda 表达式使用 Java 泛型时，由于泛型擦除，需要显式声明类型信息
                // 此处指定返回类型为 (String, Long) 元组
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // ==================== 步骤4: 按照word进行分组 ====================
        // groupBy(0) 表示按照元组的第一个字段（索引0）进行分组
        // 即按照单词进行分组：("hello", 1), ("flink", 1), ("hello", 1) 
        // -> [(hello, [1, 1]), (flink, [1])]
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);

        // ==================== 步骤5: 分组内聚合统计 ====================
        // sum(1) 表示对元组的第二个字段（索引1）求和
        // 即对每个分组的数值部分求和：hello -> 1+1=2
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // ==================== 步骤6: 打印结果 ====================
        sum.print();
        
        /* 
        期望输出：
        (flink,3)
        (world,1)
        (hello,4)
        (awesome,1)
        (great,1)
        (java,3)
        (is,2)
        */
    }
}
```

#### 执行结果解析

```
(flink,3)      # flink 出现3次
(world,1)      # world 出现1次
(hello,4)      # hello 出现4次
(awesome,1)    # awesome 出现1次
(great,1)      # great 出现1次
(java,3)       # java 出现3次
(is,2)         # is 出现2次
```

#### 运行方式

```bash
# 方法1: 使用 Maven 运行
cd flink-01-quickstart
mvn compile exec:java -Dexec.mainClass="com.action.wc.BatchWordCount"

# 方法2: 打包后运行
mvn clean package
java -cp target/flink-01-quickstart-1.0-SNAPSHOT.jar com.action.wc.BatchWordCount

# 方法3: 在 IDE 中直接运行
# 右键 BatchWordCount.java -> Run 'BatchWordCount.main()'
```

### 5. 有界流 WordCount（DataStream API）

`src/main/java/com/action/wc/BoundedStreamWordCount.java`：使用 DataStream API 处理有界数据流

#### 核心概念

**DataStream API**：Flink 的流处理 API，用于处理数据流

**有界流（Bounded Stream）**：
- 流有明确的开始和结束
- 可以被完整处理
- 处理完所有数据后程序结束
- 行为类似于批处理，但使用流处理的 API

#### 核心实现详解

```java
package com.action.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink有界流处理WordCount程序
 * 使用DataStream API处理有界数据流（文件）
 * 
 * 执行流程：
 * 1. 创建流式执行环境
 * 2. 从文件读取数据作为流
 * 3. 将每一行文本拆分成单词，转换为(word, 1)元组
 * 4. 按单词分组
 * 5. 对每个分组求和
 * 6. 打印结果
 * 7. 执行任务
 * 
 * 与批处理的区别：
 * - 使用 StreamExecutionEnvironment 而非 ExecutionEnvironment
 * - 使用 DataStreamSource 而非 DataSource
 * - 使用 keyBy() 而非 groupBy()
 * - 使用 print() 输出结果并需要调用 env.execute()
 * - 需要 env.execute() 触发任务执行
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        
        // ==================== 步骤1: 创建流式执行环境 ====================
        // StreamExecutionEnvironment 是 Flink 流处理的入口
        // getExecutionEnvironment() 会自动判断是本地还是集群环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ==================== 步骤2: 读取文件作为数据流 ====================
        // 从文件读取数据，虽然是流处理 API，但读取的是有界数据
        // DataStreamSource<String> 表示数据源是一个字符串流
        DataStreamSource<String> lineDSS = env.readTextFile("flink-01-quickstart/input/words.txt");

        // ==================== 步骤3: 转换数据格式 ====================
        // flatMap 操作：一行文本 -> 多个单词
        // 输入：String（一行文本）
        // 输出：Tuple2<String, Long>（单词, 1）
        // 
        // 说明：这里的 flatMap 返回的是 Tuple2，需要通过 Collector 收集
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    // 将一行文本按空格切分，收集每个单词
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                // 指定 flatMap 的输出类型为 String
                .returns(Types.STRING)
                // map 操作：将每个单词转换为 (word, 1) 元组
                .map(word -> Tuple2.of(word, 1L))
                // 指定 map 的输出类型为 Tuple2<String, Long>
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        
        // 注意：当使用 Java Lambda 表达式处理泛型类型时，
        // 由于 Java 泛型擦除，Flink 无法自动推断类型信息，需要显式声明

        // ==================== 步骤4: 按照word进行分组 ====================
        // keyBy() 操作：按照 key 对流进行分组
        // t -> t.f0 表示使用元组的第一个字段（单词）作为 key
        // KeyedStream 是一个按 key 分组的流
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // ==================== 步骤5: 分组内聚合统计 ====================
        // sum() 操作：对每个 key 的数值字段求和
        // 参数 1 表示对元组的第二个字段（索引1）求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // ==================== 步骤6: 打印结果 ====================
        // print() 会将结果输出到控制台
        // 输出格式：线程ID> (word, count)
        result.print();

        // ==================== 步骤7: 执行任务 ====================
        // 必须调用 execute() 才能触发任务的执行
        // 对于流处理，这是必需的步骤
        env.execute();
        
        /*
        输出说明：
        - 前面的数字表示线程ID（subtask ID）
        - > 后面的内容表示输出结果
        - 示例输出：
        9> (hello,1)          # 线程9处理，hello第1次出现
        6> (java,1)            # 线程6处理，java第1次出现
        31> (is,1)             # 线程31处理，is第1次出现
        25> (flink,1)          # 线程25处理，flink第1次出现
        17> (world,1)          # 线程17处理，world第1次出现
        5> (awesome,1)         # 线程5处理，awesome第1次出现
        6> (java,2)            # 线程6处理，java第2次出现
        22> (great,1)          # 线程22处理，great第1次出现
        6> (java,3)            # 线程6处理，java第3次出现
        9> (hello,2)           # 线程9处理，hello第2次出现
        9> (hello,3)           # 线程9处理，hello第3次出现
        31> (is,2)             # 线程31处理，is第2次出现
        9> (hello,4)           # 线程9处理，hello第4次出现
        25> (flink,2)          # 线程25处理，flink第2次出现
        25> (flink,3)          # 线程25处理，flink第3次出现
        */
    }
}
```

#### 与批处理的区别

| 特性 | DataSet API (批处理) | DataStream API (流处理) |
|-----|---------------------|----------------------|
| 执行环境 | ExecutionEnvironment | StreamExecutionEnvironment |
| 数据源 | DataSource<T> | DataStreamSource<T> |
| 分组操作 | groupBy() | keyBy() |
| 聚合操作 | sum() | sum() |
| 输出操作 | print() | print() |
| 触发执行 | 自动执行 | 需要 env.execute() |
| 数据特点 | 有界数据 | 有界/无界数据 |
| 处理方式 | 一次处理全部数据 | 持续处理数据流 |

#### 运行方式

```bash
# 方法1: 使用 Maven 运行
cd flink-01-quickstart
mvn compile exec:java -Dexec.mainClass="com.action.wc.BoundedStreamWordCount"

# 方法2: 打包后运行
mvn clean package
java -cp target/flink-01-quickstart-1.0-SNAPSHOT.jar com.action.wc.BoundedStreamWordCount

# 方法3: 在 IDE 中直接运行
# 右键 BoundedStreamWordCount.java -> Run 'BoundedStreamWordCount.main()'
```

### 6. 无界流 WordCount（DataStream API）

`src/main/java/com/action/wc/StreamWordCount.java`：使用 DataStream API 处理无界数据流

#### 核心概念

**无界流（Unbounded Stream）**：

- 流没有明确的结束
- 持续接收数据并处理
- 需要实时或近实时处理
- 典型的流处理场景

**Socket Text Stream**：

- 从网络 socket 接收数据
- 实时数据流
- 需要外部数据源持续发送数据

#### 核心实现详解

```java
package com.action.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink无界流处理WordCount程序
 * 使用DataStream API处理无界数据流（socket文本流）
 * 
 * 执行流程：
 * 1. 创建流式执行环境
 * 2. 从 socket 读取文本流
 * 3. 将每一行文本拆分成单词，转换为(word, 1)元组
 * 4. 按单词分组
 * 5. 对每个分组求和
 * 6. 打印结果
 * 7. 执行任务
 * 
 * 与有界流的区别：
 * - 数据源是 socket 而非文件
 * - 流是持续不断的（无界）
 * - 需要外部程序持续发送数据
 * - 程序不会自动结束，需要手动停止
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        
        // ==================== 步骤1: 创建流式执行环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ==================== 步骤2: 读取文本流 ====================
        // socketTextStream() 从网络 socket 读取数据流
        // 参数：host（主机地址），port（端口号）
        // 这会创建一个无界流，持续接收数据
        DataStreamSource<String> lineDSS = env.socketTextStream("192.168.56.11", 7777);
        
        /* 
        数据源准备（在 Linux 服务器上）：
        1. 安装 nc 命令：
           sudo yum -y install nc   # CentOS/RedHat
           sudo apt-get install netcat-openbsd   # Ubuntu/Debian
        
        2. 启动 nc 监听端口：
           nc -lk 7777
        
        3. 输入测试数据（每条消息后按回车）：
           abc
           bbc
           abc
           aaa
           abc
        
        4. 观察 Flink 程序的输出
        */

        // ==================== 步骤3: 转换数据格式 ====================
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    // 将一行文本按空格切分，收集每个单词
                    // 注意：这里的正则表达式可以根据实际需求调整
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                // 指定类型信息
                .returns(Types.STRING)
                // 将每个单词转换为 (word, 1) 元组
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        
        // 说明：当 Lambda 表达式使用 Java 泛型时，由于泛型擦除，
        // Flink 需要显式声明类型信息才能正确序列化和执行

        // ==================== 步骤4: 按照word进行分组 ====================
        // keyBy() 操作会将相同的 key 路由到同一个 subtask 处理
        // t -> t.f0 表示使用元组的第一个字段作为 key
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // ==================== 步骤5: 分组内聚合统计 ====================
        // sum() 操作会对每个 key 的值进行累加
        // 参数 1 表示对元组的第二个字段求和
        // 流式处理的 sum 是增量更新，每次收到数据都会更新计数
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // ==================== 步骤6: 打印结果 ====================
        // 实时打印结果，每次收到数据都会输出
        result.print();

        // ==================== 步骤7: 执行任务 ====================
        // execute() 会启动流处理任务
        // 任务会持续运行，直到手动停止或异常退出
        env.execute();

        /*
        控制台输出示例：
        # 输入 "abc"
        29> (abc,1)
        
        # 输入 "bbc"
        20> (bbc,1)
        
        # 输入 "abc"
        29> (abc,2)   # abc 的计数从 1 增加到了 2
        
        # 输入 "aaa"
        20> (aaa,1)
        
        # 输入 "abc"
        29> (abc,3)   # abc 的计数从 2 增加到了 3
        
        输出说明：
        - 29、20 等数字表示 subtask ID
        - abc 会始终被路由到同一个 subtask（29）处理
        - 每次相同单词出现时，计数会累加
        */
    }
}
```

#### 测试步骤

**1. 启动 Flink 程序**

```bash
# 在项目根目录执行
cd flink-01-quickstart
mvn compile exec:java -Dexec.mainClass="com.action.wc.StreamWordCount"
```

**2. 准备数据源（在 Linux 服务器上）**

```bash
# 连接到远程服务器
ssh user@192.168.56.11

# 安装 nc 工具（如果没有）
sudo yum -y install nc
# 或
sudo apt-get install netcat-openbsd

# 启动 nc 监听 7777 端口
nc -lk 7777
```

**3. 发送测试数据**

在 nc 终端中输入以下数据（每条消息后按回车）：

```
abc
bbc
abc
aaa
abc
```

**4. 观察输出**

Flink 程序控制台会实时输出：

```
# 输入 "abc" 后
29> (abc,1)

# 输入 "bbc" 后
20> (bbc,1)

# 输入 "abc" 后
29> (abc,2)

# 输入 "aaa" 后
20> (aaa,1)

# 输入 "abc" 后
29> (abc,3)
```

**5. 停止程序**

- 在 IDE 中点击停止按钮
- 或在终端中按 `Ctrl+C`

**6. 停止数据源**

在 nc 终端中按 `Ctrl+C` 停止监听

#### 运行方式

```bash
# 方法1: 使用 Maven 运行
cd flink-01-quickstart
mvn compile exec:java -Dexec.mainClass="com.action.wc.StreamWordCount"

# 方法2: 打包后运行
mvn clean package
java -cp target/flink-01-quickstart-1.0-SNAPSHOT.jar com.action.wc.StreamWordCount

# 方法3: 在 IDE 中直接运行
# 右键 StreamWordCount.java -> Run 'StreamWordCount.main()'
```

## Flink 处理模式对比

### 1. 三种模式对比

| 特性 | 批处理 (DataSet API) | 有界流 (DataStream API) | 无界流 (DataStream API) |
|-----|---------------------|----------------------|----------------------|
| **数据特征** | 有界 | 有界 | 无界 |
| **结束标志** | 数据读取完自动结束 | 数据读取完自动结束 | 需要手动停止 |
| **执行环境** | ExecutionEnvironment | StreamExecutionEnvironment | StreamExecutionEnvironment |
| **数据源类型** | DataSource | DataStreamSource | DataStreamSource |
| **数据源** | 文件 | 文件 | Socket/Kafka 等 |
| **分组操作** | groupBy() | keyBy() | keyBy() |
| **处理延迟** | 高（等待全部数据） | 高（等待全部数据） | 低（实时处理） |
| **吞吐量** | 高 | 中 | 中 |
| **适用场景** | 离线分析、历史数据处理 | 数据导入、批量转换 | 实时监控、实时分析 |
| **API 状态** | 已废弃（1.12+） | 推荐使用 | 推荐使用 |

### 2. DataSet API vs DataStream API

#### 相似点

- 都支持读取文件
- 都有相似的转换操作（map、flatMap、filter 等）
- 都需要分组和聚合

#### 不同点

| 方面 | DataSet API | DataStream API |
|-----|------------|---------------|
| **API 类型** | 批处理 API | 流处理 API |
| **执行环境** | ExecutionEnvironment | StreamExecutionEnvironment |
| **数据源** | DataSource<T> | DataStreamSource<T> |
| **分组** | groupBy() | keyBy() |
| **时间概念** | 无 | 支持事件时间、处理时间、摄入时间 |
| **窗口操作** | 不支持 | 支持滑动窗口、滚动窗口等 |
| **状态管理** | 无状态 | 支持有状态操作 |
| **一致性保证** | 最终一致性 | 精确一次或至少一次 |
| **未来** | 已废弃 | 继续发展 |

### 3. 核心操作对比

#### 数据读取

```java
// DataSet API - 批处理
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
DataSource<String> dataSource = env.readTextFile("path/to/file");

// DataStream API - 有界流
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<String> dataStream = env.readTextFile("path/to/file");

// DataStream API - 无界流
DataStreamSource<String> dataStream = env.socketTextStream("host", port);
```

#### 分组操作

```java
// DataSet API - 批处理
UnsortedGrouping<Tuple2<String, Long>> group = data.groupBy(0);

// DataStream API - 流处理
KeyedStream<Tuple2<String, Long>, String> keyed = data.keyBy(t -> t.f0);
```

## 测试方法

### 1. 环境准备

**系统要求：**
- JDK 1.8 或更高版本
- Maven 3.6+ 
- Linux 系统（用于 Socket 数据源测试，可选）

**依赖安装：**

```bash
# 检查 JDK 版本
java -version

# 检查 Maven 版本
mvn -version

# 安装 nc 工具（Linux，用于 socket 测试）
sudo yum -y install nc        # CentOS/RedHat
sudo apt-get install netcat-openbsd   # Ubuntu/Debian
```

### 2. 运行批处理 WordCount

```bash
# 进入项目目录
cd flink-01-quickstart

# 方法1: 使用 Maven 直接运行
mvn compile exec:java -Dexec.mainClass="com.action.wc.BatchWordCount"

# 方法2: 打包后运行
mvn clean package
java -cp target/flink-01-quickstart-1.0-SNAPSHOT.jar com.action.wc.BatchWordCount

# 方法3: IDE 运行
# 右键 BatchWordCount.java -> Run 'BatchWordCount.main()'
```

**预期输出：**
```
(flink,3)
(world,1)
(hello,4)
(awesome,1)
(great,1)
(java,3)
(is,2)
```

### 3. 运行有界流 WordCount

```bash
cd flink-01-quickstart

# 方法1: 使用 Maven 直接运行
mvn compile exec:java -Dexec.mainClass="com.action.wc.BoundedStreamWordCount"

# 方法2: 打包后运行
mvn clean package
java -cp target/flink-01-quickstart-1.0-SNAPSHOT.jar com.action.wc.BoundedStreamWordCount

# 方法3: IDE 运行
# 右键 BoundedStreamWordCount.java -> Run 'BoundedStreamWordCount.main()'
```

**预期输出：**
```
9> (hello,1)
6> (java,1)
31> (is,1)
25> (flink,1)
17> (world,1)
5> (awesome,1)
6> (java,2)
22> (great,1)
6> (java,3)
9> (hello,2)
9> (hello,3)
31> (is,2)
9> (hello,4)
25> (flink,2)
25> (flink,3)
```

### 4. 运行无界流 WordCount

**步骤1：启动 Flink 程序**

```bash
cd flink-01-quickstart
mvn compile exec:java -Dexec.mainClass="com.action.wc.StreamWordCount"
```

**步骤2：启动数据源（在远程服务器）**

```bash
# 连接到服务器
ssh user@192.168.56.11

# 启动 nc 监听
nc -lk 7777
```

**步骤3：发送测试数据**

在 nc 终端中输入（每条消息后按回车）：
```
abc
bbc
abc
aaa
abc
```

**步骤4：观察输出**

Flink 程序会实时输出处理结果：
```
29> (abc,1)
20> (bbc,1)
29> (abc,2)
20> (aaa,1)
29> (abc,3)
```

**步骤5：停止程序**

- 在 IDE 中点击停止按钮
- 或在终端中按 `Ctrl+C`

## 核心概念详解

### 1. Lambda 表达式与泛型擦除

**问题：** 为什么需要显式声明类型信息？

```java
// 问题：由于 Java 泛型擦除，Flink 无法推断 Lambda 的类型
lineDSS.flatMap((String line, Collector<String> words) -> {
    Arrays.stream(line.split(" ")).forEach(words::collect);
})
.returns(Types.STRING)  // 必须显式声明返回类型
```

**原因：**
- Java 的泛型在编译时会被擦除
- Flink 需要类型信息进行序列化和优化
- 需要在运行时保留类型信息

**解决方案：**
- 使用 `.returns(Types.STRING)` 显式声明类型
- 或使用具体的类而非泛型
- 或使用 TypeInformation 子类

### 2. KeyBy 与 GroupBy

**KeyBy：**
- DataStream API 的分组操作
- 对流进行逻辑分区
- 相同 key 的数据路由到同一个 subtask
- 用于状态管理和分布式处理

**GroupBy：**
- DataSet API 的分组操作
- 对数据集进行分组
- 适用于批处理
- 数据需要在同一个算子内

**相似点：**
- 都是按 key 分组
- 后续都可以聚合
- 都会影响数据分布

### 3. Sum 聚合操作

**DataSet API：**
```java
AggregateOperator<Tuple2<String, Long>> sum = group.sum(1);
```

**DataStream API：**
```java
SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyed.sum(1);
```

**参数说明：**
- `sum(1)` 中的 `1` 表示元组的第二个字段（索引从 0 开始）
- `Tuple2<String, Long>` 中 `f0` 是 String（索引 0），`f1` 是 Long（索引 1）

**聚合特点：**
- 批处理：一次性计算所有数据的总和
- 流处理：增量更新，每次收到数据都会更新计数

## 注意事项

### 1. 文件路径问题

**相对路径 vs 绝对路径**

```java
// 相对路径：相对于当前工作目录
env.readTextFile("flink-01-quickstart/input/words.txt")

// 绝对路径：完整路径
env.readTextFile("/home/user/project/flink-01-quickstart/input/words.txt")
```

**注意事项：**
- 确保文件路径正确
- 如果从 IDE 运行，工作目录通常是项目根目录
- 如果打包运行，需要指定正确的文件路径

### 2. Socket 连接问题

**连接失败：**

```java
// 错误示例：无法连接到主机
env.socketTextStream("192.168.56.11", 7777);
```

**解决方案：**
- 确保目标主机可访问
- 确保端口已开放
- 使用 `nc -l 7777` 监听端口
- 检查防火墙设置

**测试连接：**
```bash
# 测试连接
telnet 192.168.56.11 7777
# 或
nc -zv 192.168.56.11 7777
```

### 3. 程序不结束问题

**无界流处理：**
- 无界流程序不会自动结束
- 需要手动停止（IDE 停止按钮或 Ctrl+C）
- Socket 断开连接后，Flink 会重连或抛出异常

**有界流处理：**
- 有界流会读取完所有数据后结束
- 不需要手动停止

### 4. 并行度设置

**默认并行度：**
```java
// Flink 默认并行度等于 CPU 核心数
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置并行度
env.setParallelism(4);
```

**并行度影响：**
- 数据会被分发到多个 subtask
- 影响性能和数据分布
- 相同 key 的数据始终发送到同一个 subtask

### 5. 依赖版本

**Scala 版本：**
- Flink 1.13.0 使用 Scala 2.12
- 必须匹配 `flink-streaming-java_2.12`
- 不同版本不兼容

**Java 版本：**
- Flink 1.13 需要 Java 8+
- 推荐使用 Java 8 或 11

### 6. 类型信息

**必须显式声明类型的情况：**
- 使用 Lambda 表达式
- 返回泛型类型
- 涉及复杂类型

**不需要声明类型的情况：**
- 使用具体类而非泛型
- 返回简单类型（String、Integer 等）
- 使用非泛型的映射函数

### 7. 输出格式说明

**批处理输出：**
```
(flink,3)
(world,1)
(hello,4)
```

**流处理输出：**
```
29> (abc,1)    # 29 是 subtask ID
20> (bbc,1)    # 20 是 subtask ID
29> (abc,2)    # abc 始终在 subtask 29 处理
```

**输出说明：**
- 前面的数字是 subtask ID（subtask ID）
- `>` 后面是输出内容
- 相同 key 的数据会路由到相同的 subtask

### 8. Maven 配置

**exec-maven-plugin 配置：**

查看 `pom.xml` 中的配置：
```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <version>3.1.0</version>
    <configuration>
        <mainClass>com.action.wc.BatchWordCount</mainClass>
    </configuration>
</plugin>
```

**切换主类：**
```bash
# 运行不同的主类
mvn exec:java -Dexec.mainClass="com.action.wc.StreamWordCount"
mvn exec:java -Dexec.mainClass="com.action.wc.BoundedStreamWordCount"
```

## 快速参考

### 代码位置

- **批处理**：`src/main/java/com/action/wc/BatchWordCount.java`
- **有界流**：`src/main/java/com/action/wc/BoundedStreamWordCount.java`
- **无界流**：`src/main/java/com/action/wc/StreamWordCount.java`

### 运行命令

```bash
# 批处理
mvn compile exec:java -Dexec.mainClass="com.action.wc.BatchWordCount"

# 有界流
mvn compile exec:java -Dexec.mainClass="com.action.wc.BoundedStreamWordCount"

# 无界流
mvn compile exec:java -Dexec.mainClass="com.action.wc.StreamWordCount"
```

### 数据源

- **文件**：`input/words.txt`
- **Socket**：`192.168.56.11:7777`（示例）

### 关键 API

- **批处理环境**：`ExecutionEnvironment.getExecutionEnvironment()`
- **流处理环境**：`StreamExecutionEnvironment.getExecutionEnvironment()`
- **分组**：`groupBy()`（批处理） / `keyBy()`（流处理）
- **聚合**：`sum()`
- **输出**：`print()`
- **执行**：`env.execute()`（仅流处理需要）

## 学习建议

### 1. 入门路径

1. **先学习批处理**：理解基本概念和 API
2. **再学习有界流**：了解流处理思想
3. **最后学习无界流**：掌握实时处理

### 2. 实践建议

- 修改输入数据，观察输出变化
- 修改并行度，观察输出差异
- 添加自己的转换操作
- 尝试不同的数据源

### 3. 进阶学习

- 学习时间语义（事件时间、处理时间）
- 学习窗口操作（滚动窗口、滑动窗口）
- 学习状态管理（KeyedState、OperatorState）
- 学习连接器和状态后端
- 学习 Flink SQL

## 常见问题

### Q1: 程序报错 "Could not find or load main class"

**原因：** 类路径问题

**解决方案：**

```bash
# 确保编译成功
mvn clean compile

# 检查类文件是否存在
ls -la target/classes/com/action/wc/

# 运行完整路径
java -cp target/classes com.action.wc.BatchWordCount
```

### Q2: Socket 连接失败

**原因：** 网络或权限问题

**解决方案：**

```bash
# 检查端口是否被占用
netstat -tuln | grep 7777

# 测试连接
telnet host port

# 使用 netcat 测试
nc -zv host port
```

### Q3: 泛型擦除错误

**原因：** 未显式声明类型

**解决方案：**

```java
// 添加 .returns() 声明类型
.returns(Types.STRING)
.returns(Types.TUPLE(Types.STRING, Types.LONG))
```

### Q4: 输出乱码

**原因：** 编码问题

**解决方案：**

- 确保文件使用 UTF-8 编码
- 检查 IDE 编码设置
- 使用命令行运行

### Q5: 程序不结束

**原因：** 无界流程序

**解决方案：**

- 对于有界流，程序会自动结束
- 对于无界流，需要手动停止
- 在 IDE 中点击停止按钮
- 或在终端按 `Ctrl+C`

## 总结

本项目通过三个经典的 WordCount 程序，展示了 Flink 的三种核心处理模式：

1. **批处理（DataSet API）**：处理有界数据集，适合离线分析
2. **有界流（DataStream API）**：用流处理 API 处理有界数据，理解流处理思想
3. **无界流（DataStream API）**：处理实时数据流，适合实时监控和分析

## 参考资料

- [Flink 官方文档 - 快速开始](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/overview/)
- [Flink 官方文档 - 批处理与流处理](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/concepts/overview/)
- [Flink 官方文档 - DataStream API](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/overview/)
- [Flink 官方文档 - 执行环境](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/execution_environment/)

