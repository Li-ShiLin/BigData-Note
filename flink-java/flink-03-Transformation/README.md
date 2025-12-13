<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 转换算子（Transformation）演示](#flink-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90transformation%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event.java - 事件数据模型](#321-eventjava---%E4%BA%8B%E4%BB%B6%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.2 ClickSource.java - 自定义数据源](#322-clicksourcejava---%E8%87%AA%E5%AE%9A%E4%B9%89%E6%95%B0%E6%8D%AE%E6%BA%90)
    - [3.3 Map 转换算子](#33-map-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
    - [3.4 Filter 转换算子](#34-filter-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
    - [3.5 FlatMap 转换算子](#35-flatmap-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
      - [3.5.1 FlatMap 字符串分词示例](#351-flatmap-%E5%AD%97%E7%AC%A6%E4%B8%B2%E5%88%86%E8%AF%8D%E7%A4%BA%E4%BE%8B)
      - [3.5.2 FlatMap 事件拆分示例](#352-flatmap-%E4%BA%8B%E4%BB%B6%E6%8B%86%E5%88%86%E7%A4%BA%E4%BE%8B)
      - [3.5.3 FlatMap 条件过滤和转换示例](#353-flatmap-%E6%9D%A1%E4%BB%B6%E8%BF%87%E6%BB%A4%E5%92%8C%E8%BD%AC%E6%8D%A2%E7%A4%BA%E4%BE%8B)
      - [3.5.4 FlatMap 嵌套数据展开示例](#354-flatmap-%E5%B5%8C%E5%A5%97%E6%95%B0%E6%8D%AE%E5%B1%95%E5%BC%80%E7%A4%BA%E4%BE%8B)
      - [3.5.5 FlatMap URL 解析示例](#355-flatmap-url-%E8%A7%A3%E6%9E%90%E7%A4%BA%E4%BE%8B)
      - [3.5.6 FlatMap 字符拆分示例](#356-flatmap-%E5%AD%97%E7%AC%A6%E6%8B%86%E5%88%86%E7%A4%BA%E4%BE%8B)
    - [3.6 KeyBy 按键分区](#36-keyby-%E6%8C%89%E9%94%AE%E5%88%86%E5%8C%BA)
      - [3.6.1 KeyBy 基础概念](#361-keyby-%E5%9F%BA%E7%A1%80%E6%A6%82%E5%BF%B5)
      - [3.6.2 KeyBy 基础用法示例](#362-keyby-%E5%9F%BA%E7%A1%80%E7%94%A8%E6%B3%95%E7%A4%BA%E4%BE%8B)
      - [3.6.3 KeyBy 简单聚合示例](#363-keyby-%E7%AE%80%E5%8D%95%E8%81%9A%E5%90%88%E7%A4%BA%E4%BE%8B)
      - [3.6.4 KeyBy 多字段组合键示例](#364-keyby-%E5%A4%9A%E5%AD%97%E6%AE%B5%E7%BB%84%E5%90%88%E9%94%AE%E7%A4%BA%E4%BE%8B)
      - [3.6.5 KeyBy 统计用户访问次数示例](#365-keyby-%E7%BB%9F%E8%AE%A1%E7%94%A8%E6%88%B7%E8%AE%BF%E9%97%AE%E6%AC%A1%E6%95%B0%E7%A4%BA%E4%BE%8B)
      - [3.6.6 KeyBy 分组后计算总访问时长示例](#366-keyby-%E5%88%86%E7%BB%84%E5%90%8E%E8%AE%A1%E7%AE%97%E6%80%BB%E8%AE%BF%E9%97%AE%E6%97%B6%E9%95%BF%E7%A4%BA%E4%BE%8B)
      - [3.6.7 KeyBy 按 URL 类型分组示例](#367-keyby-%E6%8C%89-url-%E7%B1%BB%E5%9E%8B%E5%88%86%E7%BB%84%E7%A4%BA%E4%BE%8B)
    - [3.7 UDF 自定义函数](#37-udf-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%87%BD%E6%95%B0)
    - [3.8 Reduce 转换算子](#38-reduce-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
      - [3.8.1 Reduce 基础概念](#381-reduce-%E5%9F%BA%E7%A1%80%E6%A6%82%E5%BF%B5)
      - [3.8.2 Reduce 统计用户PV并找出最大PV用户示例](#382-reduce-%E7%BB%9F%E8%AE%A1%E7%94%A8%E6%88%B7pv%E5%B9%B6%E6%89%BE%E5%87%BA%E6%9C%80%E5%A4%A7pv%E7%94%A8%E6%88%B7%E7%A4%BA%E4%BE%8B)
      - [3.8.3 Reduce 求和归约示例](#383-reduce-%E6%B1%82%E5%92%8C%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
      - [3.8.4 Reduce 最大值归约示例](#384-reduce-%E6%9C%80%E5%A4%A7%E5%80%BC%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
      - [3.8.5 Reduce 最小值归约示例](#385-reduce-%E6%9C%80%E5%B0%8F%E5%80%BC%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
      - [3.8.6 Reduce 平均值归约示例](#386-reduce-%E5%B9%B3%E5%9D%87%E5%80%BC%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
      - [3.8.7 Reduce 字符串拼接归约示例](#387-reduce-%E5%AD%97%E7%AC%A6%E4%B8%B2%E6%8B%BC%E6%8E%A5%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
      - [3.8.8 Reduce 自定义对象归约示例](#388-reduce-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%AF%B9%E8%B1%A1%E5%BD%92%E7%BA%A6%E7%A4%BA%E4%BE%8B)
    - [3.9 Tuple 聚合算子](#39-tuple-%E8%81%9A%E5%90%88%E7%AE%97%E5%AD%90)
    - [3.10 POJO 聚合算子](#310-pojo-%E8%81%9A%E5%90%88%E7%AE%97%E5%AD%90)
    - [3.11 返回类型处理](#311-%E8%BF%94%E5%9B%9E%E7%B1%BB%E5%9E%8B%E5%A4%84%E7%90%86)
    - [3.12 Rich Function 富函数](#312-rich-function-%E5%AF%8C%E5%87%BD%E6%95%B0)
    - [3.13 物理分区](#313-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA)
      - [3.13.1 物理分区基础概念](#3131-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA%E5%9F%BA%E7%A1%80%E6%A6%82%E5%BF%B5)
      - [3.13.2 物理分区综合示例](#3132-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA%E7%BB%BC%E5%90%88%E7%A4%BA%E4%BE%8B)
      - [3.13.3 物理分区 - shuffle随机分区](#3133-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---shuffle%E9%9A%8F%E6%9C%BA%E5%88%86%E5%8C%BA)
      - [3.13.4 物理分区 - rebalance轮询分区](#3134-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---rebalance%E8%BD%AE%E8%AF%A2%E5%88%86%E5%8C%BA)
      - [3.13.5 物理分区 - rescale重缩放分区](#3135-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---rescale%E9%87%8D%E7%BC%A9%E6%94%BE%E5%88%86%E5%8C%BA)
      - [3.13.6 物理分区 - broadcast广播分区](#3136-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---broadcast%E5%B9%BF%E6%92%AD%E5%88%86%E5%8C%BA)
      - [3.13.7 物理分区 - global全局分区](#3137-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---global%E5%85%A8%E5%B1%80%E5%88%86%E5%8C%BA)
      - [3.13.8 物理分区 - 自定义分区示例](#3138-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA---%E8%87%AA%E5%AE%9A%E4%B9%89%E5%88%86%E5%8C%BA%E7%A4%BA%E4%BE%8B)
  - [4. 转换算子类型对比](#4-%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90%E7%B1%BB%E5%9E%8B%E5%AF%B9%E6%AF%94)
    - [4.1 基本转换算子](#41-%E5%9F%BA%E6%9C%AC%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
    - [4.2 KeyBy 按键分区](#42-keyby-%E6%8C%89%E9%94%AE%E5%88%86%E5%8C%BA)
    - [4.3 聚合转换算子](#43-%E8%81%9A%E5%90%88%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
    - [4.4 分区转换算子](#44-%E5%88%86%E5%8C%BA%E8%BD%AC%E6%8D%A2%E7%AE%97%E5%AD%90)
    - [4.5 函数类型对比](#45-%E5%87%BD%E6%95%B0%E7%B1%BB%E5%9E%8B%E5%AF%B9%E6%AF%94)
  - [5. 核心概念详解](#5-%E6%A0%B8%E5%BF%83%E6%A6%82%E5%BF%B5%E8%AF%A6%E8%A7%A3)
    - [5.1 MapFunction 接口](#51-mapfunction-%E6%8E%A5%E5%8F%A3)
    - [5.2 FilterFunction 接口](#52-filterfunction-%E6%8E%A5%E5%8F%A3)
    - [5.3 FlatMapFunction 接口](#53-flatmapfunction-%E6%8E%A5%E5%8F%A3)
    - [5.4 KeySelector 接口](#54-keyselector-%E6%8E%A5%E5%8F%A3)
    - [5.6 ReduceFunction 接口](#56-reducefunction-%E6%8E%A5%E5%8F%A3)
    - [5.7 Rich Function 接口](#57-rich-function-%E6%8E%A5%E5%8F%A3)
    - [5.8 物理分区策略](#58-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5)
  - [6. 常见问题](#6-%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
    - [6.1 Lambda 表达式类型推断问题](#61-lambda-%E8%A1%A8%E8%BE%BE%E5%BC%8F%E7%B1%BB%E5%9E%8B%E6%8E%A8%E6%96%AD%E9%97%AE%E9%A2%98)
    - [6.2 Reduce 函数要求](#62-reduce-%E5%87%BD%E6%95%B0%E8%A6%81%E6%B1%82)
    - [6.3 聚合函数区别](#63-%E8%81%9A%E5%90%88%E5%87%BD%E6%95%B0%E5%8C%BA%E5%88%AB)
    - [6.4 物理分区选择](#64-%E7%89%A9%E7%90%86%E5%88%86%E5%8C%BA%E9%80%89%E6%8B%A9)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 转换算子（Transformation）演示

## 1. 项目作用

本项目演示了 Flink 支持的各种转换算子（Transformation），包括 Map、Filter、FlatMap、KeyBy、Reduce、Aggregation、Physical
Partitioning、Rich Function、UDF、Return Type 等。通过实际代码示例帮助开发者快速掌握 Flink 转换算子的创建和使用方法。

## 2. 项目结构

```
flink-03-Transformation/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源实现
│   └── transformation/
│       ├── map/
│       │   └── TransMapTest.java                        # Map 转换算子示例
│       ├── filter/
│       │   └── TransFilterTest.java                     # Filter 转换算子示例
│       ├── flatmap/
│       │   ├── TransFlatmapTest.java                    # FlatMap 转换算子示例（基础）
│       │   ├── TransFlatmapWordSplitTest.java           # FlatMap 字符串分词示例
│       │   ├── TransFlatmapEventSplitTest.java          # FlatMap 事件拆分示例
│       │   ├── TransFlatmapConditionalTest.java        # FlatMap 条件过滤和转换示例
│       │   ├── TransFlatmapNestedDataTest.java          # FlatMap 嵌套数据展开示例
│       │   ├── TransFlatmapUrlParseTest.java            # FlatMap URL 解析示例
│       │   └── TransFlatmapCharSplitTest.java           # FlatMap 字符拆分示例
│       ├── keyBy/
│       │   ├── TransKeyBySimpleAggTest.java             # KeyBy 简单聚合示例
│       │   ├── TransKeyByBasicTest.java                  # KeyBy 基础用法示例
│       │   ├── TransKeyByCompositeKeyTest.java          # KeyBy 多字段组合键示例
│       │   ├── TransKeyByCountTest.java                 # KeyBy 统计用户访问次数示例
│       │   ├── TransKeyByAvgTest.java                  # KeyBy 分组后计算总访问时长示例
│       │   └── TransKeyByUrlTypeTest.java              # KeyBy 按 URL 类型分组示例
│       ├── udf/
│       │   └── TransUdfTest.java                       # UDF 自定义函数示例
│       ├── reduce/
│       │   ├── TransReduceTest.java                    # Reduce 转换算子示例（统计用户PV并找出最大PV用户）
│       │   ├── TransReduceSumTest.java                 # Reduce 求和归约示例
│       │   ├── TransReduceMaxTest.java                # Reduce 最大值归约示例
│       │   ├── TransReduceMinTest.java                # Reduce 最小值归约示例
│       │   ├── TransReduceAvgTest.java                # Reduce 平均值归约示例
│       │   ├── TransReduceStringConcatTest.java       # Reduce 字符串拼接归约示例
│       │   └── TransReduceCustomObjectTest.java       # Reduce 自定义对象归约示例
│       ├── TransTupleAggreationTest.java               # Tuple 聚合算子示例
│       ├── TransPojoAggregationTest.java               # POJO 聚合算子示例
│       ├── TransReturnTypeTest.java                     # 返回类型处理示例
│       ├── richFunction/
│       │   └── TransRichFunctionTest.java              # Rich Function 示例
│       └── physicalPartition/
│           ├── TransPhysicalPatitioningTest.java       # 物理分区综合示例
│           ├── TransPhysicalPartitionShuffleTest.java  # 物理分区 - 随机分区示例
│           ├── TransPhysicalPartitionRebalanceTest.java # 物理分区 - 轮询分区示例
│           ├── TransPhysicalPartitionRescaleTest.java  # 物理分区 - 重缩放分区示例
│           ├── TransPhysicalPartitionBroadcastTest.java # 物理分区 - 广播分区示例
│           ├── TransPhysicalPartitionGlobalTest.java   # 物理分区 - 全局分区示例
│           └── TransPhysicalPartitionCustomTest.java  # 物理分区 - 自定义分区示例
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

### 3.3 Map 转换算子

`src/main/java/com/action/transformation/map/TransMapTest.java`：使用 `map()` 方法对数据流中的每个元素进行一对一转换

```java
package com.action.transformation.map;


import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Map 转换算子示例
 * 使用 map() 方法对数据流中的每个元素进行一对一转换
 * <p>
 * 特点说明：
 * - 功能：对数据流中的每个元素进行一对一转换
 * - 输入输出：一个输入元素对应一个输出元素
 * - 实现方式：实现 MapFunction<T, O> 接口，其中 T 是输入类型，O 是输出类型
 * - 使用场景：数据格式转换、字段提取、类型转换等
 * <p>
 * 预期输出：
 * Mary
 * Bob
 */
public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 进行转换计算，提取user字段
        // 1，使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new UserExtractor());

//        result1.print();

        //2．使用匿名类实现MapFunction接口
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });

        // 传入MapFunction的实现类
        stream.map(new UserExtractor()).print();

        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}
```

### 3.4 Filter 转换算子

`src/main/java/com/action/transformation/filter/TransFilterTest.java`：使用 `filter()` 方法对数据流中的元素进行过滤

```java
package com.action.transformation.filter;


import com.action.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Filter 转换算子示例
 * 使用 filter() 方法对数据流中的元素进行过滤
 * <p>
 * 特点说明：
 * - 功能：对数据流中的元素进行过滤，只保留满足条件的元素
 * - 输入输出：一个输入元素可能对应零个或一个输出元素
 * - 实现方式：实现 FilterFunction<T> 接口，返回 true 表示保留，false 表示过滤
 * - 使用场景：数据清洗、条件筛选、异常数据过滤等
 * <p>
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 */
public class TransFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 传入匿名类实现FilterFunction
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event e) throws Exception {
                return e.user.equals("Mary");
            }
        });

        // 传入FilterFunction实现类
        stream.filter(new UserFilter()).print();

        env.execute();
    }

    public static class UserFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event e) throws Exception {
            return e.user.equals("Mary");
        }
    }
}
```

### 3.5 FlatMap 转换算子

`src/main/java/com/action/transformation/flatmap/TransFlatmapTest.java`：使用 `flatMap()` 方法对数据流中的每个元素进行一对多转换

```java
package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 转换算子示例
 * 使用 flatMap() 方法对数据流中的每个元素进行一对多转换
 * <p>
 * 特点说明：
 * - 功能：对数据流中的每个元素进行一对多转换，可以输出零个、一个或多个元素
 * - 输入输出：一个输入元素可以对应零个、一个或多个输出元素
 * - 实现方式：实现 FlatMapFunction<T, O> 接口，通过 Collector 收集输出元素
 * - 使用场景：数据拆分、字符串分词、一对多转换等
 * <p>
 * 预期输出：
 * Mary
 * Bob
 * ./cart
 */
public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}
```

#### 3.5.1 FlatMap 字符串分词示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapWordSplitTest.java`：使用 `flatMap()` 方法将句子拆分成单词

```java
package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 字符串分词示例
 * 使用 flatMap() 方法将句子拆分成单词
 * <p>
 * 特点说明：
 * - 功能：将输入的句子字符串拆分成多个单词
 * - 输入输出：一个句子字符串对应多个单词字符串
 * - 实现方式：使用 split() 方法分割字符串，然后通过 Collector 收集每个单词
 * - 使用场景：文本处理、日志分析、关键词提取等
 * <p>
 * 预期输出：
 * hello
 * world
 * hello
 * flink
 * hello
 * java
 * apache
 * flink
 * is
 * great
 */
public class TransFlatmapWordSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements(
                "hello world",
                "hello flink",
                "hello java",
                "apache flink is great"
        );

        // 将每个句子拆分成单词
        stream.flatMap(new WordSplitter()).print();

        env.execute();
    }

    public static class WordSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> out) throws Exception {
            // 按空格分割句子，得到单词数组
            String[] words = sentence.split("\\s+");
            // 遍历每个单词，收集到输出流
            for (String word : words) {
                if (!word.isEmpty()) {
                    out.collect(word);
                }
            }
        }
    }
}
```

#### 3.5.2 FlatMap 事件拆分示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapEventSplitTest.java`：使用 `flatMap()` 方法将一个事件对象拆分成多个字段值

```java
package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 事件拆分示例
 * 使用 flatMap() 方法将一个事件对象拆分成多个字段值
 * <p>
 * 特点说明：
 * - 功能：将 Event 对象拆分成多个字段值（user、url、timestamp）
 * - 输入输出：一个 Event 对象对应三个字段值字符串
 * - 实现方式：通过 Collector 收集每个字段的值
 * - 使用场景：数据扁平化、字段提取、数据转换等
 * <p>
 * 预期输出：
 * user:Mary
 * url:./home
 * timestamp:1000
 * user:Bob
 * url:./cart
 * timestamp:2000
 */
public class TransFlatmapEventSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 将每个事件拆分成多个字段
        stream.flatMap(new EventSplitter()).print();

        env.execute();
    }

    public static class EventSplitter implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            // 将事件的每个字段都输出为一条记录
            out.collect("user:" + event.user);
            out.collect("url:" + event.url);
            out.collect("timestamp:" + event.timestamp);
        }
    }
}
```

#### 3.5.3 FlatMap 条件过滤和转换示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapConditionalTest.java`：使用 `flatMap()` 方法根据条件输出不同数量的元素

```java
package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 条件过滤和转换示例
 * 使用 flatMap() 方法根据条件输出不同数量的元素
 * <p>
 * 特点说明：
 * - 功能：根据事件的条件，输出不同数量的元素（可能为0个、1个或多个）
 * - 输入输出：根据条件决定输出元素的数量
 * - 实现方式：使用条件判断，决定是否输出以及输出多少个元素
 * - 使用场景：条件过滤、数据转换、规则匹配等
 * <p>
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransFlatmapConditionalTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./fav", 3000L)
        );

        // 根据条件输出不同数量的元素
        stream.flatMap(new ConditionalFlatMap()).print();

        env.execute();
    }

    public static class ConditionalFlatMap implements FlatMapFunction<Event, Event> {
        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            // 如果用户是 Mary，输出2次
            if ("Mary".equals(event.user)) {
                out.collect(event);
                out.collect(event);
            }
            // 如果用户是 Bob，输出3次
            else if ("Bob".equals(event.user)) {
                out.collect(event);
                out.collect(event);
                out.collect(event);
            }
            // 其他用户不输出（过滤掉）
            // 注意：这里不输出任何元素，相当于过滤
        }
    }
}
```

#### 3.5.4 FlatMap 嵌套数据展开示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapNestedDataTest.java`：使用 `flatMap()` 方法将包含列表的数据展开成多个元素

```java
package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * FlatMap 嵌套数据展开示例
 * 使用 flatMap() 方法将包含列表的数据展开成多个元素
 * <p>
 * 特点说明：
 * - 功能：将包含列表或数组的数据结构展开成多个独立的元素
 * - 输入输出：一个包含列表的对象对应多个列表中的元素
 * - 实现方式：遍历列表，将每个元素收集到输出流
 * - 使用场景：嵌套数据展开、数组扁平化、一对多数据转换等
 * <p>
 * 预期输出：
 * apple
 * banana
 * orange
 * dog
 * cat
 * bird
 * red
 * green
 * blue
 */
public class TransFlatmapNestedDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建包含列表的数据
        DataStreamSource<List<String>> stream = env.fromElements(
                Arrays.asList("apple", "banana", "orange"),
                Arrays.asList("dog", "cat", "bird"),
                Arrays.asList("red", "green", "blue")
        );

        // 将每个列表展开成多个元素
        stream.flatMap(new ListFlattener()).print();

        env.execute();
    }

    public static class ListFlattener implements FlatMapFunction<List<String>, String> {
        @Override
        public void flatMap(List<String> list, Collector<String> out) throws Exception {
            // 遍历列表中的每个元素，收集到输出流
            for (String item : list) {
                out.collect(item);
            }
        }
    }
}
```

#### 3.5.5 FlatMap URL 解析示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapUrlParseTest.java`：使用 `flatMap()` 方法解析 URL 参数并输出多个键值对

```java
package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap URL 解析示例
 * 使用 flatMap() 方法解析 URL 参数并输出多个键值对
 * <p>
 * 特点说明：
 * - 功能：解析 URL 中的查询参数，将每个参数作为独立的键值对输出
 * - 输入输出：一个包含查询参数的 URL 对应多个键值对
 * - 实现方式：解析 URL 参数，分割成键值对，收集到输出流
 * - 使用场景：URL 解析、参数提取、数据清洗等
 * <p>
 * 预期输出：
 * param:id=1
 * param:category=electronics
 * param:price=100
 * param:name=laptop
 * param:brand=dell
 */
public class TransFlatmapUrlParseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./prod?id=1&category=electronics&price=100", 1000L),
                new Event("Bob", "./prod?name=laptop&brand=dell", 2000L)
        );

        // 解析 URL 中的查询参数
        stream.flatMap(new UrlParameterParser()).print();

        env.execute();
    }

    public static class UrlParameterParser implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            String url = event.url;
            // 检查 URL 是否包含查询参数
            if (url.contains("?")) {
                // 提取查询参数字符串（? 后面的部分）
                String queryString = url.substring(url.indexOf("?") + 1);
                // 按 & 分割参数
                String[] params = queryString.split("&");
                // 遍历每个参数，收集到输出流
                for (String param : params) {
                    if (!param.isEmpty()) {
                        out.collect("param:" + param);
                    }
                }
            }
        }
    }
}
```

#### 3.5.6 FlatMap 字符拆分示例

`src/main/java/com/action/transformation/flatmap/TransFlatmapCharSplitTest.java`：使用 `flatMap()` 方法将字符串拆分成单个字符

```java
package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 字符拆分示例
 * 使用 flatMap() 方法将字符串拆分成单个字符
 * <p>
 * 特点说明：
 * - 功能：将输入的字符串拆分成单个字符
 * - 输入输出：一个字符串对应多个字符
 * - 实现方式：遍历字符串的每个字符，收集到输出流
 * - 使用场景：字符分析、文本处理、数据验证等
 * <p>
 * 预期输出：
 * F
 * l
 * i
 * n
 * k
 * H
 * e
 * l
 * l
 * o
 * W
 * o
 * r
 * l
 * d
 */
public class TransFlatmapCharSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements(
                "Flink",
                "Hello",
                "World"
        );

        // 将每个字符串拆分成单个字符
        stream.flatMap(new CharSplitter()).print();

        env.execute();
    }

    public static class CharSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String word, Collector<String> out) throws Exception {
            // 将字符串转换为字符数组
            char[] chars = word.toCharArray();
            // 遍历每个字符，收集到输出流
            for (char c : chars) {
                out.collect(String.valueOf(c));
            }
        }
    }
}
```

### 3.6 KeyBy 按键分区

#### 3.6.1 KeyBy 基础概念

**按键分区（keyBy）**是 Flink 中非常重要的一个算子，用于在聚合前对数据流进行逻辑分区。

**核心特点：**

- **功能**：通过指定键（key），将数据流从逻辑上划分成不同的分区（partitions）
- **返回值**：keyBy 返回 `KeyedStream`，不再是 `DataStream`
- **分区机制**：通过计算 key 的哈希值，对分区数进行取模运算来实现
- **前置条件**：POJO 类型作为 key 时，必须重写 `hashCode()` 方法

**KeyBy 的多种实现方式：**

1. **Lambda 表达式**：`stream.keyBy(e -> e.user)`（推荐，代码简洁）
2. **KeySelector 匿名类**：`stream.keyBy(new KeySelector<Event, String>() {...})`
3. **自定义 KeySelector 类**：`stream.keyBy(new UserKeySelector())`

#### 3.6.2 KeyBy 基础用法示例

`src/main/java/com/action/transformation/keyBy/TransKeyByBasicTest.java`：演示 keyBy 的多种实现方式

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 基础用法示例
 * 演示 keyBy 的多种实现方式
 * <p>
 * 特点说明：
 * - 功能：演示 keyBy 的多种实现方式（Lambda 表达式、KeySelector 匿名类、自定义 KeySelector 类）
 * - 实现方式：
 *   1. Lambda 表达式：stream.keyBy(e -> e.user)
 *   2. KeySelector 匿名类：stream.keyBy(new KeySelector<Event, String>() {...})
 *   3. 自定义 KeySelector 类：stream.keyBy(new UserKeySelector())
 * - 返回值：keyBy 返回 KeyedStream，不再是 DataStream
 * - 使用场景：数据分组、分流、为后续聚合操作做准备
 * <p>
 * 预期输出：
 * keyBy1> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy1> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * keyBy2> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy2> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * keyBy3> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy3> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransKeyByBasicTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 方式1：使用 Lambda 表达式（推荐，代码简洁）
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(e -> e.user);
        keyedStream1.print("keyBy1");

        // 方式2：使用 KeySelector 匿名类
        KeyedStream<Event, String> keyedStream2 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event e) throws Exception {
                return e.user;
            }
        });
        keyedStream2.print("keyBy2");

        // 方式3：使用自定义 KeySelector 类（适合复杂逻辑）
        KeyedStream<Event, String> keyedStream3 = stream.keyBy(new UserKeySelector());
        keyedStream3.print("keyBy3");

        env.execute();
    }

    /**
     * 自定义 KeySelector 实现类
     */
    public static class UserKeySelector implements KeySelector<Event, String> {
        @Override
        public String getKey(Event value) throws Exception {
            return value.user;
        }
    }
}
```

#### 3.6.3 KeyBy 简单聚合示例

`src/main/java/com/action/transformation/keyBy/TransKeyBySimpleAggTest.java`：按键分组之后进行聚合，提取当前用户最近一次访问数据

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 简单聚合示例
 * 按键分组之后进行聚合，提取当前用户最近一次访问数据
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 按键分组，然后使用 max/maxBy 提取每个用户最近一次访问数据
 * - 实现方式：
 * 1. 使用 KeySelector 匿名类和 max() 方法
 * 2. 使用 Lambda 表达式和 maxBy() 方法
 * - 区别说明：
 * - max()：只更新指定字段，其他字段保持第一个元素的值
 * - maxBy()：返回完整记录，所有字段都更新为最大值对应的记录
 * - 使用场景：用户行为分析、最近访问记录提取、分组聚合等
 * <p>
 * 预期输出：
 max> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 maxBy> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 max> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 maxBy> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 maxBy> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.3}
 maxBy> Event{user='Bob', url='./prod?id=1', timestamp=1970-01-01 08:00:03.3}
 max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.5}
 maxBy> Event{user='Bob', url='./home', timestamp=1970-01-01 08:00:03.5}
 max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.8}
 maxBy> Event{user='Bob', url='./prod?id=2', timestamp=1970-01-01 08:00:03.8}
 max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:04.2}
 maxBy> Event{user='Bob', url='./prod?id=3', timestamp=1970-01-01 08:00:04.2}
 */
public class TransKeyBySimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 按键分组之后进行聚合，提取当前用户最近一次访问数据

        // 方式1：使用 KeySelector 匿名类和 max() 方法
        KeyedStream<Event, String> keyedStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
        keyedStream.max("timestamp").print("max");

        // 方式2：使用 Lambda 表达式和 maxBy() 方法
        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy");

        env.execute();
    }
}
```

#### 3.6.4 KeyBy 多字段组合键示例

`src/main/java/com/action/transformation/keyBy/TransKeyByCompositeKeyTest.java`：使用多个字段组合作为 key 进行分组

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 多字段组合键示例
 * 使用多个字段组合作为 key 进行分组
 * <p>
 * 特点说明：
 * - 功能：使用多个字段组合作为 key 进行分组，只有所有字段都相同的记录才会被分到同一组
 * - 实现方式：返回 Tuple 类型作为组合键
 * - 使用场景：需要按多个维度分组、复合键分组、统计用户对特定URL的访问次数等
 * <p>
 * 数据说明：
 * - 测试数据包含相同用户访问不同URL、不同用户访问相同URL、相同用户访问相同URL等多种情况
 * - 使用组合键（用户 + URL）后，只有用户和URL都相同的记录才会被分到同一组
 * - 例如：(Mary, ./home) 和 (Mary, ./home) 会被分到同一组
 * - 例如：(Mary, ./home) 和 (Mary, ./cart) 会被分到不同组
 * - 例如：(Mary, ./home) 和 (Bob, ./home) 会被分到不同组
 * <p>
 * 预期输出：
 * count> ((Mary,./home),2)
 * count> ((Mary,./cart),1)
 * count> ((Bob,./home),2)
 * count> ((Bob,./cart),1)
 * count> ((Alice,./prod?id=100),1)
 * count> ((Alice,./home),1)
 */
public class TransKeyByCompositeKeyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试数据：包含相同用户访问不同URL、不同用户访问相同URL、相同用户访问相同URL等情况
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),      // (Mary, ./home) - 组1
                new Event("Mary", "./cart", 2000L),      // (Mary, ./cart) - 组2
                new Event("Bob", "./home", 3000L),       // (Bob, ./home) - 组3
                new Event("Bob", "./cart", 4000L),       // (Bob, ./cart) - 组4
                new Event("Alice", "./prod?id=100", 5000L), // (Alice, ./prod?id=100) - 组5
                new Event("Mary", "./home", 6000L),      // (Mary, ./home) - 组1（与第一条相同）
                new Event("Bob", "./home", 7000L),       // (Bob, ./home) - 组3（与第三条相同）
                new Event("Alice", "./home", 8000L)      // (Alice, ./home) - 组6
        );

        // 使用多个字段组合作为 key（用户 + URL）
        // 只有用户和URL都相同的记录才会被分到同一组
        KeyedStream<Event, Tuple2<String, String>> keyedStream = stream.keyBy(new KeySelector<Event, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Event value) throws Exception {
                return Tuple2.of(value.user, value.url);
            }
        });

        // 统计每个（用户，URL）组合的访问次数，以展示组合键的分组效果
        keyedStream.map(new MapFunction<Event, Tuple2<Tuple2<String, String>, Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Long> map(Event value) throws Exception {
                        Tuple2<String, String> key = Tuple2.of(value.user, value.url);
                        return Tuple2.of(key, 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> value) throws Exception {
                        return value.f0;  // 使用组合键作为 key
                    }
                })
                .sum(1)             // 统计访问次数（对第二个字段求和）
                .print("count");

        env.execute();
    }
}
```

#### 3.6.5 KeyBy 统计用户访问次数示例

`src/main/java/com/action/transformation/keyBy/TransKeyByCountTest.java`：使用 keyBy 分组后统计每个用户的访问次数（PV统计）

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 统计用户访问次数示例
 * 使用 keyBy 分组后统计每个用户的访问次数（PV统计）
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 分组后统计每个用户的访问次数
 * - 实现方式：keyBy + map + sum 聚合
 * - 使用场景：用户行为统计、PV/UV统计、分组计数等
 * <p>
 * 预期输出：
 * count> (Mary,2)
 * count> (Bob,4)
 * count> (Alice,1)
 */
public class TransKeyByCountTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 统计每个用户的访问次数
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .sum(1)             // 对第二个字段（计数）求和
                .print("count");

        env.execute();
    }
}
```

#### 3.6.6 KeyBy 分组后计算总访问时长示例

`src/main/java/com/action/transformation/keyBy/TransKeyByAvgTest.java`：使用 keyBy 分组后计算每个用户的总访问时长

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 分组后计算总访问时长示例
 * 使用 keyBy 分组后计算每个用户的总访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 分组后计算每个用户的总访问时长
 * - 实现方式：keyBy + map + reduce
 * - 使用场景：用户行为分析、时长统计、分组汇总等
 * <p>
 * 预期输出：
 * totalDuration> (Mary,./home,1000)
 * totalDuration> (Bob,./cart,2000)
 * totalDuration> (Alice,./prod?id=100,3000)
 * totalDuration> (Bob,./cart,5300)
 * totalDuration> (Bob,./cart,8800)
 * totalDuration> (Bob,./cart,12600)
 * totalDuration> (Mary,./home,5000)
 */
public class TransKeyByAvgTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 计算每个用户的总访问时长
        stream.map(new MapFunction<Event, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Event value) throws Exception {
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple3.of(value.user, value.url, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        // 累加访问时长
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .print("totalDuration");

        env.execute();
    }
}
```

#### 3.6.7 KeyBy 按 URL 类型分组示例

`src/main/java/com/action/transformation/keyBy/TransKeyByUrlTypeTest.java`：使用 keyBy 按 URL 类型（如
home、cart、prod）进行分组统计

```java
package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 按 URL 类型分组示例
 * 使用 keyBy 按 URL 类型（如 home、cart、prod）进行分组统计
 * <p>
 * 特点说明：
 * - 功能：按 URL 类型进行分组，统计每种类型的访问次数
 * - 实现方式：提取 URL 类型作为 key，然后进行分组统计
 * - 使用场景：页面访问分析、URL 类型统计、业务指标分析等
 * <p>
 * 预期输出：
 * urlType> (home,3)
 * urlType> (cart,2)
 * urlType> (prod,2)
 */
public class TransKeyByUrlTypeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L),
                new Event("Alice", "./cart", 5000L)
        );

        // 按 URL 类型分组统计
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        // 提取 URL 类型（如 ./home -> home, ./cart -> cart, ./prod?id=1 -> prod）
                        String urlType = extractUrlType(value.url);
                        return Tuple2.of(urlType, 1L);
                    }

                    /**
                     * 提取 URL 类型
                     */
                    private String extractUrlType(String url) {
                        if (url.contains("./home")) {
                            return "home";
                        } else if (url.contains("./cart")) {
                            return "cart";
                        } else if (url.contains("./prod")) {
                            return "prod";
                        } else {
                            return "other";
                        }
                    }
                })
                .keyBy(r -> r.f0)  // 按 URL 类型分组
                .sum(1)             // 统计每种类型的访问次数
                .print("urlType");

        env.execute();
    }
}
```

### 3.7 UDF 自定义函数

`src/main/java/com/action/transformation/udf/TransUdfTest.java`：演示多种方式实现自定义函数（UDF）

```java
package com.action.transformation.udf;


import com.action.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * UDF 自定义函数示例
 * 演示多种方式实现自定义函数（UDF）
 * <p>
 * 特点说明：
 * - 功能：演示三种实现自定义函数的方式
 * - 实现方式：
 * 1. 实现接口的自定义函数类
 * 2. 匿名类
 * 3. Lambda 表达式
 * - 使用场景：根据业务需求灵活选择实现方式
 * <p>
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 */
public class TransUdfTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 1. 传入实现FilterFunction接口的自定义函数类
        DataStream<Event> stream1 = clicks.filter(new FlinkFilter());

        // 传入属性字段
        DataStream<Event> stream2 = clicks.filter(new KeyWordFilter("home"));

        // 2. 传入匿名类
        DataStream<Event> stream3 = clicks.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.url.contains("home");
            }
        });

        // 3. 传入Lambda表达式
        SingleOutputStreamOperator<Event> stream4 = clicks.filter(data -> data.url.contains("home"));

//        stream1.print();
//        stream2.print();
//        stream3.print();
        stream4.print();

        env.execute();
    }

    public static class FlinkFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains("home");
        }
    }

    public static class KeyWordFilter implements FilterFunction<Event> {
        private String keyWord;

        KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains(this.keyWord);
        }
    }
}
```

### 3.8 Reduce 转换算子

#### 3.8.1 Reduce 基础概念

**归约聚合（reduce）**是 Flink 中一个一般化的聚合统计操作。从大名鼎鼎的 MapReduce 开始，我们对 reduce
操作就不陌生：它可以对已有的数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算。

**核心特点：**

- **功能**：对数据流进行归约操作，将两个元素合并为一个元素
- **返回值**：reduce 操作会将 KeyedStream 转换为 DataStream，不会改变流的元素数据类型，所以输出类型和输入类型是一样的
- **前置条件**：需要先使用 keyBy() 进行分组
- **累加器机制**：reduce 的语义是针对列表进行规约操作，运算规则由 ReduceFunction 中的 reduce 方法来定义，而在
  ReduceFunction 内部会维护一个初始值为空的累加器，注意累加器的类型和输入元素的类型相同，当第一条元素到来时，累加器的值更新为第一条元素的值，当新的元素到来时，新元素会和累加器进行累加操作，这里的累加操作就是
  reduce 函数定义的运算规则。然后将更新以后的累加器的值向下游输出

**ReduceFunction 接口定义：**

```java
public interface ReduceFunction<T> extends Function, Serializable {
    T reduce(T value1, T value2) throws Exception;
}
```

**实现方式：**

1. **自定义函数类**：实现 ReduceFunction<T> 接口
2. **匿名类**：`new ReduceFunction<T>() {...}`
3. **Lambda 表达式**：`(value1, value2) -> {...}`（注意类型推断问题）

**注意事项：**

- reduce 同简单聚合算子一样，也要针对每一个 key 保存状态
- 因为状态不会清空，所以我们需要将 reduce 算子作用在一个有限 key 的流上

#### 3.8.2 Reduce 统计用户PV并找出最大PV用户示例

`src/main/java/com/action/transformation/reduce/TransReduceTest.java`：使用 `reduce()` 方法统计每个用户的 PV，并找出最大
PV 用户

```java
package com.action.transformation.reduce;


import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 转换算子示例
 * 使用 reduce() 方法对数据流进行归约操作
 * <p>
 * 特点说明：
 * - 功能：对数据流进行归约操作，将两个元素合并为一个元素
 * - 输入输出：两个输入元素合并为一个输出元素
 * - 实现方式：实现 ReduceFunction<T> 接口，定义两个元素的合并逻辑
 * - 使用场景：累加统计、最大值/最小值计算、自定义聚合等
 * - 注意事项：需要先使用 keyBy() 进行分组
 * <p>
 * 预期输出：
 * (用户, PV统计值)
 * ...
 * (最大PV用户, 最大PV值)
 */
public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里的使用了之前自定义数据源小节中的ClickSource()
        env.addSource(new ClickSource())
                // 将Event数据类型转换成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event e) throws Exception {
                        return Tuple2.of(e.user, 1L);
                    }
                })
                .keyBy(r -> r.f0) // 使用用户名来进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 每到一条数据，用户pv的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .keyBy(r -> true) // 为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();

        env.execute();

    }
}
```

#### 3.8.3 Reduce 求和归约示例

`src/main/java/com/action/transformation/reduce/TransReduceSumTest.java`：使用 `reduce()` 方法计算每个用户的总访问时长

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 求和归约示例
 * 使用 reduce() 方法计算每个用户的总访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 对每个用户的访问时长进行累加求和
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：将两个元素的时长值相加，返回累加结果
 * - 使用场景：用户行为分析、时长统计、累加计算等
 * <p>
 * 预期输出：
 * sum> (Mary,5000)
 * sum> (Bob,12800)
 * sum> (Alice,3000)
 */
public class TransReduceSumTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 计算每个用户的总访问时长
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple2.of(value.user, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 累加访问时长：将两个元素的时长值相加
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print("sum");

        env.execute();
    }
}
```

#### 3.8.4 Reduce 最大值归约示例

`src/main/java/com/action/transformation/reduce/TransReduceMaxTest.java`：使用 `reduce()` 方法找出每个用户访问的最大时间戳

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 最大值归约示例
 * 使用 reduce() 方法找出每个用户访问的最大时间戳
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 找出每个用户访问的最大时间戳
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：比较两个元素的时间戳，返回时间戳较大的元素
 * - 使用场景：找出最新记录、最大值计算、最近访问时间等
 * <p>
 * 预期输出：
 * max> (Mary,4000)
 * max> (Bob,3800)
 * max> (Alice,3000)
 */
public class TransReduceMaxTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 找出每个用户访问的最大时间戳
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 比较两个元素的时间戳，返回时间戳较大的元素
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print("max");

        env.execute();
    }
}
```

#### 3.8.5 Reduce 最小值归约示例

`src/main/java/com/action/transformation/reduce/TransReduceMinTest.java`：使用 `reduce()` 方法找出每个用户访问的最小时间戳（最早访问时间）

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 最小值归约示例
 * 使用 reduce() 方法找出每个用户访问的最小时间戳（最早访问时间）
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 找出每个用户访问的最小时间戳
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：比较两个元素的时间戳，返回时间戳较小的元素
 * - 使用场景：找出最早记录、最小值计算、首次访问时间等
 * <p>
 * 预期输出：
 * min> (Mary,1000)
 * min> (Bob,2000)
 * min> (Alice,3000)
 */
public class TransReduceMinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 找出每个用户访问的最小时间戳
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 比较两个元素的时间戳，返回时间戳较小的元素
                        return value1.f1 < value2.f1 ? value1 : value2;
                    }
                })
                .print("min");

        env.execute();
    }
}
```

#### 3.8.6 Reduce 平均值归约示例

`src/main/java/com/action/transformation/reduce/TransReduceAvgTest.java`：使用 `reduce()` 方法计算每个用户的平均访问时长

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 平均值归约示例
 * 使用 reduce() 方法计算每个用户的平均访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 计算每个用户的平均访问时长
 * - 实现方式：keyBy + map + reduce + map（需要额外的 map 来计算平均值）
 * - 归约逻辑：维护累加和和计数，最后计算平均值
 * - 使用场景：平均值计算、统计分析、用户行为分析等
 * <p>
 * 数据说明：
 * - 使用 Tuple3<String, Long, Long> 存储（用户，总时长，访问次数）
 * - reduce 过程中累加总时长和访问次数
 * - 最后通过 map 计算平均值
 * <p>
 * 预期输出：
 * avg> (Mary,2500.0)
 * avg> (Bob,3200.0)
 * avg> (Alice,3000.0)
 */
public class TransReduceAvgTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 计算每个用户的平均访问时长
        stream.map(new MapFunction<Event, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Event value) throws Exception {
                        // Tuple3: (用户, 访问时长, 访问次数)
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple3.of(value.user, value.timestamp, 1L);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
                        // 累加总时长和访问次数
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                        // 计算平均值：总时长 / 访问次数
                        double avg = (double) value.f1 / value.f2;
                        return Tuple3.of(value.f0, avg, value.f2);
                    }
                })
                .print("avg");

        env.execute();
    }
}
```

#### 3.8.7 Reduce 字符串拼接归约示例

`src/main/java/com/action/transformation/reduce/TransReduceStringConcatTest.java`：使用 `reduce()` 方法将每个用户访问的所有
URL 拼接成一个字符串

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 字符串拼接归约示例
 * 使用 reduce() 方法将每个用户访问的所有 URL 拼接成一个字符串
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 将每个用户访问的所有 URL 拼接成一个字符串
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：将两个元素的 URL 字符串用分隔符连接
 * - 使用场景：URL 列表收集、字符串拼接、访问路径记录等
 * <p>
 * 预期输出：
 * concat> (Mary,./home;./home)
 * concat> (Bob,./cart;./prod?id=1;./home;./prod?id=2)
 * concat> (Alice,./prod?id=100)
 */
public class TransReduceStringConcatTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 将每个用户访问的所有 URL 拼接成一个字符串
        stream.map(new MapFunction<Event, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Event value) throws Exception {
                        return Tuple2.of(value.user, value.url);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
                        // 将两个元素的 URL 字符串用分号连接
                        String concatenated = value1.f1 + ";" + value2.f1;
                        return Tuple2.of(value1.f0, concatenated);
                    }
                })
                .print("concat");

        env.execute();
    }
}
```

#### 3.8.8 Reduce 自定义对象归约示例

`src/main/java/com/action/transformation/reduce/TransReduceCustomObjectTest.java`：使用 `reduce()` 方法对 Event
对象进行归约，找出每个用户最近访问的记录

```java
package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 自定义对象归约示例
 * 使用 reduce() 方法对 Event 对象进行归约，找出每个用户最近访问的记录
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 对 Event 对象进行归约，找出每个用户最近访问的记录
 * - 实现方式：keyBy + reduce
 * - 归约逻辑：比较两个 Event 对象的时间戳，返回时间戳较大的 Event
 * - 使用场景：对象归约、最新记录提取、自定义聚合逻辑等
 * <p>
 * 预期输出：
 * latest> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:04.0}
 * latest> Event{user='Bob', url='./prod?id=2', timestamp=1970-01-01 08:00:03.8}
 * latest> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class TransReduceCustomObjectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 找出每个用户最近访问的记录
        stream.keyBy(e -> e.user)  // 按用户分组
                .reduce(new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event value1, Event value2) throws Exception {
                        // 比较两个 Event 对象的时间戳，返回时间戳较大的 Event
                        return value1.timestamp > value2.timestamp ? value1 : value2;
                    }
                })
                .print("latest");

        env.execute();
    }
}
```

### 3.9 Tuple 聚合算子

`src/main/java/com/action/transformation/TransTupleAggreationTest.java`：演示对 Tuple 类型数据进行聚合操作

```java
package com.action.transformation;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Tuple 聚合算子示例
 * 演示对 Tuple 类型数据进行聚合操作
 *
 * 特点说明：
 * - 功能：对 Tuple 类型数据进行聚合操作
 * - 聚合方法：
 *   - sum()：求和
 *   - max()：最大值（只更新指定字段）
 *   - min()：最小值（只更新指定字段）
 *   - maxBy()：最大值（返回完整记录）
 *   - minBy()：最小值（返回完整记录）
 * - 字段指定：可以通过位置索引（如 1）或字段名称（如 "f1"）指定聚合字段
 * - 使用场景：数值统计、分组聚合等
 * 
 * 预期输出：
 * (a,1)
 * (b,3)
 */
public class TransTupleAggreationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

//        stream.keyBy(r -> r.f0).sum(1).print();
//        stream.keyBy(r -> r.f0).sum("f1").print();
//        stream.keyBy(r -> r.f0).max(1).print();
//        stream.keyBy(r -> r.f0).max("f1").print();
//        stream.keyBy(r -> r.f0).min(1).print();
//        stream.keyBy(r -> r.f0).min("f1").print();
//        stream.keyBy(r -> r.f0).maxBy(1).print();
//        stream.keyBy(r -> r.f0).maxBy("f1").print();
//        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).minBy("f1").print();

        env.execute();
    }
}
```

### 3.10 POJO 聚合算子

`src/main/java/com/action/transformation/TransPojoAggregationTest.java`：演示对 POJO 类型数据进行聚合操作

```java
package com.action.transformation;


import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * POJO 聚合算子示例
 * 演示对 POJO 类型数据进行聚合操作
 *
 * 特点说明：
 * - 功能：对 POJO 类型数据进行聚合操作
 * - 聚合方法：
 *   - max()：最大值（只更新指定字段，其他字段保持第一个元素的值）
 *   - min()：最小值（只更新指定字段，其他字段保持第一个元素的值）
 *   - maxBy()：最大值（返回完整记录）
 *   - minBy()：最小值（返回完整记录）
 * - 字段指定：通过字段名称指定聚合字段
 * - 使用场景：对象字段聚合、分组统计等
 * 
 * 预期输出：
 * Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:04.0}
 * Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransPojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./cart", 3000L),
                new Event("Mary", "./fav", 4000L)
        );

        stream.keyBy(e -> e.user)
//                .maxBy("timestamp")
                .max("timestamp")    // 指定字段名称
                .print();

        env.execute();
    }
}
```

### 3.11 返回类型处理

`src/main/java/com/action/transformation/TransReturnTypeTest.java`：演示 Lambda 表达式返回类型的处理方式

```java
package com.action.transformation;


import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 返回类型处理示例
 * 演示 Lambda 表达式返回类型的处理方式
 *
 * 特点说明：
 * - 功能：演示 Lambda 表达式返回复杂类型时的处理方式
 * - 问题：Java 的 Lambda 表达式在返回泛型类型（如 Tuple2）时，类型擦除会导致 Flink 无法推断类型
 * - 解决方案：
 *   1. 使用 .returns() 显式指定返回类型
 *   2. 使用类替代 Lambda 表达式
 *   3. 使用匿名类替代 Lambda 表达式
 * - 使用场景：Lambda 表达式返回复杂类型时
 *
 * 预期输出：
 * (Mary,1)
 * (Bob,1)
 * (Mary,1)
 * (Bob,1)
 * (Mary,1)
 * (Bob,1)
 */
public class TransReturnTypeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 想要转换成二元组类型，需要进行以下处理
        // 1) 使用显式的 ".returns(...)"
        DataStream<Tuple2<String, Long>> stream3 = clicks
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        stream3.print();


        // 2) 使用类来替代Lambda表达式
        clicks.map(new MyTuple2Mapper())
                .print();

        // 3) 使用匿名类来代替Lambda表达式
        clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).print();

        env.execute();
    }

    // 自定义MapFunction的实现类
    public static class MyTuple2Mapper implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event value) throws Exception {
            return Tuple2.of(value.user, 1L);
        }
    }
}
```

### 3.12 Rich Function 富函数

`src/main/java/com/action/transformation/richFunction/TransRichFunctionTest.java`：演示 Rich Function 的使用，可以访问运行时上下文

```java
package com.action.transformation.richFunction;


import com.action.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Rich Function 富函数示例
 * 演示 Rich Function 的使用，可以访问运行时上下文
 * <p>
 * 特点说明：
 * - 功能：Rich Function 提供了生命周期方法和运行时上下文访问
 * - 生命周期方法：
 * - open()：任务初始化时调用，可以用于初始化资源
 * - close()：任务结束时调用，可以用于清理资源
 * - 运行时上下文：通过 getRuntimeContext() 可以访问：
 * - 并行子任务索引
 * - 并行度
 * - 任务名称
 * - 分布式缓存
 * - 累加器
 * - 使用场景：需要访问运行时信息、初始化资源、使用分布式缓存等
 * <p>
 * 预期输出：
 * 索引为 0 的任务开始
 * 索引为 1 的任务开始
 * 1000
 * 2000
 * 5000
 * 60000
 * 索引为 0 的任务结束
 * 索引为 1 的任务结束
 */
public class TransRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        // 将点击事件转换成长整型的时间戳输出
        clicks.map(new RichMapFunction<Event, Long>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
                    }

                    @Override
                    public Long map(Event value) throws Exception {
                        return value.timestamp;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
                    }
                })
                .print();

        env.execute();
    }
}
```

### 3.13 物理分区

#### 3.13.1 物理分区基础概念

**物理分区（Physical Partitioning）**是 Flink 中用于控制数据在并行任务之间物理分布的操作。

**核心特点：**

- **功能**：将数据进行重新分布，传递到不同的流分区去进行下一步处理
- **与 keyBy 的区别**：
    - **keyBy**：逻辑分区（logical partitioning），只能保证把数据按 key"分开"，至于分得均不均匀、每个
      key的数据具体会分到哪一区去，这些是完全无从控制的
    - **物理分区**：真正硬核的分区，可以真正控制分区策略，精准地调配数据，告诉每个数据到底去哪里
- **返回值**：物理分区之后结果仍是 DataStream，且流中元素数据类型保持不变（与 keyBy 不同，keyBy 之后得到的是一个KeyedStream）
- **数据转换**：分区算子并不对数据进行转换处理，只是定义了数据的传输方式

**自动分区场景：** 当数据执行的上下游任务并行度变化时，系统会自动地将数据均匀地发往下游所有的并行任务，保证各个分区的负载均衡。

**手动分区场景：** 当发生数据倾斜的时候，系统无法自动调整，这时就需要我们重新进行负载均衡，将数据流较为平均地发送到下游任务操作分区中去。

**常见的物理分区策略：**

1. **随机分区（shuffle）**：最简单的重分区方式，将数据随机地分配到下游算子的并行任务中去
2. **轮询分区（Round-Robin/rebalance）**：按照先后顺序将数据做依次分发，使用 Round-Robin 负载均衡算法
3. **重缩放分区（rescale）**：在上下游并行度之间进行局部重平衡，只将数据轮询发送到下游并行任务的一部分中
4. **广播（broadcast）**：数据会在不同的分区都保留一份，复制到所有下游并行任务
5. **全局分区（global）**：将所有输入流数据都发送到下游算子的第一个并行子任务中去
6. **自定义分区（Custom）**：通过 partitionCustom() 方法自定义分区策略

#### 3.13.2 物理分区综合示例

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPatitioningTest.java`：演示各种物理分区策略的综合示例

```java
package com.action.transformation.physicalPartition;


import com.action.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 物理分区示例
 * 演示各种物理分区策略
 * <p>
 * 特点说明：
 * - 功能：控制数据在并行任务之间的物理分布
 * - 分区策略：
 * 1. shuffle()：随机分区，数据随机分布到下游并行任务
 * 2. rebalance()：轮询分区，数据轮询分布到下游并行任务
 * 3. rescale()：重缩放分区，在上下游并行度之间进行局部重平衡
 * 4. broadcast()：广播分区，数据复制到所有下游并行任务
 * 5. global()：全局分区，所有数据发送到第一个并行任务
 * 6. partitionCustom()：自定义分区，根据自定义逻辑分区
 * - 使用场景：负载均衡、数据分布优化、广播数据等
 * <p>
 * 预期输出：
 * shuffle> Event{user='...', url='...', timestamp=...}
 * rebalance> Event{user='...', url='...', timestamp=...}
 * ...
 */
public class TransPhysicalPatitioningTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        // 1. 随机分区
        stream.shuffle().print("shuffle").setParallelism(4);

        // 2. 轮询分区
        stream.rebalance().print("rebalance").setParallelism(4);

        // 3. rescale重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {  // 这里使用了并行数据源的富函数版本
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇数发送到索引为1的并行子任务
                            // 将偶数发送到索引为0的并行子任务
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);

        // 4. 广播
        stream.broadcast().print("broadcast").setParallelism(4);

        // 5. 全局分区
        stream.global().print("global").setParallelism(4);

        // 6. 自定义重分区
        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);

        env.execute();
    }
}
```

#### 3.13.3 物理分区 - shuffle随机分区

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionShuffleTest.java`：使用 `shuffle()`
方法将数据随机分配到下游并行任务

```java
package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 随机分区示例
 * 使用 shuffle() 方法将数据随机分配到下游并行任务
 * <p>
 * 特点说明：
 * - 功能：最简单的重分区方式，通过"洗牌"将数据随机分配到下游算子的并行任务
 * - 分区策略：随机分区服从均匀分布（uniform distribution），可以把流中的数据随机打乱
 * - 数据分布：均匀地传递到下游任务分区
 * - 结果特点：对于同样的输入数据，每次执行得到的结果不会相同（因为是完全随机的）
 * - 返回值：经过随机分区之后，得到的依然是一个 DataStream
 * - 使用场景：负载均衡、数据随机分布、打破数据倾斜等
 */
public class TransPhysicalPartitionShuffleTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经洗牌后打印输出，并行度为 4
        stream.shuffle().print("shuffle").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * shuffle:4> Event{user='Bob', url='./home', timestamp=2025-11-15 23:33:15.91}
         * shuffle:4> Event{user='Bob', url='./home', timestamp=2025-11-15 23:33:16.939}
         * shuffle:3> Event{user='Bob', url='./cart', timestamp=2025-11-15 23:33:17.949}
         * shuffle:4> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:33:18.963}
         * shuffle:4> Event{user='Bob', url='./fav', timestamp=2025-11-15 23:33:19.972}
         * shuffle:4> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:33:20.981}
         * shuffle:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:33:21.983}
         * shuffle:3> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:33:22.993}
         * shuffle:1> Event{user='Mary', url='./home', timestamp=2025-11-15 23:33:24.002}
         * shuffle:2> Event{user='Bob', url='./fav', timestamp=2025-11-15 23:33:25.013}
         * shuffle:2> Event{user='Alice', url='./cart', timestamp=2025-11-15 23:33:26.02}
         */
    }
}
```

#### 3.13.4 物理分区 - rebalance轮询分区

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionRebalanceTest.java`：使用 `rebalance()`
方法将数据轮询分配到下游并行任务

```java
package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 轮询分区示例
 * 使用 rebalance() 方法将数据轮询分配到下游并行任务
 * <p>
 * 特点说明：
 * - 功能：按照先后顺序将数据做依次分发，使用 Round-Robin 负载均衡算法
 * - 分区策略：轮询分区，将输入流数据平均分配到下游的并行任务中去
 * - 算法说明：Round-Robin 算法用在了很多地方，例如 Kafka 和 Nginx
 * - 数据分布：数据被平均分配到所有并行任务中
 * - 返回值：经过轮询分区之后，得到的依然是一个 DataStream
 * - 使用场景：负载均衡、数据平均分布、打破数据倾斜等
 */
public class TransPhysicalPartitionRebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经轮询重分区后打印输出，并行度为 4
        stream.rebalance().print("rebalance").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * rebalance:4> Event{user='Cary', url='./prod?id=1', timestamp=2025-11-15 23:28:32.155}
         * rebalance:1> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:28:33.179}
         * rebalance:2> Event{user='Cary', url='./prod?id=2', timestamp=2025-11-15 23:28:34.189}
         * rebalance:3> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:35.201}
         * rebalance:4> Event{user='Alice', url='./home', timestamp=2025-11-15 23:28:36.213}
         * rebalance:1> Event{user='Mary', url='./prod?id=2', timestamp=2025-11-15 23:28:37.226}
         * rebalance:2> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:38.233}
         * rebalance:3> Event{user='Cary', url='./prod?id=1', timestamp=2025-11-15 23:28:39.244}
         * rebalance:4> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:40.245}
         * rebalance:1> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:41.253}
         * rebalance:2> Event{user='Cary', url='./prod?id=2', timestamp=2025-11-15 23:28:42.265}
         * rebalance:3> Event{user='Mary', url='./prod?id=2', timestamp=2025-11-15 23:28:43.276}
         * rebalance:4> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:28:44.288}
         */
    }
}
```

#### 3.13.5 物理分区 - rescale重缩放分区

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionRescaleTest.java`：使用 `rescale()`
方法在上下游并行度之间进行局部重平衡

```java
package com.action.transformation.physicalPartition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 物理分区 - 重缩放分区示例
 * 使用 rescale() 方法在上下游并行度之间进行局部重平衡
 * <p>
 * 特点说明：
 * - 功能：重缩放分区和轮询分区非常相似，但只会将数据轮询发送到下游并行任务的一部分中
 * - 分区策略：底层使用 Round-Robin 算法进行轮询，但只针对部分下游任务
 * - 区别说明：
 *   - rebalance：每个发牌人都面向所有人发牌（所有上游任务和所有下游任务之间建立通信通道）
 *   - rescale：分成小团体，发牌人只给自己团体内的所有人轮流发牌（每个任务和下游对应的部分任务之间建立通信通道）
 * - 效率优势：当下游任务数量是上游任务数量的整数倍时，rescale 的效率明显会更高
 * - 网络优化：rescale 可以让数据只在当前 TaskManager 的多个 slot 之间重新分配，从而避免了网络传输带来的损耗
 * - 使用场景：上下游并行度调整、局部重平衡、减少网络传输等
 * <p>
 * 数据说明：
 * - 使用并行数据源的富函数版本，可以调用 getRuntimeContext 方法获取运行时上下文信息
 * - 将奇数发送到索引为 1 的并行子任务
 * - 将偶数发送到索引为 0 的并行子任务
 */
public class TransPhysicalPartitionRescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务
                            // 将偶数发送到索引为 0 的并行子任务
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * 3> 1
         * 2> 4
         * 1> 2
         * 4> 3
         * 1> 6
         * 2> 8
         * 3> 5
         * 4> 7
         */
    }
}
```

#### 3.13.6 物理分区 - broadcast广播分区

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionBroadcastTest.java`：使用 `broadcast()`
方法将数据复制到所有下游并行任务

```java
package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 广播分区示例
 * 使用 broadcast() 方法将数据复制到所有下游并行任务
 * <p>
 * 特点说明：
 * - 功能：经过广播之后，数据会在不同的分区都保留一份
 * - 分区策略：数据被复制然后广播到下游的所有并行任务中
 * - 数据特点：每个下游并行任务都会收到完整的数据副本
 * - 返回值：经过广播分区之后，得到的依然是一个 DataStream
 * - 使用场景：动态更新配置、动态发送规则、小表关联、广播变量等
 */
public class TransPhysicalPartitionBroadcastTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * broadcast:1> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:3> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:2> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:4> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:4> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:3> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:2> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         */
    }
}
```

#### 3.13.7 物理分区 - global全局分区

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionGlobalTest.java`：使用 `global()`
方法将所有数据发送到下游算子的第一个并行子任务

```java
package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 全局分区示例
 * 使用 global() 方法将所有数据发送到下游算子的第一个并行子任务
 * <p>
 * 特点说明：
 * - 功能：将所有输入流数据都发送到下游算子的第一个并行子任务中去
 * - 分区策略：非常极端的分区方式，相当于强行让下游任务并行度变成了 1
 * - 数据分布：所有数据都发送到第一个并行任务（索引为 0）
 * - 注意事项：使用这个操作要非常谨慎，可能对程序造成很大的压力
 * - 返回值：经过全局分区之后，得到的依然是一个 DataStream
 * - 使用场景：全局聚合、单点处理、调试等
 */
public class TransPhysicalPartitionGlobalTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经全局分区后打印输出，并行度为 4（但所有数据都会发送到第一个任务）
        stream.global().print("global").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * global:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:26:56.925}
         * global:1> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:26:57.957}
         * global:1> Event{user='Bob', url='./home', timestamp=2025-11-15 23:26:58.97}
         * global:1> Event{user='Bob', url='./cart', timestamp=2025-11-15 23:26:59.984}
         * global:1> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:27:00.996}
         * global:1> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:27:02.007}
         * global:1> Event{user='Mary', url='./fav', timestamp=2025-11-15 23:27:03.02}
         * global:1> Event{user='Mary', url='./prod?id=1', timestamp=2025-11-15 23:27:04.021}
         * global:1> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:27:05.033}
         */
    }
}
```

#### 3.13.8 物理分区 - 自定义分区示例

`src/main/java/com/action/transformation/physicalPartition/TransPhysicalPartitionCustomTest.java`：使用
`partitionCustom()` 方法根据自定义逻辑进行分区

```java
package com.action.transformation.physicalPartition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 自定义分区示例
 * 使用 partitionCustom() 方法根据自定义逻辑进行分区
 * <p>
 * 特点说明：
 * - 功能：当 Flink 提供的所有分区策略都不能满足用户的需求时，可以通过自定义分区策略
 * - 分区策略：需要传入自定义分区器（Partitioner）对象和应用分区器的字段
 * - 字段指定：可以通过字段名称指定、通过字段位置索引指定，还可以实现一个 KeySelector
 * - 返回值：经过自定义分区之后，得到的依然是一个 DataStream
 * - 使用场景：特殊分区需求、业务规则分区、自定义负载均衡等
 * <p>
 * 数据说明：
 * - 将自然数按照奇偶分区
 * - 奇数发送到分区 1，偶数发送到分区 0
 */
public class TransPhysicalPartitionCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // 奇数返回 1，偶数返回 0
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);

        env.execute();

        /*
         * 预期输出：
         * 2> 1
         * 1> 2
         * 2> 3
         * 1> 4
         * 2> 5
         * 1> 6
         * 2> 7
         * 1> 8
         */
    }
}
```

## 4. 转换算子类型对比

### 4.1 基本转换算子

| 算子          | 功能    | 输入输出关系 | 使用场景        |
|-------------|-------|--------|-------------|
| **map**     | 一对一转换 | 1:1    | 数据格式转换、字段提取 |
| **filter**  | 过滤    | 1:0或1  | 数据清洗、条件筛选   |
| **flatMap** | 一对多转换 | 1:0或N  | 数据拆分、字符串分词  |

### 4.2 KeyBy 按键分区

| 算子        | 功能   | 返回值类型       | 使用场景           |
|-----------|------|-------------|----------------|
| **keyBy** | 按键分组 | KeyedStream | 数据分组、分流、为聚合做准备 |

**KeyBy 实现方式：**

- Lambda 表达式：`stream.keyBy(e -> e.user)`
- KeySelector 匿名类：`stream.keyBy(new KeySelector<Event, String>() {...})`
- 自定义 KeySelector 类：`stream.keyBy(new UserKeySelector())`

### 4.3 聚合转换算子

| 算子              | 功能         | 前置条件     | 使用场景          |
|-----------------|------------|----------|---------------|
| **reduce**      | 归约操作       | 需要 keyBy | 累加统计、自定义聚合    |
| **sum/max/min** | 聚合操作       | 需要 keyBy | 数值统计、分组聚合     |
| **maxBy/minBy** | 聚合操作（完整记录） | 需要 keyBy | 获取最大/最小值的完整记录 |

### 4.4 分区转换算子

| 算子                  | 功能    | 特点         | 使用场景      |
|---------------------|-------|------------|-----------|
| **shuffle**         | 随机分区  | 数据随机分布     | 负载均衡      |
| **rebalance**       | 轮询分区  | 数据轮询分布     | 负载均衡      |
| **rescale**         | 重缩放分区 | 局部重平衡      | 上下游并行度调整  |
| **broadcast**       | 广播分区  | 数据复制到所有任务  | 广播配置、小表关联 |
| **global**          | 全局分区  | 所有数据到第一个任务 | 全局聚合      |
| **partitionCustom** | 自定义分区 | 自定义分区逻辑    | 特殊分区需求    |

### 4.5 函数类型对比

| 函数类型              | 特点   | 生命周期方法     | 运行时上下文 |
|-------------------|------|------------|--------|
| **普通函数**          | 简单实现 | 无          | 无      |
| **Rich Function** | 富函数  | open/close | 有      |

## 5. 核心概念详解

### 5.1 MapFunction 接口

**接口定义：**

```java
public interface MapFunction<T, O> extends Function, Serializable {
    O map(T value) throws Exception;
}
```

**核心方法：**

- `map(T value)`：对输入元素进行转换，返回转换后的元素

**实现要点：**

1. 一个输入元素对应一个输出元素
2. 可以改变数据类型
3. 不能改变数据流的基数（元素数量）

### 5.2 FilterFunction 接口

**接口定义：**

```java
public interface FilterFunction<T> extends Function, Serializable {
    boolean filter(T value) throws Exception;
}
```

**核心方法：**

- `filter(T value)`：判断元素是否保留，返回 true 表示保留，false 表示过滤

**实现要点：**

1. 一个输入元素可能对应零个或一个输出元素
2. 不能改变数据类型
3. 可以改变数据流的基数（元素数量）

### 5.3 FlatMapFunction 接口

**接口定义：**

```java
public interface FlatMapFunction<T, O> extends Function, Serializable {
    void flatMap(T value, Collector<O> out) throws Exception;
}
```

**核心方法：**

- `flatMap(T value, Collector<O> out)`：对输入元素进行转换，通过 Collector 收集输出元素

**实现要点：**

1. 一个输入元素可以对应零个、一个或多个输出元素
2. 可以改变数据类型
3. 可以改变数据流的基数（元素数量）

### 5.4 KeySelector 接口

**接口定义：**

```java
public interface KeySelector<IN, KEY> extends Function, Serializable {
    KEY getKey(IN value) throws Exception;
}
```

**核心方法：**

- `getKey(IN value)`：从输入元素中提取 key

**实现要点：**

1. keyBy 通过 KeySelector 提取 key 进行分组
2. 可以使用 Lambda 表达式、匿名类或自定义类实现
3. 返回的 key 类型可以是任意类型（String、Integer、Tuple 等）
4. POJO 类型作为 key 时，必须重写 `hashCode()` 方法

### 5.6 ReduceFunction 接口

**接口定义：**

```java
public interface ReduceFunction<T> extends Function, Serializable {
    T reduce(T value1, T value2) throws Exception;
}
```

**核心方法：**

- `reduce(T value1, T value2)`：将两个元素合并为一个元素

**实现要点：**

1. 需要先使用 `keyBy()` 进行分组
2. 两个输入元素合并为一个输出元素
3. 必须满足结合律和交换律（对于有状态操作）

### 5.7 Rich Function 接口

**接口定义：**

```java
public abstract class RichFunction implements Function {
    public abstract void open(Configuration parameters) throws Exception;

    public abstract void close() throws Exception;

    public abstract RuntimeContext getRuntimeContext();
}
```

**核心方法：**

- `open(Configuration parameters)`：任务初始化时调用
- `close()`：任务结束时调用
- `getRuntimeContext()`：获取运行时上下文

**实现要点：**

1. 可以访问运行时信息（并行度、任务索引等）
2. 可以使用分布式缓存
3. 可以使用累加器
4. 可以初始化外部资源（如数据库连接）

### 5.8 物理分区策略

**分区策略说明：**

1. **shuffle()**：随机分区
    - 数据随机分布到下游并行任务
    - 适用于负载均衡

2. **rebalance()**：轮询分区
    - 数据轮询分布到下游并行任务
    - 适用于负载均衡

3. **rescale()**：重缩放分区
    - 在上下游并行度之间进行局部重平衡
    - 适用于上下游并行度调整

4. **broadcast()**：广播分区
    - 数据复制到所有下游并行任务
    - 适用于广播配置、小表关联

5. **global()**：全局分区
    - 所有数据发送到第一个并行任务
    - 适用于全局聚合

6. **partitionCustom()**：自定义分区
    - 根据自定义逻辑分区
    - 适用于特殊分区需求

## 6. 常见问题

### 6.1 Lambda 表达式类型推断问题

**问题：** 使用 Lambda 表达式返回复杂类型（如 `Tuple2`）时，Flink 无法推断类型

**解决方案：**

1. 使用 `.returns()` 显式指定返回类型
2. 使用类替代 Lambda 表达式
3. 使用匿名类替代 Lambda 表达式

### 6.2 Reduce 函数要求

**问题：** Reduce 函数必须满足结合律和交换律吗？

**答案：** 对于有状态操作（如窗口操作），Reduce 函数必须满足结合律和交换律。对于无状态操作，建议满足结合律和交换律，但不是必须的。

### 6.3 聚合函数区别

**问题：** `max()` 和 `maxBy()` 的区别是什么？

**答案：**

- `max()`：只更新指定字段，其他字段保持第一个元素的值
- `maxBy()`：返回完整记录，所有字段都更新为最大值对应的记录

### 6.4 物理分区选择

**问题：** 如何选择合适的物理分区策略？

**答案：**

- **负载均衡**：使用 `shuffle()` 或 `rebalance()`
- **广播数据**：使用 `broadcast()`
- **全局聚合**：使用 `global()`
- **特殊需求**：使用 `partitionCustom()`

## 7. 参考资料

- [Flink 官方文档 - 转换算子](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/)
- [Flink 官方文档 - 基本转换算子](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#基本转换算子)
- [Flink 官方文档 - KeyBy 和分组](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#keyby-和分组)
- [Flink 官方文档 - 物理分区](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#物理分区)
- [Flink 官方文档 - DataStream API](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/overview/)