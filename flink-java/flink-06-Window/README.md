<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 窗口（Window）演示](#flink-%E7%AA%97%E5%8F%A3window%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 窗口函数示例](#4-%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [4.1 窗口归约函数示例](#41-%E7%AA%97%E5%8F%A3%E5%BD%92%E7%BA%A6%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [4.2 窗口聚合函数示例](#42-%E7%AA%97%E5%8F%A3%E8%81%9A%E5%90%88%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [4.3 全窗口函数示例](#43-%E5%85%A8%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [4.4 增量聚合和全窗口函数结合使用示例](#44-%E5%A2%9E%E9%87%8F%E8%81%9A%E5%90%88%E5%92%8C%E5%85%A8%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0%E7%BB%93%E5%90%88%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B)
    - [4.5 处理迟到数据示例](#45-%E5%A4%84%E7%90%86%E8%BF%9F%E5%88%B0%E6%95%B0%E6%8D%AE%E7%A4%BA%E4%BE%8B)
  - [5. 窗口特性总结](#5-%E7%AA%97%E5%8F%A3%E7%89%B9%E6%80%A7%E6%80%BB%E7%BB%93)
    - [5.1 窗口的概念](#51-%E7%AA%97%E5%8F%A3%E7%9A%84%E6%A6%82%E5%BF%B5)
    - [5.2 窗口的分类](#52-%E7%AA%97%E5%8F%A3%E7%9A%84%E5%88%86%E7%B1%BB)
    - [5.3 窗口函数](#53-%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0)
    - [5.4 处理迟到数据](#54-%E5%A4%84%E7%90%86%E8%BF%9F%E5%88%B0%E6%95%B0%E6%8D%AE)
  - [6. 快速参考](#6-%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [6.1 代码位置](#61-%E4%BB%A3%E7%A0%81%E4%BD%8D%E7%BD%AE)
    - [6.2 关键 API](#62-%E5%85%B3%E9%94%AE-api)
    - [6.3 窗口分配器](#63-%E7%AA%97%E5%8F%A3%E5%88%86%E9%85%8D%E5%99%A8)
    - [6.4 学习建议](#64-%E5%AD%A6%E4%B9%A0%E5%BB%BA%E8%AE%AE)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 窗口（Window）演示

## 1. 项目作用

本项目演示了 Flink 中窗口（Window）的相关知识点，包括：

- 窗口的概念和分类
- 窗口分配器（Window Assigners）的使用
- 窗口函数（Window Functions）的使用
- 增量聚合函数和全窗口函数的结合使用
- 处理迟到数据的方法

通过实际代码示例帮助开发者快速掌握 Flink 窗口的创建和使用方法。

## 2. 项目结构

```
flink-06-Window/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── window/
│       ├── WindowReduceExample.java                    # 窗口归约函数示例
│       ├── WindowAggregateFunctionExample.java         # 窗口聚合函数示例
│       ├── UrlViewCount.java                           # URL 浏览量统计结果数据模型
│       ├── UrlViewCountExample.java                     # 增量聚合和全窗口函数结合示例
│       ├── UvCountByWindowExample.java                 # 全窗口函数示例
│       └── ProcessLateDataExample.java                 # 处理迟到数据示例
├── pom.xml                                              # Maven 配置
└── README.md                                            # 本文档
```

## 3. 核心实现

### 3.1 依赖配置

`pom.xml`：引入 Flink 核心依赖

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

#### 3.2.1 Event 类

`Event.java`：定义事件数据模型，包含用户、URL 和时间戳字段

```java
package com.action;

import java.sql.Timestamp;

/**
 * 事件数据模型
 * 用于演示 Flink 窗口相关功能
 */
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

#### 3.2.2 ClickSource 类

`ClickSource.java`：自定义数据源，生成有序的点击事件流

```java
package com.action;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源：生成有序的点击事件流
 * 用于演示窗口相关功能
 */
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

## 4. 窗口函数示例

### 4.1 窗口归约函数示例

文件路径：`src/main/java/com/action/window/WindowReduceExample.java`

功能说明：演示如何使用 `ReduceFunction` 进行窗口增量聚合。`ReduceFunction` 是最基本的聚合方式，将窗口中收集到的数据两两进行归约。

核心概念：

**增量聚合函数：**

- 每来一条数据就立即进行计算，中间只要保持一个简单的聚合状态就可以了
- 区别只是在于不立即输出结果，而是要等到窗口结束时间
- 等到窗口到了结束时间需要输出计算结果的时候，只需要拿出之前聚合的状态直接输出
- 这样可以大大提高程序运行的效率和实时性

**ReduceFunction：**

- 需要重写一个 `reduce` 方法，它的两个参数代表输入的两个元素
- 归约最终输出结果的数据类型，与输入的数据类型必须保持一致
- 也就是说，中间聚合的状态和输出的结果，都和输入的数据类型是一样的

代码实现

```java
package com.action.window;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 窗口归约函数示例
 * 演示如何使用 ReduceFunction 进行窗口增量聚合
 */
public class WindowReduceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，并提取时间戳、生成水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        // 将数据转换成二元组，方便计算
                        return Tuple2.of(value.user, 1L);
                    }
                })
                .keyBy(r -> r.f0)
                // 设置滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 定义累加规则，窗口闭合时，向下游发送累加结果
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

**（1）运行代码**

直接运行 `WindowReduceExample` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`reduce` 方法会被调用，更新聚合状态
- 当窗口到达结束时间时，输出累加结果
- 输出结果形式如下：

```
(Bob,1)
(Alice,2)
(Mary,2)
...
```

**（3）代码逻辑**

- 首先将数据转换成 `(user, count)` 的二元组形式，每条数据对应的初始 count 值都是 1
- 然后按照用户 id 分组，在事件时间下开滚动窗口，统计每 5 秒内的用户行为数量
- 对于窗口的计算，用 `ReduceFunction` 对 count 值做了增量聚合：窗口中会将当前的总 count 值保存成一个归约状态，每来一条数据，就会调用内部的 reduce 方法，将新数据中的 count 值叠加到状态上，并得到新的状态保存起来
- 等到了 5 秒窗口的结束时间，就把归约好的状态直接输出

注意事项

1. **数据类型一致性**：`ReduceFunction` 的输入、中间状态和输出结果的数据类型必须保持一致
2. **窗口类型**：这里使用的是滚动事件时间窗口，窗口大小为 5 秒
3. **水位线设置**：使用 `forBoundedOutOfOrderness(Duration.ZERO)` 表示有序流，延迟时间为 0

### 4.2 窗口聚合函数示例

文件路径：`src/main/java/com/action/window/WindowAggregateFunctionExample.java`

功能说明：演示如何使用 `AggregateFunction` 进行窗口聚合。`AggregateFunction` 可以看作是 `ReduceFunction` 的通用版本，输入类型、累加器类型和输出类型可以不同，使得应用更加灵活方便。

核心概念：

**AggregateFunction 的优势：**

- 取消了类型一致的限制，让输入数据、中间状态、输出结果三者类型都可以不同
- 可以定义多个状态，然后再基于这些聚合的状态计算出一个结果进行输出
- 例如计算平均值，可以把 sum 和 count 作为状态放入累加器，而在调用 `getResult` 方法时相除得到最终结果

**AggregateFunction 接口方法：**

- `createAccumulator()`：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次
- `add()`：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进一步聚合的过程。每条数据到来之后都会调用这个方法
- `getResult()`：从累加器中提取聚合的输出结果。这个方法只在窗口要输出结果时调用
- `merge()`：合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用；最常见的合并窗口的场景就是会话窗口（Session Windows）

代码实现

```java
package com.action.window;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

/**
 * 窗口聚合函数示例
 * 演示如何使用 AggregateFunction 计算人均 PV（页面浏览量）
 */
public class WindowAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 所有数据设置相同的 key，发送到同一个分区统计 PV和UV，再相除
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();

        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `WindowAggregateFunctionExample` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`add` 方法会被调用，更新聚合状态
- 当窗口到达结束时间时，`getResult` 方法会被调用，输出计算结果
- 输出结果形式如下：

```
1.0
1.6666666666666667
...
```

**（3）代码逻辑**

- 创建了事件时间滑动窗口，统计 10 秒钟的"人均 PV"，每 2 秒统计一次
- 为了统计 UV，用一个 `HashSet` 保存所有出现过的用户 id，实现自动去重
- 而 PV 的统计则类似一个计数器，每来一个数据加一就可以了
- 所以这里的状态，定义为包含一个 `HashSet` 和一个 count 值的二元组 `Tuple2<HashSet<String>, Long>`
- 每来一条数据，就将 user 存入 `HashSet`，同时 count 加 1
- 这里的 count 就是 PV，而 `HashSet` 中元素的个数（size）就是 UV
- 所以最终窗口的输出结果，就是它们的比值（PV/UV）

注意事项

1. **类型灵活性**：`AggregateFunction` 的输入类型（IN）、累加器类型（ACC）和输出类型（OUT）可以不同
2. **窗口类型**：这里使用的是滑动事件时间窗口，窗口大小为 10 秒，滑动步长为 2 秒
3. **merge 方法**：这里没有涉及会话窗口，所以 `merge()` 方法可以不做任何操作，返回 null

### 4.3 全窗口函数示例

文件路径：`src/main/java/com/action/window/UvCountByWindowExample.java`

功能说明：演示如何使用 `ProcessWindowFunction` 进行全窗口处理。全窗口函数需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。

核心概念：

**全窗口函数的特点：**

- 需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算
- 这是典型的批处理思路——先攒数据，等一批都到齐了再正式启动处理流程
- 全窗口函数的优势在于提供了更多的信息，可以认为是更加"通用"的窗口操作
- 输出的结果有可能要包含上下文中的一些信息（比如窗口的起始时间），这是增量聚合函数做不到的

**ProcessWindowFunction：**

- 是 Window API 中最底层的通用窗口函数接口
- 除了可以拿到窗口中的所有数据之外，还可以获取到一个"上下文对象"（Context）
- 这个上下文对象非常强大，不仅能够获取窗口信息，还可以访问当前的时间和状态信息
- 这里的时间就包括了处理时间（processing time）和事件时间水位线（event time watermark）

代码实现

```java
package com.action.window;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * 全窗口函数示例
 * 演示如何使用 ProcessWindowFunction 统计每小时 UV（独立访客数）
 */
public class UvCountByWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(java.time.Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 将数据全部发往同一分区，按窗口统计UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();

        env.execute();
    }

    // 自定义窗口处理函数
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            // 遍历所有数据，放到 Set里去重
            for (Event event: elements){
                userSet.add(event.user);
            }
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(" 窗口: " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " 的独立访客数量是 ：" + userSet.size());
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `UvCountByWindowExample` 类的 `main` 方法。

**（2）观察输出**

- 当窗口到达结束时间时，`process` 方法会被调用，处理窗口中的所有数据
- 输出结果形式如下：

```
窗口：...~...的独立访客数量是： 2
窗口：...~...的独立访客数量是： 3
...
```

**（3）代码逻辑**

- 使用的是事件时间语义
- 定义 10 秒钟的滚动事件窗口后，直接使用 `ProcessWindowFunction` 来定义处理的逻辑
- 创建一个 `HashSet`，将窗口所有数据的 userId 写入实现去重，最终得到 `HashSet` 的元素个数就是 UV 值
- 结合窗口信息，包装输出内容，包含窗口的起始和结束时间

注意事项

1. **性能考虑**：全窗口函数因为运行效率较低，很少直接单独使用，往往会和增量聚合函数结合在一起，共同实现窗口的处理计算
2. **窗口类型**：这里使用的是滚动事件时间窗口，窗口大小为 10 秒
3. **上下文信息**：`ProcessWindowFunction` 可以通过 `Context` 对象获取窗口信息、当前时间和状态信息

### 4.4 增量聚合和全窗口函数结合使用示例

文件路径：`src/main/java/com/action/window/UrlViewCountExample.java`

功能说明：演示如何将增量聚合函数和全窗口函数结合使用。这样既保证了处理性能和实时性，又支持了更加丰富的应用场景。

核心概念：

**结合使用的优势：**

- 增量聚合函数处理计算会更高效，相当于把计算量"均摊"到了窗口收集数据的过程中
- 全窗口函数的优势在于提供了更多的信息，可以认为是更加"通用"的窗口操作
- 结合使用可以兼具这两者的优点：既保证了处理性能和实时性，又支持了更加丰富的应用场景

**结合使用的机制：**

- 基于第一个参数（增量聚合函数）来处理窗口数据，每来一个数据就做一次聚合
- 等到窗口需要触发计算时，则调用第二个参数（全窗口函数）的处理逻辑输出结果
- 需要注意的是，这里的全窗口函数就不再缓存所有数据了，而是直接将增量聚合函数的结果拿来当作了 `Iterable` 类型的输入
- 一般情况下，这时的可迭代集合中就只有一个元素了

代码实现

**（1）数据模型**

`src/main/java/com/action/window/UrlViewCount.java`

```java
package com.action.window;

import java.sql.Timestamp;

/**
 * URL 浏览量统计结果数据模型
 */
public class UrlViewCount {
    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
```

**（2）使用示例**

`src/main/java/com/action/window/UrlViewCountExample.java`

```java
package com.action.window;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 增量聚合函数和全窗口函数结合使用示例
 * 演示如何统计每个 URL 的浏览量，并包含窗口信息
 */
public class UrlViewCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        // 需要按照url分组，开滑动窗口统计
        stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 同时传入增量聚合函数和全窗口函数
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义窗口处理函数， 只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `UrlViewCountExample` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`add` 方法会被调用，更新聚合状态
- 当窗口到达结束时间时，`process` 方法会被调用，包装窗口信息并输出结果
- 输出结果形式如下：

```
UrlViewCount{url='./home', count=3, windowStart=2021-07-01 14:44:10.0, windowEnd=2021-07-01 14:44:20.0}
UrlViewCount{url='./cart', count=2, windowStart=2021-07-01 14:44:10.0, windowEnd=2021-07-01 14:44:20.0}
...
```

**（3）代码逻辑**

- 用一个 `AggregateFunction` 来实现增量聚合，每来一个数据就计数加一
- 得到的结果交给 `ProcessWindowFunction`，结合窗口信息包装成我们想要的 `UrlViewCount`，最终输出统计结果
- 窗口处理的主体还是增量聚合，而引入全窗口函数又可以获取到更多的信息包装输出

注意事项

1. **性能优势**：窗口处理的主体还是增量聚合，保证了处理性能和实时性
2. **功能丰富**：引入全窗口函数可以获取到更多的信息（如窗口起始和结束时间）包装输出
3. **窗口类型**：这里使用的是滑动事件时间窗口，窗口大小为 10 秒，滑动步长为 5 秒

### 4.5 处理迟到数据示例

文件路径：`src/main/java/com/action/window/ProcessLateDataExample.java`

功能说明：演示如何使用水位线延迟、窗口允许延迟和侧输出流来处理迟到数据。Flink 处理迟到数据对于结果的正确性有三重保障：水位线的延迟、窗口允许迟到数据，以及将迟到数据放入窗口侧输出流。

核心概念：

**处理迟到数据的三重保障：**

1. **设置水位线延迟时间**：水位线延迟设置的比较小，用来对付分布式网络传输导致的数据乱序，一般设在毫秒~秒级
2. **允许窗口处理迟到数据**：窗口可以设置延迟时间，允许继续处理迟到数据。由于大部分乱序数据已经被水位线的延迟等到了，所以往往迟到的数据不会太多
3. **将迟到数据放入侧输出流**：即使有了前面的双重保证，可窗口不能一直等下去，最后总要真正关闭。窗口一旦关闭，后续的数据就都要被丢弃了。用窗口的侧输出流来收集关窗以后的迟到数据，这种方式是最后"兜底"的方法，只能保证数据不丢失

代码实现

```java
package com.action.window;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 处理迟到数据示例
 * 演示如何使用水位线延迟、窗口允许延迟和侧输出流来处理迟到数据
 * 在终端执行以下命令启动 Socket 服务器：
 * 运行说明
 * （1）启动 Socket 服务器*
 * 在终端执行以下命令启动 Socket 服务器：
 * nc -lk 7777
 * （2）运行代码
 * 直接运行 `ProcessLateDataExample` 类的 `main` 方法。
 * （3）输入测试数据
 * 在 Socket 服务器终端依次输入以下数据（注意：数据格式为 `user url timestamp`，用空格分隔）：
 * ```
 * Alice ./home 1000
 * Alice ./home 2000
 * Alice ./home 10000
 * Alice ./home 9000
 * Alice ./cart 12000
 * Alice ./prod?id=100 15000
 * Alice ./home 9000
 * Alice ./home 8000
 * Alice ./prod?id=200 70000
 * Alice ./home 8000
 * Alice ./prod?id=300 72000
 * Alice ./home 8000
 * ```
 */
public class ProcessLateDataExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取socket 文本流
        SingleOutputStreamOperator<Event> stream =
                env.socketTextStream("server01", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                // 方式一：设置 watermark 延迟时间 ，2秒钟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {
        };

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 方式二：允许窗口处理迟到数据 ，设置1分钟的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三： 将最后的迟到数据输出到 侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        result.print("result");
        result.getSideOutput(outputTag).print("late");

        // 为方便观察， 可以将原始数据也输出
        stream.print("input");

        env.execute();
    }

    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }
}
```

运行说明

**（1）启动 Socket 服务器**

在终端执行以下命令启动 Socket 服务器：

```bash
nc -lk 7777
```

**（2）运行代码**

直接运行 `ProcessLateDataExample` 类的 `main` 方法。

**（3）输入测试数据**

在 Socket 服务器终端依次输入以下数据（注意：数据格式为 `user url timestamp`，用空格分隔）：

```
Alice ./home 1000
Alice ./home 2000
Alice ./home 10000
Alice ./home 9000
Alice ./cart 12000
Alice ./prod?id=100 15000
Alice ./home 9000
Alice ./home 8000
Alice ./prod?id=200 70000
Alice ./home 8000
Alice ./prod?id=300 72000
Alice ./home 8000
```

**（4）观察输出**

- 当输入数据 `Alice ./home 10000` 时，时间戳为 10000，由于设置了 2 秒钟的水位线延迟时间，所以此时水位线到达了 8 秒，并没有触发 `[0, 10s)` 窗口的计算
- 所以接下来时间戳为 9000 的数据到来，同样可以直接进入窗口做增量聚合
- 当时间戳为 12000 的数据到来时，水位线到达了 10000，所以触发了 `[0, 10s)` 窗口的计算，第一次输出了窗口统计结果
- 不过窗口触发计算之后并没有关闭销毁，而是继续等待迟到数据
- 之后时间戳为 9000 的数据继续到来，会立即输出一条更新后的统计结果
- 我们设置窗口等待的时间为 1 分钟，所以当时间推进到 70000 时，窗口就会真正被销毁
- 此前的所有迟到数据可以直接更新窗口的计算结果，而之后的迟到数据已经无法整合进窗口，就只能用侧输出流来捕获了

注意事项

1. **数据源**：这个示例使用 Socket 文本流作为数据源，需要先启动 Socket 服务器
2. **数据格式**：输入数据格式为 `user url timestamp`，用空格分隔
3. **三重保障**：水位线的延迟、窗口允许迟到数据，以及将迟到数据放入窗口侧输出流，这三重保障可以确保结果的正确性

## 5. 窗口特性总结

### 5.1 窗口的概念

窗口是用来处理无界流的核心。窗口可以把流切割成有限大小的多个"存储桶"（bucket）；每个数据都会分发到对应的桶中，当到达窗口结束时间时，就对每个桶中收集的数据进行计算处理。

### 5.2 窗口的分类

**按照驱动类型分类：**

1. **时间窗口（Time Window）**：以时间点来定义窗口的开始和结束，所以截取出的就是某一时间段的数据
2. **计数窗口（Count Window）**：基于元素的个数来截取数据，到达固定的个数时就触发计算并关闭窗口

**按照窗口分配数据的规则分类：**

1. **滚动窗口（Tumbling Windows）**：有固定的大小，窗口之间没有重叠，也不会有间隔，是"首尾相接"的状态
2. **滑动窗口（Sliding Windows）**：窗口大小也是固定的，但窗口之间可以"错开"一定的位置，会出现重叠
3. **会话窗口（Session Windows）**：基于"会话"来对数据进行分组，如果一段时间一直没收到数据，那就认为会话超时失效，窗口自动关闭
4. **全局窗口（Global Windows）**：全局有效，会把相同 key 的所有数据都分配到同一个窗口中，默认是不会做触发计算的

### 5.3 窗口函数

**增量聚合函数：**

- `ReduceFunction`：最基本的聚合方式，将窗口中收集到的数据两两进行归约
- `AggregateFunction`：可以看作是 `ReduceFunction` 的通用版本，输入类型、累加器类型和输出类型可以不同

**全窗口函数：**

- `WindowFunction`：老版本的通用窗口函数接口，可以获取到包含窗口所有数据的可迭代集合，还可以拿到窗口本身的信息
- `ProcessWindowFunction`：Window API 中最底层的通用窗口函数接口，除了可以拿到窗口中的所有数据之外，还可以获取到一个"上下文对象"（Context）

### 5.4 处理迟到数据

Flink 处理迟到数据对于结果的正确性有三重保障：

1. **设置水位线延迟时间**：水位线延迟设置的比较小，用来对付分布式网络传输导致的数据乱序
2. **允许窗口处理迟到数据**：窗口可以设置延迟时间，允许继续处理迟到数据
3. **将迟到数据放入侧输出流**：用窗口的侧输出流来收集关窗以后的迟到数据，这是最后"兜底"的方法

## 6. 快速参考

### 6.1 代码位置

- **Event 类**：`src/main/java/com/action/Event.java`
- **ClickSource 类**：`src/main/java/com/action/ClickSource.java`
- **窗口归约函数示例**：`src/main/java/com/action/window/WindowReduceExample.java`
- **窗口聚合函数示例**：`src/main/java/com/action/window/WindowAggregateFunctionExample.java`
- **URL 浏览量统计结果数据模型**：`src/main/java/com/action/window/UrlViewCount.java`
- **增量聚合和全窗口函数结合示例**：`src/main/java/com/action/window/UrlViewCountExample.java`
- **全窗口函数示例**：`src/main/java/com/action/window/UvCountByWindowExample.java`
- **处理迟到数据示例**：`src/main/java/com/action/window/ProcessLateDataExample.java`

### 6.2 关键 API

- `.window(WindowAssigner)`：定义窗口分配器，得到 `WindowedStream`
- `.reduce(ReduceFunction)`：使用归约函数进行窗口聚合
- `.aggregate(AggregateFunction)`：使用聚合函数进行窗口聚合
- `.aggregate(AggregateFunction, ProcessWindowFunction)`：增量聚合函数和全窗口函数结合使用
- `.process(ProcessWindowFunction)`：使用全窗口函数进行窗口处理
- `.allowedLateness(Time)`：允许窗口处理迟到数据
- `.sideOutputLateData(OutputTag)`：将迟到数据放入侧输出流
- `.getSideOutput(OutputTag)`：获取侧输出流

### 6.3 窗口分配器

**时间窗口：**

- `TumblingEventTimeWindows.of(Time)`：滚动事件时间窗口
- `TumblingProcessingTimeWindows.of(Time)`：滚动处理时间窗口
- `SlidingEventTimeWindows.of(Time, Time)`：滑动事件时间窗口
- `SlidingProcessingTimeWindows.of(Time, Time)`：滑动处理时间窗口
- `EventTimeSessionWindows.withGap(Time)`：事件时间会话窗口
- `ProcessingTimeSessionWindows.withGap(Time)`：处理时间会话窗口

**计数窗口：**

- `.countWindow(long)`：滚动计数窗口
- `.countWindow(long, long)`：滑动计数窗口

### 6.4 学习建议

1. 先理解窗口的概念和分类
2. 掌握窗口分配器的使用
3. 理解增量聚合函数和全窗口函数的区别和适用场景
4. 学习增量聚合函数和全窗口函数的结合使用
5. 了解处理迟到数据的方法
6. 注意窗口的生命周期和触发机制

## 7. 参考资料

- [窗口 | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/)
- [窗口函数 | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/#窗口函数window-functions)

