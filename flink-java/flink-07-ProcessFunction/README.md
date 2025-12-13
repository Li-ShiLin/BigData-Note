<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 处理函数（ProcessFunction）演示](#flink-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0processfunction%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 处理函数示例](#4-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [4.1 ProcessFunction 基本使用示例](#41-processfunction-%E5%9F%BA%E6%9C%AC%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B)
    - [4.2 处理时间定时器示例](#42-%E5%A4%84%E7%90%86%E6%97%B6%E9%97%B4%E5%AE%9A%E6%97%B6%E5%99%A8%E7%A4%BA%E4%BE%8B)
    - [4.3 事件时间定时器示例](#43-%E4%BA%8B%E4%BB%B6%E6%97%B6%E9%97%B4%E5%AE%9A%E6%97%B6%E5%99%A8%E7%A4%BA%E4%BE%8B)
    - [4.4 使用 ProcessAllWindowFunction 实现 Top N](#44-%E4%BD%BF%E7%94%A8-processallwindowfunction-%E5%AE%9E%E7%8E%B0-top-n)
    - [4.5 使用 KeyedProcessFunction 实现 Top N](#45-%E4%BD%BF%E7%94%A8-keyedprocessfunction-%E5%AE%9E%E7%8E%B0-top-n)
  - [5. 处理函数特性总结](#5-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E7%89%B9%E6%80%A7%E6%80%BB%E7%BB%93)
    - [5.1 处理函数的概念](#51-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E7%9A%84%E6%A6%82%E5%BF%B5)
    - [5.2 处理函数的分类](#52-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E7%9A%84%E5%88%86%E7%B1%BB)
    - [5.3 定时器和定时服务](#53-%E5%AE%9A%E6%97%B6%E5%99%A8%E5%92%8C%E5%AE%9A%E6%97%B6%E6%9C%8D%E5%8A%A1)
    - [5.4 状态管理](#54-%E7%8A%B6%E6%80%81%E7%AE%A1%E7%90%86)
  - [6. 快速参考](#6-%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [6.1 代码位置](#61-%E4%BB%A3%E7%A0%81%E4%BD%8D%E7%BD%AE)
    - [6.2 关键 API](#62-%E5%85%B3%E9%94%AE-api)
    - [6.3 处理函数的方法](#63-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E7%9A%84%E6%96%B9%E6%B3%95)
    - [6.4 学习建议](#64-%E5%AD%A6%E4%B9%A0%E5%BB%BA%E8%AE%AE)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 处理函数（ProcessFunction）演示

## 1. 项目作用

本项目演示了 Flink 中处理函数（ProcessFunction）的相关知识点，包括：

- ProcessFunction 的基本使用
- KeyedProcessFunction 和定时器的使用
- ProcessWindowFunction 的使用
- Top N 应用案例的实现
- 侧输出流的使用

通过实际代码示例帮助开发者快速掌握 Flink 处理函数的创建和使用方法。

## 2. 项目结构

```
flink-07-ProcessFunction/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── ProcessFunction/
│       ├── ProcessFunctionExample.java                  # ProcessFunction 基本示例
│       ├── ProcessingTimeTimerTest.java                  # 处理时间定时器示例
│       ├── EventTimeTimerTest.java                      # 事件时间定时器示例
│       ├── UrlViewCount.java                            # URL 浏览量统计结果数据模型
│       ├── ProcessAllWindowTopN.java                    # 使用 ProcessAllWindowFunction 实现 Top N
│       └── KeyedProcessTopN.java                        # 使用 KeyedProcessFunction 实现 Top N
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
 * 用于演示 Flink 处理函数相关功能
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
 * 用于演示处理函数相关功能
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

## 4. 处理函数示例

### 4.1 ProcessFunction 基本使用示例

文件路径：`src/main/java/com/action/ProcessFunction/ProcessFunctionExample.java`

功能说明：演示如何使用 `ProcessFunction` 进行数据处理。`ProcessFunction` 是最基本的处理函数，提供了对数据流中事件、时间戳、水位线的完全控制权。

核心概念：

**ProcessFunction 的特点：**

- 提供了一个"定时服务"（TimerService），可以访问流中的事件（event）、时间戳（timestamp）、水位线（watermark），甚至可以注册"定时事件"
- 继承了 `AbstractRichFunction` 抽象类，所以拥有富函数类的所有特性，同样可以访问状态（state）和其他运行时信息
- 还可以直接将数据输出到侧输出流（side output）中
- 处理函数是最为灵活的处理方法，可以实现各种自定义的业务逻辑

**ProcessFunction 的方法：**

- `processElement()`：用于"处理元素"，定义了处理的核心逻辑。这个方法对于流中的每个元素都会调用一次
- `onTimer()`：用于定义定时触发的操作，这个方法只有在注册好的定时器触发的时候才会调用

代码实现

```java
package com.action.ProcessFunction;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction 基本使用示例
 * 演示如何使用 ProcessFunction 进行数据处理
 */
public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (value.user.equals("Mary")) {
                            out.collect(value.user);
                        } else if (value.user.equals("Bob")) {
                            out.collect(value.user);
                            out.collect(value.user);
                        }
                        System.out.println(ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

**（1）运行代码**

直接运行 `ProcessFunctionExample` 类的 `main` 方法。

**（2）观察输出**

- 当数据的 user 为 "Mary" 时，将其输出一次
- 当数据的 user 为 "Bob" 时，将 user 输出两次
- 每次处理数据时，都会打印当前的水位线信息

**（3）代码逻辑**

- `ProcessFunction` 函数有点像 `FlatMapFunction` 的升级版，可以实现 Map、Filter、FlatMap 的所有功能
- 通过 `ctx.timerService().currentWatermark()` 可以获取当前的水位线
- 通过 `out.collect()` 可以向下游发出数据，这个方法可以调用任意多次

注意事项

1. **功能强大**：`ProcessFunction` 非常强大，能够做很多之前做不到的事情
2. **定时器限制**：在 `ProcessFunction` 中虽然可以访问 `TimerService`，但只有基于 `KeyedStream` 的处理函数才能注册和删除定时器
3. **性能考虑**：处理函数比较抽象，没有具体的操作，所以对于一些常见的简单应用（比如求和、开窗口）会显得有些麻烦

### 4.2 处理时间定时器示例

文件路径：`src/main/java/com/action/ProcessFunction/ProcessingTimeTimerTest.java`

功能说明：演示如何使用 `KeyedProcessFunction` 注册和处理时间定时器。定时器是处理函数中进行时间相关操作的主要机制。

核心概念：

**定时器（Timer）和定时服务（TimerService）：**

- 定时器是处理函数中进行时间相关操作的主要机制
- 在 `.onTimer()` 方法中可以实现定时处理的逻辑，而它能触发的前提，就是之前曾经注册过定时器、并且现在已经到了触发时间
- 注册定时器的功能，是通过上下文中提供的"定时服务"（TimerService）来实现的

**TimerService 的方法：**

- `currentProcessingTime()`：获取当前的处理时间
- `currentWatermark()`：获取当前的水位线（事件时间）
- `registerProcessingTimeTimer(long time)`：注册处理时间定时器，当处理时间超过 time 时触发
- `registerEventTimeTimer(long time)`：注册事件时间定时器，当水位线超过 time 时触发
- `deleteProcessingTimeTimer(long time)`：删除触发时间为 time 的处理时间定时器
- `deleteEventTimeTimer(long time)`：删除触发时间为 time 的事件时间定时器

代码实现

```java
package com.action.ProcessFunction;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 处理时间定时器示例
 * 演示如何使用 KeyedProcessFunction 注册和处理时间定时器
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和 watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 要用定时器，必须基于 KeyedStream
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timerService().currentProcessingTime();
                        out.collect(" 数据到达，到达时间： " + new Timestamp(currTs));
                        // 注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(" 定时器触发， 触发时间： " + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

**（1）运行代码**

直接运行 `ProcessingTimeTimerTest` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`processElement` 方法会被调用，输出"数据到达"的信息，并注册一个 10 秒后的定时器
- 等待 10 秒之后，`onTimer` 方法会被调用，输出"定时器触发"的信息
- 输出结果形式如下：

```
数据到达，到达时间： 2021-07-01 15:24:15.0
数据到达，到达时间： 2021-07-01 15:24:16.0
...
定时器触发， 触发时间： 2021-07-01 15:24:25.0
定时器触发， 触发时间： 2021-07-01 15:24:26.0
...
```

**（3）代码逻辑**

- 由于定时器只能在 `KeyedStream` 上使用，所以先要进行 `keyBy`
- 这里的 `.keyBy(data -> true)` 是将所有数据的 key 都指定为了 true，其实就是所有数据拥有相同的 key，会分配到同一个分区
- 之后自定义了一个 `KeyedProcessFunction`，其中 `.processElement()` 方法是每来一个数据都会调用一次，主要是定义了一个 10 秒之后的定时器
- 而 `.onTimer()` 方法则会在定时器触发时调用

注意事项

1. **定时器去重**：`TimerService` 会以键（key）和时间戳为标准，对定时器进行去重。也就是说对于每个 key 和时间戳，最多只有一个定时器，如果注册了多次，`onTimer()` 方法也将只被调用一次
2. **定时器精度**：定时器默认的区分精度是毫秒
3. **容错性**：Flink 的定时器同样具有容错性，它和状态一起都会被保存到一致性检查点（checkpoint）中

### 4.3 事件时间定时器示例

文件路径：`src/main/java/com/action/ProcessFunction/EventTimeTimerTest.java`

功能说明：演示如何使用 `KeyedProcessFunction` 注册和事件时间定时器。事件时间语义下，定时器触发的条件就是水位线推进到设定的时间。

核心概念：

**事件时间定时器的特点：**

- 事件时间语义下，定时器触发的条件就是水位线推进到设定的时间
- 数据到来之后，当前的水位线与时间戳并不是一致的
- 水位线的生成是周期性的（默认 200ms 一次），不会立即发生改变
- 当水位线推进到设定的时间时，定时器才会触发

代码实现

```java
package com.action.ProcessFunction;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * 事件时间定时器示例
 * 演示如何使用 KeyedProcessFunction 注册和事件时间定时器
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 基于KeyedStream 定义事件时间定时器
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(" 数据到达，时间戳为： " + ctx.timestamp());
                        out.collect(" 数据到达，水位线为： " + ctx.timerService().currentWatermark() + " \n ------- 分割线-------");
                        // 注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(" 定时器触发，触发时间： " + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿 5秒钟
            Thread.sleep(5000L);

            // 发出10秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10秒+1ms后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() { }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `EventTimeTimerTest` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`processElement` 方法会被调用，输出数据的时间戳和当前水位线信息
- 当水位线推进到设定的时间时，`onTimer` 方法会被调用，输出"定时器触发"的信息
- 输出结果形式如下：

```
数据到达，时间戳为： 1000
数据到达，水位线为： -9223372036854775808
 ------- 分割线-------
数据到达，时间戳为： 11000
数据到达，水位线为： 999
 ------- 分割线-------
数据到达，时间戳为： 11001
数据到达，水位线为： 10999
 ------- 分割线-------
定时器触发，触发时间： 11000
定时器触发，触发时间： 21000
定时器触发，触发时间： 21001
```

**（3）代码逻辑**

- 第一条数据到来后，设定的定时器时间为 1000 + 10 * 1000 = 11000
- 当时间戳为 11000 的第二条数据到来，水位线还处在 999 的位置，当然不会立即触发定时器
- 而之后水位线会推进到 10999，同样是无法触发定时器的
- 必须等到第三条数据到来，将水位线真正推进到 11000，就可以触发第一个定时器了
- 第三条数据发出后再过 5 秒，没有更多的数据生成了，整个程序运行结束将要退出，此时 Flink 会自动将水位线推进到长整型的最大值（Long.MAX_VALUE）。于是所有尚未触发的定时器这时就统一触发了

注意事项

1. **水位线延迟**：数据到来之后，当前的水位线与时间戳并不是一致的，水位线总是"滞后"数据
2. **定时器触发条件**：事件时间语义下，定时器触发的条件就是水位线推进到设定的时间
3. **程序退出时的处理**：当程序运行结束将要退出时，Flink 会自动将水位线推进到长整型的最大值，所有尚未触发的定时器这时就统一触发了

### 4.4 使用 ProcessAllWindowFunction 实现 Top N

文件路径：`src/main/java/com/action/ProcessFunction/ProcessAllWindowTopN.java`

功能说明：演示如何使用 `ProcessAllWindowFunction` 实现 Top N 统计。统计最近 10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次。

核心概念：

**Top N 问题：**

- 网站中一个非常经典的例子，就是实时统计一段时间内的热门 url
- 需要统计最近 10 秒钟内最热门的两个 url 链接，并且每 5 秒钟更新一次
- 这可以用一个滑动窗口来实现，而"热门度"一般可以直接用访问量来表示

**ProcessAllWindowFunction 的特点：**

- 基于 `AllWindowedStream` 调用 `.process()` 方法
- 相当于对没有 keyBy 的数据流直接开窗并调用 `.process()` 方法
- 不区分 url 链接，而是将所有访问数据都收集起来，统一进行统计计算
- 在窗口中可以用一个 HashMap 来保存每个 url 的访问次数，只要遍历窗口中的所有数据，自然就能得到所有 url 的热门度

代码实现

```java
package com.action.ProcessFunction;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * 使用 ProcessAllWindowFunction 实现 Top N 示例
 * 演示如何统计最近10秒钟内最热门的两个 url链接，并且每 5秒钟更新一次
 */
public class ProcessAllWindowTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // 只需要url就可以统计数量，所以转换成String直接开窗统计
        SingleOutputStreamOperator<String> result = eventStream
                .map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.url;
                    }
                })
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))    // 开滑动窗口
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        HashMap<String, Long> urlCountMap = new HashMap<>();
                        // 遍历窗口中数据，将浏览量保存到一个 HashMap 中
                        for (String url : elements) {
                            if (urlCountMap.containsKey(url)) {
                                long count = urlCountMap.get(url);
                                urlCountMap.put(url, count + 1L);
                            } else {
                                urlCountMap.put(url, 1L);
                            }
                        }

                        ArrayList<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
                        // 将浏览量数据放入ArrayList，进行排序
                        for (String key : urlCountMap.keySet()) {
                            mapList.add(Tuple2.of(key, urlCountMap.get(key)));
                        }

                        mapList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });

                        // 取排序后的前两名，构建输出结果
                        StringBuilder result = new StringBuilder();
                        result.append("========================================\n");
                        for (int i = 0; i < 2; i++) {
                            Tuple2<String, Long> temp = mapList.get(i);
                            String info = "浏览量No." + (i + 1) +
                                    " url：" + temp.f0 +
                                    " 浏览量：" + temp.f1 +
                                    " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";

                            result.append(info);
                        }
                        result.append("========================================\n");
                        out.collect(result.toString());
                    }
                });

        result.print();

        env.execute();
    }
}
```

运行说明

**（1）运行代码**

直接运行 `ProcessAllWindowTopN` 类的 `main` 方法。

**（2）观察输出**

- 当窗口到达结束时间时，`process` 方法会被调用，处理窗口中的所有数据
- 输出结果形式如下：

```
========================================
浏览量No.1 url：./prod?id=1 浏览量：2 窗口结束时间： 2021-07-01 15:24:25.0
浏览量No.2 url：./cart 浏览量：1 窗口结束时间： 2021-07-01 15:24:25.0
========================================
```

**（3）代码逻辑**

- 使用滑动窗口，窗口大小为 10 秒，滑动步长为 5 秒
- 在窗口中用一个 HashMap 来保存每个 url 的访问次数，遍历窗口中的所有数据，自然就能得到所有 url 的热门度
- 最后把 HashMap 转成一个列表 ArrayList，然后进行排序、取出前两名输出就可以了

注意事项

1. **并行度限制**：没有进行按键分区，直接将所有数据放在一个分区上进行了开窗操作。这相当于将并行度强行设置为 1，在实际应用中是要尽量避免的
2. **性能考虑**：在全窗口函数中定义了 HashMap 来统计 url 链接的浏览量，计算过程是要先收集齐所有数据、然后再逐一遍历更新 HashMap，这显然不够高效
3. **适用场景**：这种方式实现简单，但性能较差，适合数据量较小的场景

### 4.5 使用 KeyedProcessFunction 实现 Top N

文件路径：`src/main/java/com/action/ProcessFunction/KeyedProcessTopN.java`

功能说明：演示如何通过增量聚合和 `KeyedProcessFunction` 实现更高效的 Top N 统计。这种方式可以充分利用并行计算的优势，并且使用增量聚合提高处理效率。

核心概念：

**优化思路：**

- 对数据进行按键分区，分别统计浏览量
- 进行增量聚合，得到结果最后再做排序输出
- 使用增量聚合函数 `AggregateFunction` 进行浏览量的统计，然后结合 `ProcessWindowFunction` 排序输出来实现 Top N 的需求

**处理流程：**

1. 读取数据源
2. 提取时间戳并生成水位线
3. 按照 url 进行 keyBy 分区操作
4. 开长度为 10 秒、步长为 5 秒的事件时间滑动窗口
5. 使用增量聚合函数 `AggregateFunction`，并结合全窗口函数 `ProcessWindowFunction` 进行窗口聚合，得到每个 url、在每个统计窗口内的浏览量，包装成 `UrlViewCount`
6. 按照窗口进行 keyBy 分区操作
7. 对同一窗口的统计结果数据，使用 `KeyedProcessFunction` 进行收集并排序输出

**状态管理：**

- 使用自定义的"列表状态"（ListState）来进行存储
- 每来一个 `UrlViewCount`，就把它添加到当前的列表状态中，并注册一个触发时间为窗口结束时间加 1 毫秒（windowEnd + 1）的定时器
- 待到水位线到达这个时间，定时器触发，可以保证当前窗口所有 url 的统计结果 `UrlViewCount` 都到齐了；于是从状态中取出进行排序输出

代码实现

**（1）数据模型**

`src/main/java/com/action/ProcessFunction/UrlViewCount.java`

```java
package com.action.ProcessFunction;

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

`src/main/java/com/action/ProcessFunction/KeyedProcessTopN.java`

```java
package com.action.ProcessFunction;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 使用 KeyedProcessFunction 实现 Top N 示例
 * 演示如何通过增量聚合和 KeyedProcessFunction 实现更高效的 Top N 统计
 */
public class KeyedProcessTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 需要按照url分组，求出每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream =
                eventStream.keyBy(data -> data.url)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .aggregate(new UrlViewCountAgg(),
                                new UrlViewCountResult());


        // 对结果中同一个窗口的统计数据，进行排序处理
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopN(2));

        result.print("result");

        env.execute();
    }

    // 自定义增量聚合
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

    // 自定义全窗口函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    // 自定义处理函数，排序取top n
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 将n作为属性
        private Integer n;
        // 定义一个列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-view-count-list",
                            Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);
            // 注册 window end + 1ms后的定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出，放入ArrayList，方便排序
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 清空状态，释放资源
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + UrlViewCount.url + " "
                        + "浏览量：" + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `KeyedProcessTopN` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`add` 方法会被调用，更新聚合状态
- 当窗口到达结束时间时，`process` 方法会被调用，包装窗口信息并输出结果
- 当定时器触发时，`onTimer` 方法会被调用，进行排序并输出 Top N 结果
- 输出结果形式如下：

```
result> ========================================
窗口结束时间：2021-07-01 15:24:25.0
No.1 url：./prod?id=1 浏览量：3
No.2 url：./cart 浏览量：2
========================================
```

**（3）代码逻辑**

- 先按照 url 对数据进行 keyBy 分区，然后开窗进行增量聚合
- 使用增量聚合函数 `AggregateFunction` 来实现增量聚合，每来一个数据就计数加一
- 得到的结果交给 `ProcessWindowFunction`，结合窗口信息包装成我们想要的 `UrlViewCount`
- 之后按照窗口结束时间进行 keyBy 分区，使用 `KeyedProcessFunction` 进行收集并排序输出
- 使用列表状态（ListState）来缓存同一窗口的统计结果，等待所有数据到齐后再进行排序

注意事项

1. **性能优势**：这种方式可以充分利用并行计算的优势，并且使用增量聚合提高处理效率
2. **状态管理**：使用列表状态来缓存数据，需要在定时器触发时清空状态，释放资源
3. **定时器去重**：针对同一 key、同一时间戳会进行去重，所以对于同一个窗口而言，我们接到统计结果数据后设定的 windowEnd + 1 的定时器都是一样的，最终只会触发一次计算

## 5. 处理函数特性总结

### 5.1 处理函数的概念

处理函数是 Flink 底层 API，提供了对流处理最为本质的组成部分的完全控制权。在处理函数中，我们直面的就是数据流中最基本的元素：数据事件（event）、状态（state）以及时间（time）。

### 5.2 处理函数的分类

Flink 提供了 8 个不同的处理函数：

1. **ProcessFunction**：最基本的处理函数，基于 DataStream 直接调用 `.process()` 时作为参数传入
2. **KeyedProcessFunction**：对流按键分区后的处理函数，基于 KeyedStream 调用 `.process()` 时作为参数传入。要想使用定时器，必须基于 KeyedStream
3. **ProcessWindowFunction**：开窗之后的处理函数，也是全窗口函数的代表。基于 WindowedStream 调用 `.process()` 时作为参数传入
4. **ProcessAllWindowFunction**：同样是开窗之后的处理函数，基于 AllWindowedStream 调用 `.process()` 时作为参数传入
5. **CoProcessFunction**：合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用 `.process()` 时作为参数传入
6. **ProcessJoinFunction**：间隔连接（interval join）两条流之后的处理函数，基于 IntervalJoined 调用 `.process()` 时作为参数传入
7. **BroadcastProcessFunction**：广播连接流处理函数，基于 BroadcastConnectedStream 调用 `.process()` 时作为参数传入
8. **KeyedBroadcastProcessFunction**：按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用 `.process()` 时作为参数传入

### 5.3 定时器和定时服务

**定时器（Timer）：**

- 定时器是处理函数中进行时间相关操作的主要机制
- 在 `.onTimer()` 方法中可以实现定时处理的逻辑，而它能触发的前提，就是之前曾经注册过定时器、并且现在已经到了触发时间

**定时服务（TimerService）：**

- 提供了获取当前时间、注册定时器、删除定时器的方法
- 基于处理时间和基于事件时间两种类型的定时器
- 只有基于 KeyedStream 的处理函数，才能去调用注册和删除定时器的方法

### 5.4 状态管理

处理函数继承了 `AbstractRichFunction` 抽象类，所以拥有富函数类的所有特性，同样可以访问状态（state）和其他运行时信息。可以使用各种状态类型，如：

- **ValueState**：值状态
- **ListState**：列表状态
- **MapState**：映射状态
- **ReducingState**：归约状态
- **AggregatingState**：聚合状态

## 6. 快速参考

### 6.1 代码位置

- **Event 类**：`src/main/java/com/action/Event.java`
- **ClickSource 类**：`src/main/java/com/action/ClickSource.java`
- **ProcessFunction 基本示例**：`src/main/java/com/action/ProcessFunction/ProcessFunctionExample.java`
- **处理时间定时器示例**：`src/main/java/com/action/ProcessFunction/ProcessingTimeTimerTest.java`
- **事件时间定时器示例**：`src/main/java/com/action/ProcessFunction/EventTimeTimerTest.java`
- **URL 浏览量统计结果数据模型**：`src/main/java/com/action/ProcessFunction/UrlViewCount.java`
- **使用 ProcessAllWindowFunction 实现 Top N**：`src/main/java/com/action/ProcessFunction/ProcessAllWindowTopN.java`
- **使用 KeyedProcessFunction 实现 Top N**：`src/main/java/com/action/ProcessFunction/KeyedProcessTopN.java`

### 6.2 关键 API

- `.process(ProcessFunction)`：基于 DataStream 调用，传入 ProcessFunction
- `.process(KeyedProcessFunction)`：基于 KeyedStream 调用，传入 KeyedProcessFunction
- `.process(ProcessWindowFunction)`：基于 WindowedStream 调用，传入 ProcessWindowFunction
- `ctx.timerService().currentProcessingTime()`：获取当前的处理时间
- `ctx.timerService().currentWatermark()`：获取当前的水位线（事件时间）
- `ctx.timerService().registerProcessingTimeTimer(long time)`：注册处理时间定时器
- `ctx.timerService().registerEventTimeTimer(long time)`：注册事件时间定时器
- `ctx.output(OutputTag, value)`：将数据输出到侧输出流

### 6.3 处理函数的方法

**ProcessFunction 和 KeyedProcessFunction：**

- `processElement(value, ctx, out)`：处理流中的每一个数据，必须实现
- `onTimer(timestamp, ctx, out)`：定义定时触发的操作，可选实现

**ProcessWindowFunction：**

- `process(key, context, elements, out)`：处理窗口中的所有数据，必须实现
- `clear(context)`：窗口清理工作，可选实现

### 6.4 学习建议

1. 先理解处理函数的概念和特点
2. 掌握 ProcessFunction 和 KeyedProcessFunction 的基本使用
3. 理解定时器和定时服务的工作原理
4. 学习如何使用状态管理
5. 了解 Top N 等经典应用案例的实现
6. 注意处理函数的性能考虑和适用场景

## 7. 参考资料

- [处理函数 | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/process_function/)
- [定时器 | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/process_function/#timers)


