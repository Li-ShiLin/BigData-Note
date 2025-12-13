<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 多流转换演示](#flink-%E5%A4%9A%E6%B5%81%E8%BD%AC%E6%8D%A2%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 分流操作](#4-%E5%88%86%E6%B5%81%E6%93%8D%E4%BD%9C)
    - [4.1 使用 Filter 方法分流](#41-%E4%BD%BF%E7%94%A8-filter-%E6%96%B9%E6%B3%95%E5%88%86%E6%B5%81)
    - [4.2 使用侧输出流分流](#42-%E4%BD%BF%E7%94%A8%E4%BE%A7%E8%BE%93%E5%87%BA%E6%B5%81%E5%88%86%E6%B5%81)
  - [5. 基本合流操作](#5-%E5%9F%BA%E6%9C%AC%E5%90%88%E6%B5%81%E6%93%8D%E4%BD%9C)
    - [5.1 Union 联合](#51-union-%E8%81%94%E5%90%88)
    - [5.2 Connect 连接](#52-connect-%E8%BF%9E%E6%8E%A5)
    - [5.3 CoProcessFunction 实时对账](#53-coprocessfunction-%E5%AE%9E%E6%97%B6%E5%AF%B9%E8%B4%A6)
  - [6. 基于时间的合流操作](#6-%E5%9F%BA%E4%BA%8E%E6%97%B6%E9%97%B4%E7%9A%84%E5%90%88%E6%B5%81%E6%93%8D%E4%BD%9C)
    - [6.1 窗口联结（Window Join）](#61-%E7%AA%97%E5%8F%A3%E8%81%94%E7%BB%93window-join)
    - [6.2 间隔联结（Interval Join）](#62-%E9%97%B4%E9%9A%94%E8%81%94%E7%BB%93interval-join)
    - [6.3 窗口同组联结（Window CoGroup）](#63-%E7%AA%97%E5%8F%A3%E5%90%8C%E7%BB%84%E8%81%94%E7%BB%93window-cogroup)
  - [7. 总结](#7-%E6%80%BB%E7%BB%93)
    - [7.1 分流操作对比](#71-%E5%88%86%E6%B5%81%E6%93%8D%E4%BD%9C%E5%AF%B9%E6%AF%94)
    - [7.2 合流操作对比](#72-%E5%90%88%E6%B5%81%E6%93%8D%E4%BD%9C%E5%AF%B9%E6%AF%94)
    - [7.3 选择建议](#73-%E9%80%89%E6%8B%A9%E5%BB%BA%E8%AE%AE)
  - [8. 参考资料](#8-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 多流转换演示

## 1. 项目作用

本项目演示了 Flink 中多流转换的相关知识点，包括：

- 分流操作：使用 filter 方法和侧输出流（side output）
- 基本合流操作：联合（Union）、连接（Connect）
- 协同处理函数：CoMapFunction、CoProcessFunction
- 基于时间的合流：窗口联结（Window Join）、间隔联结（Interval Join）、窗口同组联结（Window CoGroup）

通过实际代码示例帮助开发者快速掌握 Flink 多流转换的创建和使用方法。

## 2. 项目结构

```
flink-08-Multi-Stream-Transformations/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── multistream/
│       ├── SplitStreamByFilter.java                     # 使用 filter 方法分流示例
│       ├── SplitStreamByOutputTag.java                 # 使用侧输出流分流示例
│       ├── UnionExample.java                           # Union 联合示例
│       ├── CoMapExample.java                           # Connect 连接示例（CoMapFunction）
│       ├── BillCheckExample.java                       # CoProcessFunction 示例（实时对账）
│       ├── WindowJoinExample.java                     # 窗口联结示例
│       ├── IntervalJoinExample.java                    # 间隔联结示例
│       └── CoGroupExample.java                         # 窗口同组联结示例
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
 * 用于演示 Flink 多流转换相关功能
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
 * 用于演示多流转换相关功能
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

## 4. 分流操作

### 4.1 使用 Filter 方法分流

文件路径：`src/main/java/com/action/multistream/SplitStreamByFilter.java`

功能说明：演示使用 `filter()` 方法实现分流。这种方式简单但代码冗余，需要多次调用 filter 方法。实际应用中推荐使用侧输出流（side output）方式。

代码实现

```java
package com.action.multistream;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流示例：使用 filter 方法实现分流
 * 
 * 这种方式简单但代码冗余，需要多次调用 filter 方法
 * 实际应用中推荐使用侧输出流（side output）方式
 */
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
        
        // 筛选 Mary 的浏览行为放入 MaryStream 流中
        DataStream<Event> MaryStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        });
        
        // 筛选 Bob 的购买行为放入 BobStream 流中
        DataStream<Event> BobStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });
        
        // 筛选其他人的浏览行为放入 elseStream 流中
        DataStream<Event> elseStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return !value.user.equals("Mary") && !value.user.equals("Bob");
            }
        });

        MaryStream.print("Mary pv");
        BobStream.print("Bob pv");
        elseStream.print("else pv");

        env.execute();
    }
}
```

运行说明

直接运行 `SplitStreamByFilter` 类的 `main` 方法，观察不同用户的数据被分流到不同的流中。

注意事项

1. **代码冗余**：这种方式需要多次调用 filter 方法，代码冗余
2. **性能问题**：这种方式相当于将原始数据流复制多份，然后对每一份分别做筛选，效率较低
3. **推荐方式**：实际应用中推荐使用侧输出流（side output）方式

### 4.2 使用侧输出流分流

文件路径：`src/main/java/com/action/multistream/SplitStreamByOutputTag.java`

功能说明：演示使用侧输出流（side output）实现分流。这是推荐的分流方式，使用 ProcessFunction 的侧输出流功能，可以灵活地输出不同类型的数据到不同的流中。

核心概念

**侧输出流（Side Output）：**

- 侧输出流是 Flink 中实现分流的标准方式
- 使用 `OutputTag` 定义侧输出流的标记和类型
- 在 `ProcessFunction` 中使用 `ctx.output()` 方法输出到侧输出流
- 使用 `getSideOutput()` 方法获取侧输出流

代码实现

```java
package com.action.multistream;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流示例：使用侧输出流（side output）实现分流
 * 
 * 这是推荐的分流方式，使用 ProcessFunction 的侧输出流功能
 * 可以灵活地输出不同类型的数据到不同的流中
 */
public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组 (user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag =
            new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {};
    private static OutputTag<Tuple3<String, String, Long>> BobTag =
            new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 使用 ProcessFunction 处理数据，根据用户类型输出到不同的侧输出流
        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    // 将 Mary 的数据输出到侧输出流，转换为三元组
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    // 将 Bob 的数据输出到侧输出流，转换为三元组
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    // 其他用户的数据输出到主流，保持 Event 类型
                    out.collect(value);
                }
            }
        });

        // 获取侧输出流并打印
        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        // 打印主流数据
        processedStream.print("else");

        env.execute();
    }
}
```

运行说明

直接运行 `SplitStreamByOutputTag` 类的 `main` 方法，观察不同用户的数据被分流到不同的侧输出流中。

注意事项

1. **灵活性**：侧输出流可以输出不同类型的数据，比 filter 方式更灵活
2. **效率**：不需要复制流，只需要一次处理即可完成分流
3. **推荐方式**：这是 Flink 推荐的分流方式，在 Flink 1.13 版本后，`split()` 方法已被弃用

## 5. 基本合流操作

### 5.1 Union 联合

文件路径：`src/main/java/com/action/multistream/UnionExample.java`

功能说明：演示 Union 操作，联合多条流。Union 操作要求流中的数据类型必须相同，合并之后的新流会包括所有流中的元素。在事件时间语义下，合流之后的水位线以最小的那个为准（木桶原理）。

核心概念

**Union 操作：**

- Union 可以合并多条流，但数据类型必须相同
- 合并之后的新流会包括所有流中的元素
- 在事件时间语义下，合流之后的水位线以最小的那个为准（木桶原理）
- 多流合并时处理的时效性是以最慢的那个流为准的

代码实现

```java
package com.action.multistream;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Union 示例：联合多条流
 * 
 * Union 操作要求流中的数据类型必须相同，合并之后的新流会包括所有流中的元素
 * 在事件时间语义下，合流之后的水位线以最小的那个为准（木桶原理）
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流（从 socket 读取，这里用 fromElements 模拟）
        SingleOutputStreamOperator<Event> stream1 = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );
        stream1.print("stream1");

        // 创建第二条流
        SingleOutputStreamOperator<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 3000L),
                new Event("Cary", "./prod?id=1", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );
        stream2.print("stream2");

        // 合并两条流：union 可以合并多条流，但数据类型必须相同
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        // 输出当前水位线，观察合流后水位线的变化
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print("union");

        env.execute();
    }
}
```

运行说明

直接运行 `UnionExample` 类的 `main` 方法，观察合流后水位线的变化。

注意事项

1. **数据类型**：Union 操作要求流中的数据类型必须相同
2. **水位线**：合流之后的水位线以最小的那个为准（木桶原理）
3. **多条流**：Union 可以同时合并多条流，而 Connect 只能连接两条流

### 5.2 Connect 连接

文件路径：`src/main/java/com/action/multistream/CoMapExample.java`

功能说明：演示 Connect 操作，连接两条流。Connect 操作允许流的数据类型不同，得到的是 ConnectedStreams。需要使用 CoMapFunction、CoFlatMapFunction 或 CoProcessFunction 来处理。注意：Connect 只能连接两条流，而 Union 可以连接多条流。

核心概念

**Connect 操作：**

- Connect 允许流的数据类型不同，得到的是 ConnectedStreams
- 需要使用 CoMapFunction、CoFlatMapFunction 或 CoProcessFunction 来处理
- Connect 只能连接两条流，而 Union 可以连接多条流
- ConnectedStreams 也可以直接调用 `.keyBy()` 进行按键分区的操作

代码实现

```java
package com.action.multistream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Connect 示例：连接两条流
 * 
 * Connect 操作允许流的数据类型不同，得到的是 ConnectedStreams
 * 需要使用 CoMapFunction、CoFlatMapFunction 或 CoProcessFunction 来处理
 * 注意：Connect 只能连接两条流，而 Union 可以连接多条流
 */
public class CoMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建两条数据类型不同的流
        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        // 连接两条流，得到 ConnectedStreams
        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        
        // 使用 CoMapFunction 处理连接流
        // map1 处理第一条流的数据，map2 处理第二条流的数据
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) {
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }
}
```

运行说明

直接运行 `CoMapExample` 类的 `main` 方法，观察两条不同类型流的数据被连接处理。

注意事项

1. **数据类型**：Connect 操作允许流的数据类型不同
2. **处理函数**：需要使用 CoMapFunction、CoFlatMapFunction 或 CoProcessFunction 来处理
3. **流数量**：Connect 只能连接两条流，而 Union 可以连接多条流
4. **按键分区**：ConnectedStreams 也可以直接调用 `.keyBy()` 进行按键分区的操作

### 5.3 CoProcessFunction 实时对账

文件路径：`src/main/java/com/action/multistream/BillCheckExample.java`

功能说明：演示 CoProcessFunction 的使用，实现实时对账需求。实现 app 的支付操作和第三方的支付操作的双流 Join。App 的支付事件和第三方的支付事件将会互相等待 5 秒钟，如果等不来对应的支付事件，那么就输出报警信息。

核心概念

**CoProcessFunction：**

- CoProcessFunction 是处理函数家族中的一员，用法与 ProcessFunction 非常相似
- 需要实现 `processElement1()` 和 `processElement2()` 两个方法
- 可以通过上下文 ctx 来访问 timestamp、水位线，并通过 TimerService 注册定时器
- 提供了 `.onTimer()` 方法，用于定义定时触发的处理操作

代码实现

```java
package com.action.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * CoProcessFunction 示例：实时对账
 * 
 * 实现 app 的支付操作和第三方的支付操作的双流 Join
 * App 的支付事件和第三方的支付事件将会互相等待 5 秒钟
 * 如果等不来对应的支付事件，那么就输出报警信息
 */
public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自 app 的支付日志：(订单ID, 来源, 时间戳)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        // 来自第三方支付平台的支付日志：(订单ID, 来源, 状态, 时间戳)
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                })
        );

        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        // 按照订单ID进行 keyBy，然后连接两条流
        appStream.connect(thirdpartStream)
                .keyBy(data -> data.f0, data -> data.f0)  // 两条流都按照订单ID分组
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    // 自定义实现 CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态变量
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>(
                            "app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );

            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                            "thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 处理来自 app 的支付事件
            // 看另一条流中事件是否来过
            if (thirdPartyEventState.value() != null) {
                // 如果第三方支付信息已经到达，对账成功
                out.collect("对账成功：" + value + " " + thirdPartyEventState.value());
                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 如果第三方支付信息还没到，更新状态并注册定时器
                appEventState.update(value);
                // 注册一个 5 秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 处理来自第三方支付平台的事件
            if (appEventState.value() != null) {
                // 如果 app 支付信息已经到达，对账成功
                out.collect("对账成功：" + appEventState.value() + " " + value);
                // 清空状态
                appEventState.clear();
            } else {
                // 如果 app 支付信息还没到，更新状态并注册定时器
                thirdPartyEventState.update(value);
                // 注册一个 5 秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " 第三方支付平台信息未到");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + " app 信息未到");
            }
            // 清空状态
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
```

运行说明

直接运行 `BillCheckExample` 类的 `main` 方法，观察对账结果。

注意事项

1. **状态管理**：使用状态变量保存已经到达的事件
2. **定时器**：使用定时器实现超时检测
3. **按键分区**：两条流都需要按照相同的 key 进行分组
4. **应用场景**：适用于需要等待两条流中匹配数据的场景

## 6. 基于时间的合流操作

### 6.1 窗口联结（Window Join）

文件路径：`src/main/java/com/action/multistream/WindowJoinExample.java`

功能说明：演示窗口联结（Window Join）操作。窗口联结可以定义时间窗口，并将两条流中共享一个公共键（key）的数据放在窗口中进行配对处理。窗口联结是内连接（inner join），只有匹配成功的数据才会输出。窗口内会计算两条流数据的笛卡尔积。

核心概念

**窗口联结：**

- 窗口联结可以定义时间窗口，并将两条流中共享一个公共键（key）的数据放在窗口中进行配对处理
- 窗口联结是内连接（inner join），只有匹配成功的数据才会输出
- 窗口内会计算两条流数据的笛卡尔积
- 支持滚动窗口、滑动窗口和会话窗口

代码实现

```java
package com.action.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口联结（Window Join）示例
 * 
 * 窗口联结可以定义时间窗口，并将两条流中共享一个公共键（key）的数据放在窗口中进行配对处理
 * 窗口联结是内连接（inner join），只有匹配成功的数据才会输出
 * 窗口内会计算两条流数据的笛卡尔积
 */
public class WindowJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        // 创建第二条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        // 窗口联结：按照 key 分组，在 5 秒的滚动窗口内进行联结
        stream1.join(stream2)
                .where(r -> r.f0)  // 第一条流的 key
                .equalTo(r -> r.f0)  // 第二条流的 key
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 5秒滚动窗口
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> left, Tuple2<String, Long> right) throws Exception {
                        // 对匹配的数据对进行处理
                        return left + "=>" + right;
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

直接运行 `WindowJoinExample` 类的 `main` 方法，观察窗口内数据的笛卡尔积匹配结果。

注意事项

1. **内连接**：窗口联结是内连接（inner join），只有匹配成功的数据才会输出
2. **笛卡尔积**：窗口内会计算两条流数据的笛卡尔积
3. **窗口类型**：支持滚动窗口、滑动窗口和会话窗口
4. **时间语义**：需要基于事件时间语义

### 6.2 间隔联结（Interval Join）

文件路径：`src/main/java/com/action/multistream/IntervalJoinExample.java`

功能说明：演示间隔联结（Interval Join）操作。间隔联结针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔，看这期间是否有来自另一条流的数据匹配。匹配条件：`a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound`。

核心概念

**间隔联结：**

- 间隔联结针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔
- 匹配条件：`a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound`
- 间隔联结目前只支持事件时间语义
- lowerBound 应该小于等于 upperBound，两者都可正可负

代码实现

```java
package com.action.multistream;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 间隔联结（Interval Join）示例
 * 
 * 间隔联结针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔
 * 看这期间是否有来自另一条流的数据匹配
 * 
 * 匹配条件：a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound
 * 
 * 本例：一条是下订单的流，一条是浏览数据的流
 * 针对同一个用户，使用一个用户的下订单事件和这个用户最近十分钟的浏览数据做联结查询
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 订单流：(用户名, 订单ID, 时间戳)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        // 浏览流：用户浏览事件
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 间隔联结：按照用户分组，订单时间前后 5 秒到 10 秒的浏览数据
        orderStream.keyBy(data -> data.f0)  // 按照用户名分组
                .intervalJoin(clickStream.keyBy(data -> data.user))  // 浏览流也按照用户名分组
                .between(Time.seconds(-5), Time.seconds(10))  // 下界 -5秒，上界 10秒
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        // 处理匹配的数据对：订单和对应的浏览记录
                        out.collect(right + " => " + left);
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

直接运行 `IntervalJoinExample` 类的 `main` 方法，观察订单和浏览数据的匹配结果。

注意事项

1. **时间间隔**：lowerBound 应该小于等于 upperBound，两者都可正可负
2. **事件时间**：间隔联结目前只支持事件时间语义
3. **按键分区**：做间隔联结的两条流 A 和 B，也必须基于相同的 key
4. **应用场景**：适用于需要基于时间间隔进行匹配的场景

### 6.3 窗口同组联结（Window CoGroup）

文件路径：`src/main/java/com/action/multistream/CoGroupExample.java`

功能说明：演示窗口同组联结（Window CoGroup）操作。CoGroup 操作比窗口的 join 更加通用，不仅可以实现类似 SQL 中的"内连接"（inner join），也可以实现左外连接（left outer join）、右外连接（right outer join）和全外连接（full outer join）。

核心概念

**窗口同组联结：**

- CoGroup 操作比窗口的 join 更加通用
- 不仅可以实现类似 SQL 中的"内连接"（inner join）
- 也可以实现左外连接（left outer join）、右外连接（right outer join）和全外连接（full outer join）
- 与 window join 的区别：window join 计算笛卡尔积，每对匹配数据调用一次 join 方法；coGroup 将窗口内所有数据一次性传入，可以自定义配对逻辑

代码实现

```java
package com.action.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 窗口同组联结（Window CoGroup）示例
 * 
 * CoGroup 操作比窗口的 join 更加通用
 * 不仅可以实现类似 SQL 中的"内连接"（inner join）
 * 也可以实现左外连接（left outer join）、右外连接（right outer join）和全外连接（full outer join）
 * 
 * 与 window join 的区别：
 * - window join 计算笛卡尔积，每对匹配数据调用一次 join 方法
 * - coGroup 将窗口内所有数据一次性传入，可以自定义配对逻辑
 */
public class CoGroupExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }
                        )
        );

        // 创建第二条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }
                        )
        );

        // 窗口同组联结：按照 key 分组，在 5 秒的滚动窗口内进行同组联结
        stream1.coGroup(stream2)
                .where(r -> r.f0)  // 第一条流的 key
                .equalTo(r -> r.f0)  // 第二条流的 key
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 5秒滚动窗口
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                        // 将窗口内两条流的所有数据一次性传入，可以自定义配对逻辑
                        collector.collect(iter1 + "=>" + iter2);
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

直接运行 `CoGroupExample` 类的 `main` 方法，观察窗口内数据的同组联结结果。

注意事项

1. **灵活性**：CoGroup 比 window join 更加灵活，可以自定义配对逻辑
2. **外连接**：可以实现左外连接、右外连接和全外连接
3. **数据传入**：将窗口内所有数据一次性传入，而不是计算笛卡尔积
4. **应用场景**：适用于需要自定义配对逻辑的场景

## 7. 总结

### 7.1 分流操作对比

| 方式 | 优点 | 缺点 | 推荐 |
|------|------|------|------|
| Filter 方法 | 简单直观 | 代码冗余，效率低 | 不推荐 |
| 侧输出流 | 灵活高效，支持不同类型 | 需要 ProcessFunction | 推荐 |

### 7.2 合流操作对比

| 操作 | 数据类型 | 流数量 | 特点 |
|------|----------|--------|------|
| Union | 必须相同 | 多条 | 简单粗暴，直接合并 |
| Connect | 可以不同 | 两条 | 灵活，支持不同类型 |
| Window Join | 可以不同 | 两条 | 基于时间窗口，内连接 |
| Interval Join | 可以不同 | 两条 | 基于时间间隔，内连接 |
| CoGroup | 可以不同 | 两条 | 基于时间窗口，支持外连接 |

### 7.3 选择建议

1. **分流**：推荐使用侧输出流（side output）方式
2. **简单合流**：数据类型相同用 Union，不同用 Connect
3. **基于时间的合流**：
   - 固定时间窗口用 Window Join 或 CoGroup
   - 时间间隔不固定用 Interval Join
   - 需要外连接用 CoGroup

## 8. 参考资料

- Flink 官方文档 - 多流转换：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/
- Flink 官方文档 - 窗口操作：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/windows/

