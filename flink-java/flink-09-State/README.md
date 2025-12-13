<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 状态编程演示](#flink-%E7%8A%B6%E6%80%81%E7%BC%96%E7%A8%8B%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 按键分区状态（Keyed State）](#4-%E6%8C%89%E9%94%AE%E5%88%86%E5%8C%BA%E7%8A%B6%E6%80%81keyed-state)
    - [4.1 基本概念](#41-%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)
    - [4.2 值状态（ValueState）](#42-%E5%80%BC%E7%8A%B6%E6%80%81valuestate)
    - [4.3 列表状态（ListState）](#43-%E5%88%97%E8%A1%A8%E7%8A%B6%E6%80%81liststate)
    - [4.4 映射状态（MapState）](#44-%E6%98%A0%E5%B0%84%E7%8A%B6%E6%80%81mapstate)
    - [4.5 聚合状态（AggregatingState）](#45-%E8%81%9A%E5%90%88%E7%8A%B6%E6%80%81aggregatingstate)
    - [4.6 状态生存时间（TTL）](#46-%E7%8A%B6%E6%80%81%E7%94%9F%E5%AD%98%E6%97%B6%E9%97%B4ttl)
  - [5. 算子状态（Operator State）](#5-%E7%AE%97%E5%AD%90%E7%8A%B6%E6%80%81operator-state)
    - [5.1 基本概念](#51-%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)
    - [5.2 列表状态（ListState）](#52-%E5%88%97%E8%A1%A8%E7%8A%B6%E6%80%81liststate)
  - [6. 广播状态（Broadcast State）](#6-%E5%B9%BF%E6%92%AD%E7%8A%B6%E6%80%81broadcast-state)
    - [6.1 基本概念](#61-%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)
    - [6.2 广播状态示例](#62-%E5%B9%BF%E6%92%AD%E7%8A%B6%E6%80%81%E7%A4%BA%E4%BE%8B)
  - [7. 状态总结](#7-%E7%8A%B6%E6%80%81%E6%80%BB%E7%BB%93)
    - [7.1 状态分类对比](#71-%E7%8A%B6%E6%80%81%E5%88%86%E7%B1%BB%E5%AF%B9%E6%AF%94)
    - [7.2 状态使用建议](#72-%E7%8A%B6%E6%80%81%E4%BD%BF%E7%94%A8%E5%BB%BA%E8%AE%AE)
    - [7.3 状态后端配置](#73-%E7%8A%B6%E6%80%81%E5%90%8E%E7%AB%AF%E9%85%8D%E7%BD%AE)
  - [8. 参考资料](#8-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 状态编程演示

## 1. 项目作用

本项目演示了 Flink 中状态编程的相关知识点，包括：

- 状态的基本概念和分类
- 按键分区状态（Keyed State）：值状态、列表状态、映射状态、聚合状态
- 状态生存时间（TTL）
- 算子状态（Operator State）
- 广播状态（Broadcast State）

通过实际代码示例帮助开发者快速掌握 Flink 状态的创建和使用方法。

## 2. 项目结构

```
flink-09-State/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── state/
│       ├── PeriodicPvExample.java                        # 值状态（ValueState）示例
│       ├── TwoStreamFullJoinExample.java                # 列表状态（ListState）示例
│       ├── FakeWindowExample.java                       # 映射状态（MapState）示例
│       ├── AverageTimestampExample.java                 # 聚合状态（AggregatingState）示例
│       ├── StateTTLExample.java                         # 状态 TTL 示例
│       ├── BufferingSinkExample.java                    # 算子状态（Operator State）示例
│       └── BroadcastStateExample.java                   # 广播状态（Broadcast State）示例
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
 * 用于演示 Flink 状态编程相关功能
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
 * 用于演示状态编程相关功能
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

## 4. 按键分区状态（Keyed State）

### 4.1 基本概念

按键分区状态（Keyed State）是任务按照键（key）来访问和维护的状态。它的特点是以 key 为作用范围进行隔离。

**特点：**

- 状态是根据输入流中定义的键（key）来维护和访问的
- 只能定义在按键分区流（KeyedStream）中，也就是 keyBy 之后才可以使用
- 对于每个 key 都会保存一份状态实例
- 相同 key 的数据会访问相同的状态，不同 key 的状态之间是彼此隔离的

**支持的结构类型：**

1. **值状态（ValueState）**：状态中只保存一个"值"
2. **列表状态（ListState）**：将需要保存的数据，以列表的形式组织起来
3. **映射状态（MapState）**：把一些键值对（key-value）作为状态整体保存起来
4. **归约状态（ReducingState）**：需要对添加进来的所有数据进行归约，将归约聚合之后的值作为状态保存下来
5. **聚合状态（AggregatingState）**：与归约状态类似，但聚合逻辑由 AggregateFunction 定义，状态类型可以跟输入数据类型不同

### 4.2 值状态（ValueState）

文件路径：`src/main/java/com/action/state/PeriodicPvExample.java`

功能说明：演示值状态（ValueState）的使用。统计每个用户的 pv 数据，隔一段时间（10s）输出一次结果。使用值状态保存当前 pv 值和定时器时间戳。

代码实现

```java
package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 值状态（ValueState）示例
 * 
 * 统计每个用户的 pv 数据，隔一段时间（10s）输出一次结果
 * 使用值状态保存当前 pv 值和定时器时间戳
 */
public class PeriodicPvExample {
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
                        })
                );

        stream.print("input");

        // 统计每个用户的 pv，隔一段时间（10s）输出一次结果
        stream.keyBy(data -> data.user)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    // 注册定时器，周期性输出 pv
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        // 定义两个状态，保存当前 pv 值，以及定时器时间戳
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在 open 生命周期方法中获取状态
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新 count 值
            Long count = countState.value();
            if (count == null) {
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }
            
            // 注册定时器：如果还没有注册过定时器，就注册一个 10 秒后的定时器
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出统计结果
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
            // 清空定时器时间戳状态，以便下次重新注册定时器
            timerTsState.clear();
        }
    }
}
```

运行说明

直接运行 `PeriodicPvExample` 类的 `main` 方法，观察每个用户的 pv 统计结果。

注意事项

1. **状态获取**：状态必须在 `open()` 生命周期方法中获取，不能在 `processElement()` 中获取
2. **状态作用域**：值状态对于每个 key 都会保存一份状态实例
3. **状态清除**：可以使用 `clear()` 方法清除当前 key 对应的状态

### 4.3 列表状态（ListState）

文件路径：`src/main/java/com/action/state/TwoStreamFullJoinExample.java`

功能说明：演示列表状态（ListState）的使用。在 Flink SQL 中，支持两条流的全量 Join，这里使用列表状态变量来实现这个 SQL 语句的功能。将两条流的所有数据都保存下来，然后进行 Join。

核心概念

**列表状态（ListState）：**

- 将需要保存的数据，以列表（List）的形式组织起来
- 提供 `get()`、`update()`、`add()`、`addAll()` 等方法操作状态
- 适用于需要保存多个元素的场景

代码实现

```java
package com.action.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 列表状态（ListState）示例
 * 
 * 在 Flink SQL 中，支持两条流的全量 Join，语法如下：
 * SELECT * FROM A INNER JOIN B WHERE A.id = B.id
 * 
 * 这里使用列表状态变量来实现这个 SQL 语句的功能
 * 将两条流的所有数据都保存下来，然后进行 Join
 */
public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流：(key, stream-name, timestamp)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
        );

        // 创建第二条流：(key, stream-name, timestamp)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
        );

        // 定义列表状态，保存流1和流2中的所有数据
        stream1.connect(stream2)
                .keyBy(data -> data.f0, data -> data.f0)  // 按照 key 分组
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    // 保存流1的所有数据
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    // 保存流2的所有数据
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化列表状态
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {
                        // 获取流1的数据，添加到列表状态中
                        stream1ListState.add(left);
                        // 遍历流2的所有数据，进行 join
                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            out.collect(left + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        // 获取流2的数据，添加到列表状态中
                        stream2ListState.add(right);
                        // 遍历流1的所有数据，进行 join
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            out.collect(left + " => " + right);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
```

运行说明

直接运行 `TwoStreamFullJoinExample` 类的 `main` 方法，观察两条流的全量 Join 结果。

注意事项

1. **全量 Join**：这种方式会将所有数据都保存下来，慎用，因为状态会不断增长
2. **内存占用**：列表状态会保存所有历史数据，需要注意内存占用
3. **应用场景**：适用于需要保存多个元素的场景，如全量 Join、历史数据查询等

### 4.4 映射状态（MapState）

文件路径：`src/main/java/com/action/state/FakeWindowExample.java`

功能说明：演示映射状态（MapState）的使用。使用 KeyedProcessFunction 模拟滚动窗口，计算每一个 url 在每一个窗口中的 pv 数据。映射状态的用法和 Java 中的 HashMap 很相似。

核心概念

**映射状态（MapState）：**

- 把一些键值对（key-value）作为状态整体保存起来
- 可以认为就是一组 key-value 映射的列表
- 提供 `get()`、`put()`、`putAll()`、`remove()`、`contains()` 等方法操作状态
- 适用于需要按 key 存储多个值的场景

代码实现

```java
package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 映射状态（MapState）示例
 * 
 * 使用 KeyedProcessFunction 模拟滚动窗口
 * 计算每一个 url 在每一个窗口中的 pv 数据
 * 
 * 映射状态的用法和 Java 中的 HashMap 很相似
 * 这里用 MapState 来完整模拟窗口的功能
 */
public class FakeWindowExample {
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
                        })
                );

        // 统计每10s窗口内，每个 url 的 pv
        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        // 定义属性，窗口长度
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 声明状态，用 map 保存 pv 值（窗口start，count）
        MapState<Long, Long> windowPvMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化映射状态：key 是窗口开始时间，value 是 pv 计数
            windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，就根据时间戳判断属于哪个窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 注册 end - 1 的定时器，窗口触发计算
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态中的 pv 值
            if (windowPvMapState.contains(windowStart)) {
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1);
            } else {
                windowPvMapState.put(windowStart, 1L);
            }
        }

        // 定时器触发，直接输出统计的 pv 结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long pv = windowPvMapState.get(windowStart);
            
            out.collect("url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd));

            // 模拟窗口的销毁，清除 map 中的 key
            windowPvMapState.remove(windowStart);
        }
    }
}
```

运行说明

直接运行 `FakeWindowExample` 类的 `main` 方法，观察每个 url 在每个窗口中的 pv 统计结果。

注意事项

1. **窗口模拟**：使用 MapState 可以完整模拟窗口的功能
2. **状态清理**：窗口关闭后需要及时清理状态，避免内存泄漏
3. **应用场景**：适用于需要按 key 存储多个值的场景，如窗口聚合、多维度统计等

### 4.5 聚合状态（AggregatingState）

文件路径：`src/main/java/com/action/state/AverageTimestampExample.java`

功能说明：演示聚合状态（AggregatingState）的使用。对用户点击事件流每 5 个数据统计一次平均时间戳。这是一个类似计数窗口（CountWindow）求平均值的计算，使用有聚合状态的 RichFlatMapFunction 来实现。

核心概念

**聚合状态（AggregatingState）：**

- 与归约状态非常类似，也是一个值，用来保存添加进来的所有数据的聚合结果
- 与 ReducingState 不同的是，它的聚合逻辑是由 AggregateFunction 来定义的
- 通过一个累加器（Accumulator）来表示状态，所以聚合的状态类型可以跟添加进来的数据类型完全不同，使用更加灵活

代码实现

```java
package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 聚合状态（AggregatingState）示例
 * 
 * 对用户点击事件流每 5 个数据统计一次平均时间戳
 * 这是一个类似计数窗口（CountWindow）求平均值的计算
 * 使用有聚合状态的 RichFlatMapFunction 来实现
 */
public class AverageTimestampExample {
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
                        })
                );

        // 统计每个用户的点击频次，到达 5 次就输出统计结果
        stream.keyBy(data -> data.user)
                .flatMap(new AvgTsResult())
                .print();

        env.execute();
    }

    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        // 定义聚合状态，用来计算平均时间戳
        AggregatingState<Event, Long> avgTsAggState;

        // 定义一个值状态，用来保存当前用户访问频次
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化聚合状态：累加器类型为 (总和, 个数)，输出类型为 Long（平均值）
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            // 累加器：f0 是时间戳总和，f1 是计数
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            // 添加元素：累加时间戳和计数
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            // 获取结果：计算平均值
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            // 合并累加器（会话窗口需要）
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 更新访问频次
            Long count = countState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }

            countState.update(count);
            // 将当前事件添加到聚合状态中
            avgTsAggState.add(value);

            // 达到 5 次就输出结果，并清空状态
            if (count == 5) {
                out.collect(value.user + " 平均时间戳： " + new Timestamp(avgTsAggState.get()));
                countState.clear();
                // 注意：聚合状态没有 clear 方法，需要重新创建状态描述器
            }
        }
    }
}
```

运行说明

直接运行 `AverageTimestampExample` 类的 `main` 方法，观察每个用户每 5 次点击的平均时间戳。

注意事项

1. **累加器类型**：聚合状态的累加器类型可以跟输入数据类型不同，使用更加灵活
2. **状态清除**：聚合状态没有 `clear()` 方法，需要重新创建状态描述器
3. **应用场景**：适用于需要复杂聚合逻辑的场景，如平均值、方差等统计计算

### 4.6 状态生存时间（TTL）

文件路径：`src/main/java/com/action/state/StateTTLExample.java`

功能说明：演示状态生存时间（TTL）的配置。在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存储空间的耗尽。可以配置状态的"生存时间"（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除。

核心概念

**状态 TTL：**

- 状态的失效其实不需要立即删除，可以给状态附加一个属性，也就是状态的"失效时间"
- 状态创建的时候，设置失效时间 = 当前时间 + TTL
- 之后如果有对状态的访问和修改，可以再对失效时间进行更新
- 当设置的清除条件被触发时，就可以判断状态是否失效、从而进行清除

**TTL 配置项：**

- `.newBuilder(Time)`：状态 TTL 配置的构造器方法，传入状态生存时间
- `.setUpdateType()`：设置更新类型
  - `OnCreateAndWrite`：只有创建状态和更改状态（写操作）时更新失效时间
  - `OnReadAndWrite`：无论读写操作都会更新失效时间
- `.setStateVisibility()`：设置状态的可见性
  - `NeverReturnExpired`：从不返回过期值（默认）
  - `ReturnExpireDefNotCleanedUp`：如果过期状态还存在，就返回它的值

代码实现

```java
package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 状态生存时间（TTL）示例
 * 
 * 在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存储空间的耗尽
 * 可以配置状态的"生存时间"（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除
 * 
 * 注意：目前的 TTL 设置只支持处理时间
 */
public class StateTTLExample {
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
                        })
                );

        stream.keyBy(data -> data.user)
                .process(new StateTTLProcessFunction())
                .print();

        env.execute();
    }

    public static class StateTTLProcessFunction extends KeyedProcessFunction<String, Event, String> {
        private ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建状态 TTL 配置
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))  // 状态生存时间为 10 秒
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 只有创建状态和更改状态时更新失效时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 从不返回过期值
                    .build();

            // 创建状态描述器并启用 TTL
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("count", Long.class);
            stateDescriptor.enableTimeToLive(ttlConfig);

            countState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 访问状态，如果状态已过期会自动清除
            Long count = countState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            countState.update(count);
            out.collect(ctx.getCurrentKey() + " 访问次数: " + count);
        }
    }
}
```

运行说明

直接运行 `StateTTLExample` 类的 `main` 方法，观察状态在 10 秒后自动清除的效果。

注意事项

1. **处理时间**：目前的 TTL 设置只支持处理时间
2. **集合类型状态**：所有集合类型的状态（例如 ListState、MapState）在设置 TTL 时，都是针对每一项（per-entry）元素的
3. **状态可见性**：`NeverReturnExpired` 是默认行为，表示从不返回过期值

## 5. 算子状态（Operator State）

### 5.1 基本概念

算子状态（Operator State）就是一个算子并行实例上定义的状态，作用范围被限定为当前算子任务。算子状态跟数据的 key 无关，所以不同 key 的数据只要被分发到同一个并行子任务，就会访问到同一个 Operator State。

**特点：**

- 状态作用范围限定为当前的算子任务实例，只对当前并行子任务有效
- 状态跟数据的 key 无关，不同 key 的数据会访问到同一个 Operator State
- 需要实现 `CheckpointedFunction` 接口来定义状态的快照保存和恢复逻辑

**状态类型：**

1. **列表状态（ListState）**：将状态表示为一组数据的列表
2. **联合列表状态（UnionListState）**：与 ListState 类似，但并行度调整时对于状态的分配方式不同
3. **广播状态（BroadcastState）**：所有并行子任务都保持同一份"全局"状态

### 5.2 列表状态（ListState）

文件路径：`src/main/java/com/action/state/BufferingSinkExample.java`

功能说明：演示算子状态中的列表状态（ListState）的使用。自定义的 SinkFunction 会在 CheckpointedFunction 中进行数据缓存，然后统一发送到下游。这个例子演示了列表状态的平均分割重组（even-split redistribution）。

核心概念

**CheckpointedFunction 接口：**

- `snapshotState()`：保存状态快照到检查点时，调用这个方法
- `initializeState()`：初始化状态时调用这个方法，也会在恢复状态时调用
- `context.isRestored()`：判断是否是从故障中恢复

代码实现

```java
package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态（Operator State）示例
 * 
 * 自定义的 SinkFunction 会在 CheckpointedFunction 中进行数据缓存，然后统一发送到下游
 * 这个例子演示了列表状态的平均分割重组（even-split redistribution）
 * 
 * 算子状态作用范围限定为当前的算子任务实例，只对当前并行子任务有效
 * 需要实现 CheckpointedFunction 接口来定义状态的快照保存和恢复逻辑
 */
public class BufferingSinkExample {
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
                        })
                );

        stream.print("input");

        // 批量缓存输出：每 10 条数据输出一次
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;  // 批量输出的阈值
        private transient ListState<Event> checkpointedState;  // 算子状态
        private List<Event> bufferedElements;  // 本地缓存列表

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            // 将数据添加到本地缓存
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                // 达到阈值，批量输出
                for (Event element : bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 保存状态快照到检查点时，调用这个方法
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 初始化状态时调用这个方法，也会在恢复状态时调用
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    Types.POJO(Event.class));
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);
            
            // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
```

运行说明

直接运行 `BufferingSinkExample` 类的 `main` 方法，观察数据批量输出的效果。

注意事项

1. **状态重组**：算子并行度发生变化时，列表状态会进行平均分割重组（even-split redistribution）
2. **故障恢复**：使用 `context.isRestored()` 判断是否是从故障中恢复
3. **应用场景**：适用于 Source 或 Sink 等与外部系统连接的算子，或者完全没有 key 定义的场景

## 6. 广播状态（Broadcast State）

### 6.1 基本概念

广播状态（Broadcast State）是一种特殊的算子状态，可以让所有并行子任务都持有同一份"全局"状态，用来做统一的配置和规则设定。

**特点：**

- 所有并行子任务都保持同一份"全局"状态
- 状态就像被"广播"到所有分区一样
- 在底层，广播状态是以类似映射结构（map）的键值对（key-value）来保存的
- 必须基于一个"广播流"（BroadcastStream）来创建

**应用场景：**

- 动态配置或动态规则
- 需要全局统一的配置和规则设定
- 配置或规则数据是全局有效的，需要广播给所有的并行子任务

### 6.2 广播状态示例

文件路径：`src/main/java/com/action/state/BroadcastStateExample.java`

功能说明：演示广播状态（Broadcast State）的使用。在电商应用中，往往需要判断用户先后发生的行为的"组合模式"，比如"登录-下单"或者"登录-支付"，检测出这些连续的行为进行统计。

核心概念

**广播状态的使用：**

1. 定义广播状态的描述器（MapStateDescriptor）
2. 调用 `.broadcast()` 方法创建广播流
3. 将数据流与广播流连接（connect）
4. 使用 `BroadcastProcessFunction` 或 `KeyedBroadcastProcessFunction` 处理

代码实现

```java
package com.action.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 广播状态（Broadcast State）示例
 * 
 * 在电商应用中，往往需要判断用户先后发生的行为的"组合模式"
 * 比如"登录-下单"或者"登录-支付"，检测出这些连续的行为进行统计
 * 
 * 广播状态可以让所有并行子任务都持有同一份状态
 * 适用于"动态配置"或者"动态规则"的场景
 */
public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>(
                "patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        // 将事件流和广播流连接起来，进行处理
        DataStream<Tuple2<String, Pattern>> matches = actionStream
                .keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternEvaluator());

        matches.print();

        env.execute();
    }

    public static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration conf) {
            prevActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING));
        }

        @Override
        public void processBroadcastElement(
                Pattern pattern,
                Context ctx,
                Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 处理广播流中的模式数据，更新广播状态
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));

            // 将广播状态更新为当前的 pattern
            bcState.put(null, pattern);
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx,
                                   Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 处理正常数据流中的行为数据
            Pattern pattern = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态：保存当前行为作为下一次的前一次行为
            prevActionState.update(action.action);
        }
    }

    // 定义用户行为事件 POJO 类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式 POJO 类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
```

运行说明

直接运行 `BroadcastStateExample` 类的 `main` 方法，观察行为模式匹配的结果。

注意事项

1. **只读访问**：在 `processElement()` 方法中，广播状态是只读的，不能修改
2. **可写访问**：在 `processBroadcastElement()` 方法中，可以更新广播状态
3. **应用场景**：适用于需要动态配置或动态规则的场景

## 7. 状态总结

### 7.1 状态分类对比

| 状态类型 | 作用范围 | 是否需要 keyBy | 状态结构 | 应用场景 |
|---------|---------|---------------|---------|---------|
| Keyed State | 按键分区 | 是 | ValueState、ListState、MapState、ReducingState、AggregatingState | 聚合、窗口计算 |
| Operator State | 算子任务 | 否 | ListState、UnionListState、BroadcastState | Source、Sink 算子 |
| Broadcast State | 全局 | 否（但可以 keyBy） | MapState | 动态配置、动态规则 |

### 7.2 状态使用建议

1. **优先使用 Keyed State**：大多数场景下，Keyed State 已经足够使用
2. **状态 TTL**：对于会不断增长的状态，建议配置 TTL 避免内存耗尽
3. **状态清理**：及时清理不再使用的状态，避免内存泄漏
4. **状态后端选择**：
   - 小状态、高性能：使用 HashMapStateBackend
   - 大状态、可扩展：使用 EmbeddedRocksDBStateBackend

### 7.3 状态后端配置

**HashMapStateBackend：**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new HashMapStateBackend());
```

**EmbeddedRocksDBStateBackend：**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

注意：使用 EmbeddedRocksDBStateBackend 需要添加依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_${scala.binary.version}</artifactId>
</dependency>
```

## 8. 参考资料

- Flink 官方文档 - 状态编程：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state/
- Flink 官方文档 - 状态后端：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/state/state_backends/

