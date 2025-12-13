<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 水位线（Watermark）演示](#flink-%E6%B0%B4%E4%BD%8D%E7%BA%BFwatermark%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 水位线生成示例](#4-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%94%9F%E6%88%90%E7%A4%BA%E4%BE%8B)
    - [4.1 Flink 内置水位线生成器](#41-flink-%E5%86%85%E7%BD%AE%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%94%9F%E6%88%90%E5%99%A8)
    - [4.2 自定义周期性水位线生成器](#42-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%91%A8%E6%9C%9F%E6%80%A7%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%94%9F%E6%88%90%E5%99%A8)
    - [4.3 自定义断点式水位线生成器](#43-%E8%87%AA%E5%AE%9A%E4%B9%89%E6%96%AD%E7%82%B9%E5%BC%8F%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%94%9F%E6%88%90%E5%99%A8)
    - [4.4 在数据源中发送水位线](#44-%E5%9C%A8%E6%95%B0%E6%8D%AE%E6%BA%90%E4%B8%AD%E5%8F%91%E9%80%81%E6%B0%B4%E4%BD%8D%E7%BA%BF)
  - [5. 水位线特性总结](#5-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%89%B9%E6%80%A7%E6%80%BB%E7%BB%93)
    - [5.1 水位线的特性](#51-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%9A%84%E7%89%B9%E6%80%A7)
    - [5.2 水位线的传递](#52-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%9A%84%E4%BC%A0%E9%80%92)
    - [5.3 水位线的默认计算公式](#53-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%9A%84%E9%BB%98%E8%AE%A4%E8%AE%A1%E7%AE%97%E5%85%AC%E5%BC%8F)
    - [5.4 水位线的初始化和结束](#54-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%9A%84%E5%88%9D%E5%A7%8B%E5%8C%96%E5%92%8C%E7%BB%93%E6%9D%9F)
    - [5.5 水位线阻塞问题](#55-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E9%98%BB%E5%A1%9E%E9%97%AE%E9%A2%98)
  - [6. 常见问题](#6-%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
    - [6.1 时间语义选择](#61-%E6%97%B6%E9%97%B4%E8%AF%AD%E4%B9%89%E9%80%89%E6%8B%A9)
    - [6.2 水位线延迟设置](#62-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E5%BB%B6%E8%BF%9F%E8%AE%BE%E7%BD%AE)
    - [6.3 水位线生成方式选择](#63-%E6%B0%B4%E4%BD%8D%E7%BA%BF%E7%94%9F%E6%88%90%E6%96%B9%E5%BC%8F%E9%80%89%E6%8B%A9)
    - [6.4 在数据源中发送水位线](#64-%E5%9C%A8%E6%95%B0%E6%8D%AE%E6%BA%90%E4%B8%AD%E5%8F%91%E9%80%81%E6%B0%B4%E4%BD%8D%E7%BA%BF)
  - [7. 快速参考](#7-%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [7.1 代码位置](#71-%E4%BB%A3%E7%A0%81%E4%BD%8D%E7%BD%AE)
    - [7.2 关键 API](#72-%E5%85%B3%E9%94%AE-api)
    - [7.3 学习建议](#73-%E5%AD%A6%E4%B9%A0%E5%BB%BA%E8%AE%AE)
  - [8. 参考资料](#8-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 水位线（Watermark）演示

## 1. 项目作用

本项目演示了 Flink 中水位线（Watermark）的相关知识点，包括：

- 时间语义（处理时间、事件时间、摄入时间）
- 水位线的概念和特性
- 有序流和乱序流中的水位线生成
- Flink 内置水位线生成器的使用
- 自定义水位线生成策略（周期性、断点式）
- 在数据源中发送水位线

通过实际代码示例帮助开发者快速掌握 Flink 水位线的创建和使用方法。

## 2. 项目结构

```
flink-05-watermark/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源（有序流）
│   ├── ClickSourceWithWatermark.java                    # 自定义数据源（带水位线）
│   └── watermark/
│       ├── WatermarkTest.java                           # 内置水位线生成器示例
│       ├── CustomWatermarkTest.java                     # 自定义周期性水位线生成器示例
│       ├── CustomPunctuatedWatermarkTest.java          # 自定义断点式水位线生成器示例
│       └── EmitWatermarkInSourceFunction.java          # 在数据源中发送水位线示例
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
 * 用于演示 Flink 水位线相关功能
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
 * 用于演示有序流的水位线生成
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

## 4. 水位线生成示例

### 4.1 Flink 内置水位线生成器

文件路径：`src/main/java/com/action/watermark/WatermarkTest.java`

功能说明：演示 Flink 内置水位线生成器的使用，包括：

- **有序流**：使用 `forMonotonousTimestamps()` 生成水位线
- **乱序流**：使用 `forBoundedOutOfOrderness()` 生成水位线

核心概念：

**时间语义：**

1. **处理时间（Processing Time）**：执行处理操作的机器的系统时间
2. **事件时间（Event Time）**：每个事件在对应的设备上发生的时间，也就是数据生成的时间
3. **摄入时间（Ingestion Time）**：数据进入 Flink 数据流的时间，也就是 Source 算子读入数据的时间

**水位线（Watermark）：**

- 水位线是插入到数据流中的一个标记，用来表示当前事件时间的进展
- 水位线基于数据的时间戳生成
- 水位线的时间戳必须单调递增，以确保任务的事件时间时钟一直向前推进
- 水位线可以通过设置延迟，来保证正确处理乱序数据

**水位线默认计算公式：**

```
水位线 = 观察到的最大事件时间 - 最大延迟时间 - 1毫秒
```

代码实现

```java
package com.action.watermark;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 水位线生成示例
 * 演示 Flink 内置水位线生成器的使用
 * 
 * 包含两种场景：
 * 1. 有序流：使用 forMonotonousTimestamps()
 * 2. 乱序流：使用 forBoundedOutOfOrderness()
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 示例 1：有序流的水位线生成 ====================
        // 对于有序流，主要特点就是时间戳单调增长，所以永远不会出现迟到数据的问题
        // 这是周期性生成水位线的最简单的场景，直接拿当前最大的时间戳作为水位线就可以了
        
        env
                .addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 有序流水位线生成器
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .print("有序流");

        // ==================== 示例 2：乱序流的水位线生成 ====================
        // 由于乱序流中需要等待迟到数据到齐，所以必须设置一个固定量的延迟时间
        // 这时生成水位线的时间戳，就是当前数据流中最大的时间戳减去延迟的结果
        
        // 注意：如果要测试乱序流，需要取消注释下面的代码，并注释掉上面的有序流代码
        /*
        env
                .addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为 5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .print("乱序流");
        */

        env.execute();
    }
}
```

运行说明

**（1）运行有序流示例**

直接运行 `WatermarkTest` 类的 `main` 方法，会使用有序流水位线生成器。

**（2）运行乱序流示例**

1. 注释掉有序流相关代码
2. 取消注释乱序流相关代码
3. 运行 `main` 方法

**（3）观察输出**

- 有序流：水位线时间戳会随着数据时间戳单调递增
- 乱序流：水位线时间戳 = 最大时间戳 - 延迟时间 - 1毫秒

注意事项

1. **时间戳单位**：时间戳和水位线的单位必须都是毫秒
2. **有序流 vs 乱序流**：
   - 有序流的水位线生成器本质上和乱序流是一样的，相当于延迟设为 0 的乱序流水位线生成器
   - `WatermarkStrategy.forMonotonousTimestamps()` 等价于 `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))`
3. **水位线周期**：默认每 200ms 生成一次水位线，可以通过 `env.getConfig().setAutoWatermarkInterval()` 方法设置

```java
// 设置水位线生成周期为 60 秒
env.getConfig().setAutoWatermarkInterval(60 * 1000L);
```

### 4.2 自定义周期性水位线生成器

文件路径：`src/main/java/com/action/watermark/CustomWatermarkTest.java`

功能说明：演示如何实现自定义的周期性水位线生成器。周期性生成器一般是通过 `onEvent()` 观察判断输入的事件，而在 `onPeriodicEmit()` 里发出水位线。

核心概念

**周期性水位线生成器：**

- `onEvent()`：每个事件（数据）到来都会调用的方法，可以基于事件做各种操作
- `onPeriodicEmit()`：周期性调用的方法，可以由 `WatermarkOutput` 发出水位线
- 周期时间为处理时间，默认 200ms，可以通过 `env.getConfig().setAutoWatermarkInterval()` 方法设置

代码实现

```java
package com.action.watermark;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义水位线生成策略示例
 * 演示如何实现自定义的周期性水位线生成器
 * 
 * 周期性生成器一般是通过 onEvent() 观察判断输入的事件，
 * 而在 onPeriodicEmit() 里发出水位线。
 */
public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    /**
     * 自定义水位线策略
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    /**
     * 自定义周期性水位线生成器
     */
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间（毫秒）
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认 200ms 调用一次
            // 水位线时间戳 = 最大时间戳 - 延迟时间 - 1毫秒
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `CustomWatermarkTest` 类的 `main` 方法。

**（2）观察输出**

- 每来一条数据，`onEvent()` 方法会被调用，更新最大时间戳
- 每 200ms（默认周期），`onPeriodicEmit()` 方法会被调用，发出水位线
- 水位线时间戳 = 最大时间戳 - 延迟时间（5000ms）- 1毫秒

注意事项

1. **延迟时间设置**：根据业务场景的乱序程度设置合适的延迟时间
2. **水位线计算公式**：与内置生成器一致，都是 `最大时间戳 - 延迟时间 - 1毫秒`
3. **周期性生成**：水位线的生成是周期性的，与数据到达无关

### 4.3 自定义断点式水位线生成器

文件路径：`src/main/java/com/action/watermark/CustomPunctuatedWatermarkTest.java`

功能说明：演示如何实现基于事件触发的断点式水位线生成。断点式生成器会不停地检测 `onEvent()` 中的事件，当发现带有水位线信息的特殊事件时，就立即发出水位线。一般来说，断点式生成器不会通过 `onPeriodicEmit()` 发出水位线。

核心概念

**断点式水位线生成器：**

- `onEvent()`：每个事件到来时调用，可以基于事件内容判断是否需要发出水位线
- `onPeriodicEmit()`：一般不需要做任何事情，因为水位线在 `onEvent()` 方法中发射
- 水位线的生成完全依靠事件来触发，所以水位线的生成一定在某个数据到来之后

代码实现

```java
package com.action.watermark;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义断点式水位线生成器示例
 * 演示如何实现基于事件触发的断点式水位线生成
 * 
 * 断点式生成器会不停地检测 onEvent() 中的事件，
 * 当发现带有水位线信息的特殊事件时，就立即发出水位线。
 * 一般来说，断点式生成器不会通过 onPeriodicEmit() 发出水位线。
 */
public class CustomPunctuatedWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomPunctuatedWatermarkStrategy())
                .print();

        env.execute();
    }

    /**
     * 自定义断点式水位线策略
     */
    public static class CustomPunctuatedWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPunctuatedGenerator();
        }
    }

    /**
     * 自定义断点式水位线生成器
     */
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event r, long eventTimestamp, WatermarkOutput output) {
            // 只有在遇到特定的 user 时，才发出水位线
            if (r.user.equals("Mary")) {
                output.emitWatermark(new Watermark(r.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }
}
```

运行说明

**（1）运行代码**

直接运行 `CustomPunctuatedWatermarkTest` 类的 `main` 方法。

**（2）观察输出**

- 只有当数据的 `user` 字段为 "Mary" 时，才会发出水位线
- 水位线时间戳 = 数据时间戳 - 1毫秒
- 其他数据不会触发水位线生成

注意事项

1. **事件触发**：断点式生成器完全依靠事件来触发，适合需要基于特定事件生成水位线的场景
2. **实时性**：相比周期性生成器，断点式生成器可以更及时地响应特定事件
3. **业务逻辑**：可以根据业务需求自定义触发条件，比如特定字段值、特定数据模式等

### 4.4 在数据源中发送水位线

文件路径：`src/main/java/com/action/watermark/EmitWatermarkInSourceFunction.java`

功能说明：演示如何在自定义数据源中直接生成和发送水位线。在自定义数据源中发送了水位线以后，就不能再在程序中使用 `assignTimestampsAndWatermarks` 方法来生成水位线了。二者只能取其一。

核心概念

**在数据源中发送水位线的优势：**

- 更加灵活，可以任意产生周期性的、非周期性的水位线
- 水位线的大小也完全由我们自定义
- 非常适合用来编写 Flink 的测试程序，测试 Flink 的各种各样的特性

**关键方法：**

- `collectWithTimestamp(event, timestamp)`：将数据发送出去，并指明数据中的时间戳字段
- `emitWatermark(new Watermark(timestamp))`：发送水位线

代码实现

**（1）数据源实现**

`src/main/java/com/action/ClickSourceWithWatermark.java`

```java
package com.action;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源：在数据源中发送水位线
 * 用于演示在 SourceFunction 中直接生成和发送水位线
 * 
 * 注意：在自定义数据源中发送了水位线以后，就不能再在程序中使用 
 * assignTimestampsAndWatermarks 方法来生成水位线了。二者只能取其一。
 */
public class ClickSourceWithWatermark implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] userArr = {"Mary", "Bob", "Alice"};
        String[] urlArr = {"./home", "./cart", "./prod?id=1"};

        while (running) {
            long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
            String username = userArr[random.nextInt(userArr.length)];
            String url = urlArr[random.nextInt(urlArr.length)];
            Event event = new Event(username, url, currTs);
            
            // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
            sourceContext.collectWithTimestamp(event, event.timestamp);
            
            // 发送水位线
            sourceContext.emitWatermark(new Watermark(event.timestamp - 1L));
            
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
```

**（2）使用示例**

`src/main/java/com/action/watermark/EmitWatermarkInSourceFunction.java`

```java
package com.action.watermark;

import com.action.ClickSourceWithWatermark;
import com.action.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 在自定义数据源中发送水位线示例
 * 
 * 注意：
 * 1. 在自定义数据源中发送了水位线以后，就不能再在程序中使用 
 *    assignTimestampsAndWatermarks 方法来生成水位线了。二者只能取其一。
 * 2. 在自定义水位线中生成水位线相比 assignTimestampsAndWatermarks 方法更加灵活，
 *    可以任意的产生周期性的、非周期性的水位线，以及水位线的大小也完全由我们自定义。
 *    所以非常适合用来编写 Flink 的测试程序，测试 Flink 的各种各样的特性。
 */
public class EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }
}
```

运行说明

**（1）运行代码**

直接运行 `EmitWatermarkInSourceFunction` 类的 `main` 方法。

**（2）观察输出**

- 每条数据发送后，立即发送一条水位线
- 水位线时间戳 = 数据时间戳 - 1毫秒

注意事项

1. **互斥性**：在自定义数据源中发送了水位线以后，不能再使用 `assignTimestampsAndWatermarks` 方法
2. **灵活性**：可以任意产生周期性的、非周期性的水位线，水位线大小完全自定义
3. **适用场景**：非常适合编写 Flink 测试程序，测试 Flink 的各种特性

## 5. 水位线特性总结

### 5.1 水位线的特性

1. **水位线是插入到数据流中的一个标记**，可以认为是一个特殊的数据
2. **水位线主要的内容是一个时间戳**，用来表示当前事件时间的进展
3. **水位线是基于数据的时间戳生成的**
4. **水位线的时间戳必须单调递增**，以确保任务的事件时间时钟一直向前推进
5. **水位线可以通过设置延迟**，来保证正确处理乱序数据
6. **一个水位线 Watermark(t)**，表示在当前流中事件时间已经达到了时间戳 t，这代表 t 之前的所有数据都到齐了，之后流中不会出现时间戳 t' ≤ t 的数据

### 5.2 水位线的传递

- 水位线是数据流中的一部分，随着数据一起流动，在不同任务之间传输
- 在"重分区"（redistributing）的传输模式下，一个任务有可能会收到来自不同分区上游子任务的数据
- 如果一个任务收到了来自上游并行任务的不同的水位线，当前任务的时钟会以最小的水位线为准（木桶原理）
- 水位线在上下游任务之间的传递，非常巧妙地避免了分布式系统中没有统一时钟的问题

### 5.3 水位线的默认计算公式

```
水位线 = 观察到的最大事件时间 - 最大延迟时间 - 1毫秒
```

### 5.4 水位线的初始化和结束

- 在数据流开始之前，Flink 会插入一个大小是负无穷大（在 Java 中是 `-Long.MAX_VALUE`）的水位线
- 在数据流结束时，Flink 会插入一个正无穷大（`Long.MAX_VALUE`）的水位线，保证所有的窗口闭合以及所有的定时器都被触发
- 对于离线数据集，Flink 只会插入两次水位线：在最开始处插入负无穷大的水位线，在结束位置插入一个正无穷大的水位线

### 5.5 水位线阻塞问题

- 不同的算子看到的水位线的大小可能是不一样的
- 因为下游的算子可能并未接收到来自上游算子的水位线，导致下游算子的时钟要落后于上游算子的时钟
- 如果在算子中编写了非常耗时间的代码，将会阻塞水位线的向下传播
- 水位线也是数据流中的一个事件，位于水位线前面的数据如果没有处理完毕，那么水位线不可能弯道超车绕过前面的数据向下游传播
- 在编写 Flink 程序时，一定要谨慎的编写每一个算子的计算逻辑，尽量避免大量计算或者是大量的 IO 操作，这样才不会阻塞水位线的向下传递

## 6. 常见问题

### 6.1 时间语义选择

**Q1：什么时候使用处理时间，什么时候使用事件时间？**

- **处理时间**：适用于对实时性要求极高、而对计算准确性要求不太高的场景
- **事件时间**：适用于需要保证计算准确性的场景，是实际应用中最常见的时间语义
- **摄入时间**：相当于事件时间和处理时间的一个中和，可以保证比较好的正确性，同时又不会引入太大的延迟

### 6.2 水位线延迟设置

**Q2：如何设置合适的水位线延迟时间？**

- 需要根据业务场景的乱序程度来设置
- 如果知道当前业务中事件的迟到时间不会超过 5 秒，那就可以将水位线的延迟时间设为 5 秒
- 可以单独创建一个 Flink 作业来监控事件流，建立概率分布或者机器学习模型，学习事件的迟到规律
- 延迟时间设置得越高，计算结果越准确，但实时性会降低
- 延迟时间设置得越低，实时性越强，但可能遗漏迟到数据

### 6.3 水位线生成方式选择

**Q3：什么时候使用周期性生成器，什么时候使用断点式生成器？**

- **周期性生成器**：适用于大多数场景，可以定期更新水位线，不需要依赖特定事件
- **断点式生成器**：适用于需要基于特定事件（如特定字段值、特定数据模式）生成水位线的场景

### 6.4 在数据源中发送水位线

**Q4：什么时候应该在数据源中发送水位线？**

- 当需要更灵活地控制水位线生成时
- 当需要编写 Flink 测试程序时
- 当数据源本身已经包含了水位线信息时

**注意**：在自定义数据源中发送了水位线以后，不能再在程序中使用 `assignTimestampsAndWatermarks` 方法。

## 7. 快速参考

### 7.1 代码位置

- **Event 类**：`src/main/java/com/action/Event.java`
- **ClickSource 类**：`src/main/java/com/action/ClickSource.java`
- **ClickSourceWithWatermark 类**：`src/main/java/com/action/ClickSourceWithWatermark.java`
- **内置水位线生成器示例**：`src/main/java/com/action/watermark/WatermarkTest.java`
- **自定义周期性水位线生成器示例**：`src/main/java/com/action/watermark/CustomWatermarkTest.java`
- **自定义断点式水位线生成器示例**：`src/main/java/com/action/watermark/CustomPunctuatedWatermarkTest.java`
- **在数据源中发送水位线示例**：`src/main/java/com/action/watermark/EmitWatermarkInSourceFunction.java`

### 7.2 关键 API

- `assignTimestampsAndWatermarks(WatermarkStrategy)`：为流中的数据分配时间戳，并生成水位线
- `WatermarkStrategy.forMonotonousTimestamps()`：有序流水位线生成器
- `WatermarkStrategy.forBoundedOutOfOrderness(Duration)`：乱序流水位线生成器
- `withTimestampAssigner(TimestampAssigner)`：指定时间戳提取逻辑
- `collectWithTimestamp(event, timestamp)`：在数据源中发送带时间戳的数据
- `emitWatermark(Watermark)`：在数据源中发送水位线

### 7.3 学习建议

1. 先理解时间语义的概念（处理时间、事件时间、摄入时间）
2. 理解水位线的作用和特性
3. 掌握 Flink 内置水位线生成器的使用
4. 学习自定义水位线生成策略的实现
5. 了解水位线在分布式系统中的传递机制
6. 注意避免阻塞水位线传递的代码编写

## 8. 参考资料

- flink 时间属性：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/concepts/time_attributes/
- flink 水位线：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/event-time/generating_watermarks/

