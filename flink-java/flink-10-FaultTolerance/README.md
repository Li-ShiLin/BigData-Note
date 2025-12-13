<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink 容错机制演示](#flink-%E5%AE%B9%E9%94%99%E6%9C%BA%E5%88%B6%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. 检查点（Checkpoint）](#4-%E6%A3%80%E6%9F%A5%E7%82%B9checkpoint)
    - [4.1 基本概念](#41-%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)
    - [4.2 检查点配置示例](#42-%E6%A3%80%E6%9F%A5%E7%82%B9%E9%85%8D%E7%BD%AE%E7%A4%BA%E4%BE%8B)
    - [4.3 保存点（Savepoint）使用示例](#43-%E4%BF%9D%E5%AD%98%E7%82%B9savepoint%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B)
  - [5. 状态一致性](#5-%E7%8A%B6%E6%80%81%E4%B8%80%E8%87%B4%E6%80%A7)
    - [5.1 基本概念](#51-%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)
    - [5.2 状态一致性保证示例](#52-%E7%8A%B6%E6%80%81%E4%B8%80%E8%87%B4%E6%80%A7%E4%BF%9D%E8%AF%81%E7%A4%BA%E4%BE%8B)
    - [5.3 端到端的状态一致性](#53-%E7%AB%AF%E5%88%B0%E7%AB%AF%E7%9A%84%E7%8A%B6%E6%80%81%E4%B8%80%E8%87%B4%E6%80%A7)
    - [5.4 端到端精确一次（end-to-end exactly-once）](#54-%E7%AB%AF%E5%88%B0%E7%AB%AF%E7%B2%BE%E7%A1%AE%E4%B8%80%E6%AC%A1end-to-end-exactly-once)
  - [6. 容错机制总结](#6-%E5%AE%B9%E9%94%99%E6%9C%BA%E5%88%B6%E6%80%BB%E7%BB%93)
    - [6.1 检查点与保存点对比](#61-%E6%A3%80%E6%9F%A5%E7%82%B9%E4%B8%8E%E4%BF%9D%E5%AD%98%E7%82%B9%E5%AF%B9%E6%AF%94)
    - [6.2 状态一致性级别对比](#62-%E7%8A%B6%E6%80%81%E4%B8%80%E8%87%B4%E6%80%A7%E7%BA%A7%E5%88%AB%E5%AF%B9%E6%AF%94)
    - [6.3 容错机制使用建议](#63-%E5%AE%B9%E9%94%99%E6%9C%BA%E5%88%B6%E4%BD%BF%E7%94%A8%E5%BB%BA%E8%AE%AE)
  - [7. 快速参考](#7-%E5%BF%AB%E9%80%9F%E5%8F%82%E8%80%83)
    - [7.1 代码位置](#71-%E4%BB%A3%E7%A0%81%E4%BD%8D%E7%BD%AE)
    - [7.2 关键 API](#72-%E5%85%B3%E9%94%AE-api)
    - [7.3 命令行工具](#73-%E5%91%BD%E4%BB%A4%E8%A1%8C%E5%B7%A5%E5%85%B7)
  - [8. 参考资料](#8-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink 容错机制演示

## 1. 项目作用

本项目演示了 Flink 中容错机制的相关知识点，包括：

- 检查点（Checkpoint）的配置和使用
- 检查点存储配置
- 保存点（Savepoint）的使用
- 状态一致性保证
- 端到端精确一次（end-to-end exactly-once）保证

通过实际代码示例帮助开发者快速掌握 Flink 容错机制的配置和使用方法。

## 2. 项目结构

```
flink-10-FaultTolerance/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── faulttolerance/
│       ├── CheckpointConfigExample.java                 # 检查点配置示例
│       ├── SavepointExample.java                        # 保存点使用示例
│       └── StateConsistencyExample.java                 # 状态一致性保证示例
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
 * 用于演示 Flink 容错机制相关功能
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
 * 用于演示容错机制相关功能
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

## 4. 检查点（Checkpoint）

### 4.1 基本概念

检查点是 Flink 容错机制的核心。检查点作为应用状态的一份"存档"，其实就是所有任务状态在同一时间点的一个"快照"（snapshot），它的触发是周期性的。

**检查点的作用：**

- 故障恢复：当发生故障时，可以从检查点恢复状态，继续处理数据
- 状态一致性：保证故障恢复后结果的正确性，实现"精确一次"（exactly-once）或"至少一次"（at-least-once）的状态一致性保证

**检查点的保存时机：**

- 周期性触发：每隔一段时间进行一次检查点保存
- 所有任务处理完同一个输入数据时保存状态，保证状态的一致性

### 4.2 检查点配置示例

文件路径：`src/main/java/com/action/faulttolerance/CheckpointConfigExample.java`

功能说明：演示如何配置 Flink 的检查点机制，包括启用检查点、设置检查点模式、配置超时时间、最小间隔时间、最大并发检查点数量、外部持久化存储、不对齐检查点等。

核心概念

**检查点配置项：**

1. **检查点模式（CheckpointingMode）**：设置检查点一致性的保证级别
   - `EXACTLY_ONCE`：精确一次（默认），保证数据只处理一次
   - `AT_LEAST_ONCE`：至少一次，保证数据不丢失，但可能重复处理

2. **超时时间（checkpointTimeout）**：检查点保存的超时时间，超时没完成就会被丢弃

3. **最小间隔时间（minPauseBetweenCheckpoints）**：上一个检查点完成之后，最快等多久可以触发保存下一个检查点

4. **最大并发检查点数量（maxConcurrentCheckpoints）**：运行中的检查点最多可以有多少个

5. **外部持久化存储（enableExternalizedCheckpoints）**：开启检查点的外部持久化

6. **不对齐检查点（enableUnalignedCheckpoints）**：不再执行检查点的分界线对齐操作，可以大大减少产生背压时的检查点保存时间

代码实现

```java
package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * 检查点配置示例
 * 
 * 演示如何配置 Flink 的检查点机制，包括：
 * - 启用检查点
 * - 设置检查点模式（精确一次/至少一次）
 * - 配置检查点超时时间
 * - 配置最小间隔时间
 * - 配置最大并发检查点数量
 * - 配置外部持久化存储
 * - 配置不对齐检查点
 */
public class CheckpointConfigExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 启用检查点 ====================
        // 每隔 1 秒启动一次检查点保存
        // 如果不传参数直接启用检查点，默认的间隔周期为 500 毫秒，这种方式已经被弃用
        env.enableCheckpointing(1000);

        // ==================== 2. 获取检查点配置对象 ====================
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // ==================== 3. 设置检查点模式 ====================
        // 设置检查点一致性的保证级别
        // EXACTLY_ONCE：精确一次（默认），保证数据只处理一次
        // AT_LEAST_ONCE：至少一次，保证数据不丢失，但可能重复处理
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ==================== 4. 设置检查点超时时间 ====================
        // 用于指定检查点保存的超时时间，超时没完成就会被丢弃掉
        // 这里设置为 1 分钟
        checkpointConfig.setCheckpointTimeout(60000);

        // ==================== 5. 设置最小间隔时间 ====================
        // 用于指定在上一个检查点完成之后，检查点协调器最快等多久可以触发保存下一个检查点
        // 这就意味着即使已经达到了周期触发的时间点，只要距离上一个检查点完成的间隔不够，就依然不能开启下一次检查点的保存
        // 当指定这个参数时，maxConcurrentCheckpoints 的值强制为 1
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // ==================== 6. 设置最大并发检查点数量 ====================
        // 用于指定运行中的检查点最多可以有多少个
        // 由于每个任务的处理进度不同，完全可能出现后面的任务还没完成前一个检查点的保存、前面任务已经开始保存下一个检查点了
        // 如果前面设置了 minPauseBetweenCheckpoints，则这个参数就不起作用了
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // ==================== 7. 开启外部持久化存储 ====================
        // 用于开启检查点的外部持久化，而且默认在作业失败的时候不会自动清理
        // RETAIN_ON_CANCELLATION：作业取消的时候也会保留外部检查点
        // DELETE_ON_CANCELLATION：在作业取消的时候会自动删除外部检查点，但是如果是作业失败退出，则会保留检查点
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ==================== 8. 启用不对齐检查点 ====================
        // 不再执行检查点的分界线对齐操作，启用之后可以大大减少产生背压时的检查点保存时间
        // 这个设置要求检查点模式必须为 EXACTLY_ONCE，并且并发的检查点个数为 1
        checkpointConfig.enableUnalignedCheckpoints();

        // ==================== 9. 设置检查点异常时是否让整个任务失败 ====================
        // 用于指定在检查点发生异常的时候，是否应该让任务直接失败退出
        // 默认为 true，如果设置为 false，则任务会丢弃掉检查点然后继续运行
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        // ==================== 10. 设置状态后端 ====================
        // 使用 HashMapStateBackend，将状态存储在内存中
        env.setStateBackend(new HashMapStateBackend());

        // ==================== 11. 设置检查点存储 ====================
        // 配置存储检查点到文件系统（生产环境推荐使用 HDFS 等分布式文件系统）
        // 这里使用本地文件系统作为示例
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 12. 设置重启策略 ====================
        // 固定延迟重启策略：失败后最多重启 3 次，每次重启间隔 10 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // ==================== 13. 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 简单的处理逻辑：统计每个用户的访问次数
        stream.keyBy(data -> data.user)
                .map(data -> {
                    // 模拟处理逻辑
                    return data.user + " 访问了 " + data.url;
                })
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        // 模拟输出到外部系统
                        System.out.println("输出: " + value);
                    }
                });

        env.execute("Checkpoint Config Example");
    }
}
```

运行说明

**（1）运行代码**

直接运行 `CheckpointConfigExample` 类的 `main` 方法。

**（2）观察输出**

- 程序会周期性地保存检查点（每 1 秒一次）
- 检查点会保存到配置的文件系统路径中
- 如果发生故障，可以从检查点恢复状态

**（3）检查点存储位置**

检查点会保存到 `file:///tmp/flink-checkpoints` 目录下（生产环境应使用 HDFS 等分布式文件系统）。

注意事项

1. **检查点间隔时间**：是对处理性能和故障恢复速度的一个权衡。如果希望对性能的影响更小，可以调大间隔时间；而如果希望故障重启后迅速赶上实时的数据处理，就需要将间隔时间设小一些。
2. **检查点存储**：生产环境推荐使用高可用的分布式文件系统（HDFS、S3 等），而不是本地文件系统。
3. **不对齐检查点**：启用不对齐检查点可以大大减少产生背压时的检查点保存时间，但要求检查点模式必须为 `EXACTLY_ONCE`，并且并发的检查点个数为 1。

### 4.3 保存点（Savepoint）使用示例

文件路径：`src/main/java/com/action/faulttolerance/SavepointExample.java`

功能说明：演示保存点（Savepoint）的使用。保存点与检查点最大的区别，就是触发的时机：检查点是由 Flink 自动管理的，定期创建，发生故障之后自动读取进行恢复，这是一个"自动存盘"的功能；而保存点不会自动创建，必须由用户明确地手动触发保存操作，所以就是"手动存盘"。

核心概念

**保存点的用途：**

1. **版本管理和归档存储**：对重要的节点进行手动备份，设置为某一版本，归档存储应用程序的状态
2. **更新 Flink 版本**：创建保存点，停掉应用、升级 Flink 后，从保存点重启就可以继续处理
3. **更新应用程序**：修复应用程序中的逻辑 bug，更新之后接着继续处理
4. **调整并行度**：将应用程序的并行度增大或减小
5. **暂停应用程序**：灵活实现应用的暂停和重启，对有限的集群资源做最好的优化配置

**保存点的特点：**

- 保存点的原理和算法与检查点完全相同，只是多了一些额外的元数据
- 保存点中的状态快照，是以算子 ID 和状态名称组织起来的
- 从保存点启动应用程序时，Flink 会将保存点的状态数据重新分配给相应的算子任务
- 保存点能够在程序更改的时候依然兼容，前提是状态的拓扑结构和数据类型不变

代码实现

```java
package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 保存点（Savepoint）使用示例
 * 
 * 保存点与检查点最大的区别，就是触发的时机：
 * - 检查点是由 Flink 自动管理的，定期创建，发生故障之后自动读取进行恢复，这是一个"自动存盘"的功能
 * - 保存点不会自动创建，必须由用户明确地手动触发保存操作，所以就是"手动存盘"
 * 
 * 保存点的用途：
 * - 版本管理和归档存储
 * - 更新 Flink 版本
 * - 更新应用程序
 * - 调整并行度
 * - 暂停应用程序
 * 
 * 注意：为了方便后续的维护，强烈建议在程序中为每一个算子手动指定 ID
 */
public class SavepointExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 启用检查点（保存点基于检查点机制） ====================
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 设置状态后端 ====================
        env.setStateBackend(new HashMapStateBackend());

        // ==================== 设置默认保存点目录 ====================
        // 保存点的默认路径可以通过配置文件 flink-conf.yaml 中的 state.savepoints.dir 项来设定
        // 也可以在程序代码中通过执行环境来设置
        env.setDefaultSavepointDirectory("file:///tmp/flink-savepoints");

        // ==================== 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .uid("source-id")  // 为算子指定 ID，方便从保存点恢复
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .uid("watermark-assigner-id");  // 为算子指定 ID

        // 统计每个用户的访问次数，使用状态保存计数
        stream.keyBy(data -> data.user)
                .process(new UserVisitCountFunction())
                .uid("visit-count-processor-id")  // 为算子指定 ID
                .print("用户访问统计");

        env.execute("Savepoint Example");
    }

    /**
     * 自定义处理函数：统计每个用户的访问次数
     * 使用状态保存计数，这样可以从保存点恢复状态
     */
    public static class UserVisitCountFunction extends KeyedProcessFunction<String, Event, String> {
        // 定义状态，保存每个用户的访问次数
        private ValueState<Long> visitCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            visitCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("visit-count", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新访问次数
            Long count = visitCountState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            visitCountState.update(count);

            // 输出统计结果
            out.collect(value.user + " 访问次数: " + count);
        }
    }
}

/**
 * 保存点的使用说明：
 * 
 * 1. 创建保存点（在命令行中执行）：
 *    bin/flink savepoint <jobId> [targetDirectory]
 *    例如：bin/flink savepoint abc123def456 /tmp/flink-savepoints
 * 
 * 2. 停止作业并创建保存点：
 *    bin/flink stop --savepointPath [targetDirectory] <jobId>
 *    例如：bin/flink stop --savepointPath /tmp/flink-savepoints abc123def456
 * 
 * 3. 从保存点重启应用：
 *    bin/flink run -s <savepointPath> [runArgs]
 *    例如：bin/flink run -s /tmp/flink-savepoints/savepoint-abc123 -c com.action.faulttolerance.SavepointExample
 * 
 * 注意事项：
 * - 保存点能够在程序更改的时候依然兼容，前提是状态的拓扑结构和数据类型不变
 * - 保存点中状态都是以算子 ID-状态名称这样的 key-value 组织起来的
 * - 对于没有设置 ID 的算子，Flink 默认会自动进行设置，所以在重新启动应用后可能会导致 ID 不同而无法兼容以前的状态
 * - 强烈建议在程序中为每一个算子手动指定 ID
 */
```

运行说明

**（1）运行代码**

直接运行 `SavepointExample` 类的 `main` 方法。

**（2）创建保存点**

在命令行中为运行的作业创建一个保存点镜像：

```bash
# 创建保存点
bin/flink savepoint <jobId> [targetDirectory]

# 例如：
bin/flink savepoint abc123def456 /tmp/flink-savepoints
```

**（3）停止作业并创建保存点**

也可以在停掉一个作业时直接创建保存点：

```bash
bin/flink stop --savepointPath [targetDirectory] <jobId>

# 例如：
bin/flink stop --savepointPath /tmp/flink-savepoints abc123def456
```

**（4）从保存点重启应用**

从保存点重启一个应用：

```bash
bin/flink run -s <savepointPath> [runArgs]

# 例如：
bin/flink run -s /tmp/flink-savepoints/savepoint-abc123 -c com.action.faulttolerance.SavepointExample
```

注意事项

1. **算子 ID**：保存点中状态都是以算子 ID-状态名称这样的 key-value 组织起来的。对于没有设置 ID 的算子，Flink 默认会自动进行设置，所以在重新启动应用后可能会导致 ID 不同而无法兼容以前的状态。强烈建议在程序中为每一个算子手动指定 ID。
2. **状态兼容性**：保存点能够在程序更改的时候依然兼容，前提是状态的拓扑结构和数据类型不变。
3. **保存点存储**：生产环境推荐使用高可用的分布式文件系统（HDFS、S3 等）来存储保存点。

## 5. 状态一致性

### 5.1 基本概念

状态一致性其实就是结果的正确性。对于 Flink 来说，多个节点并行处理不同的任务，我们要保证计算结果是正确的，就必须不漏掉任何一个数据，而且也不会重复处理同一个数据。

**状态一致性有三种级别：**

1. **最多一次（AT-MOST-ONCE）**：当任务发生故障时，直接重启，既不恢复丢失的状态，也不重放丢失的数据。每个数据在正常情况下会被处理一次，遇到故障时就会丢掉，所以就是"最多处理一次"。

2. **至少一次（AT-LEAST-ONCE）**：所有数据都不会丢，肯定被处理了；不过不能保证只处理一次，有些数据会被重复处理。适用于对重复处理不敏感的场景（如去重统计）。

3. **精确一次（EXACTLY-ONCE）**：所有数据不仅不会丢失，而且只被处理一次，不会重复处理。可以真正意义上保证结果的绝对正确，在发生故障恢复后，就好像从未发生过故障一样。

### 5.2 状态一致性保证示例

文件路径：`src/main/java/com/action/faulttolerance/StateConsistencyExample.java`

功能说明：演示如何通过检查点配置来保证不同的状态一致性级别。通过检查点机制，可以保证状态在故障恢复后的一致性。

核心概念

**状态一致性保证的关键：**

1. **检查点模式**：通过设置 `CheckpointingMode.EXACTLY_ONCE` 或 `CheckpointingMode.AT_LEAST_ONCE` 来保证不同的一致性级别
2. **数据重放**：需要外部数据源支持数据重放（如 Kafka），才能保证数据不丢失
3. **重启策略**：配置合适的重启策略，确保故障后能够自动恢复

代码实现

```java
package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 状态一致性保证示例
 * 
 * 状态一致性有三种级别：
 * 1. 最多一次（AT-MOST-ONCE）：当任务发生故障时，直接重启，不恢复丢失的状态，也不重放丢失的数据
 * 2. 至少一次（AT-LEAST-ONCE）：所有数据都不会丢，肯定被处理了；不过不能保证只处理一次，有些数据会被重复处理
 * 3. 精确一次（EXACTLY-ONCE）：所有数据不仅不会丢失，而且只被处理一次，不会重复处理
 * 
 * 本示例演示如何通过检查点配置来保证不同的状态一致性级别
 */
public class StateConsistencyExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 配置检查点 ====================
        // 启用检查点，间隔时间 1 秒
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // ==================== 精确一次（EXACTLY-ONCE）配置 ====================
        // 设置检查点模式为精确一次，这是最严格的一致性保证
        // 可以真正意义上保证结果的绝对正确，在发生故障恢复后，就好像从未发生过故障一样
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000);

        // 设置最小间隔时间，确保检查点之间有足够的间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // 设置最大并发检查点数量为 1
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 开启外部持久化存储
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ==================== 至少一次（AT-LEAST-ONCE）配置示例 ====================
        // 如果希望使用至少一次语义，可以这样配置：
        // checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 至少一次语义对于大多数低延迟的流处理程序来说已经足够，而且处理效率会更高

        // ==================== 设置状态后端 ====================
        env.setStateBackend(new HashMapStateBackend());
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 设置重启策略 ====================
        // 固定延迟重启策略：失败后最多重启 3 次，每次重启间隔 10 秒
        // 这样可以保证在故障恢复时能够重放数据，达到至少一次或精确一次的语义
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // ==================== 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 使用状态统计每个用户的访问次数
        // 通过检查点机制，可以保证状态的一致性
        stream.keyBy(data -> data.user)
                .process(new ConsistentStateProcessFunction())
                .print("一致性状态统计");

        env.execute("State Consistency Example");
    }

    /**
     * 自定义处理函数：使用状态统计访问次数
     * 通过检查点机制，可以保证状态在故障恢复后的一致性
     */
    public static class ConsistentStateProcessFunction extends KeyedProcessFunction<String, Event, String> {
        // 定义状态，保存每个用户的访问次数
        private ValueState<Long> visitCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            // 状态会被保存到检查点中，故障恢复后可以从检查点恢复
            visitCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("visit-count", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新访问次数
            Long count = visitCountState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            visitCountState.update(count);

            // 输出统计结果
            // 通过检查点机制，可以保证：
            // - 精确一次：每个数据只被处理一次，状态和输出结果都只包含一次处理
            // - 至少一次：数据不丢失，但可能重复处理
            out.collect(value.user + " 访问次数: " + count + " (时间戳: " + value.timestamp + ")");
        }
    }
}

/**
 * 状态一致性保证说明：
 * 
 * 1. 精确一次（EXACTLY-ONCE）：
 *    - 需要设置检查点模式为 EXACTLY_ONCE
 *    - 需要外部数据源支持数据重放（如 Kafka）
 *    - 需要外部存储系统支持幂等写入或事务写入
 *    - 可以真正意义上保证结果的绝对正确
 * 
 * 2. 至少一次（AT-LEAST-ONCE）：
 *    - 需要设置检查点模式为 AT_LEAST_ONCE
 *    - 需要外部数据源支持数据重放
 *    - 数据不会丢失，但可能重复处理
 *    - 适用于对重复处理不敏感的场景（如去重统计）
 * 
 * 3. 最多一次（AT-MOST-ONCE）：
 *    - 不启用检查点或检查点失败时不重启
 *    - 数据可能丢失，但不会重复处理
 *    - 适用于对数据丢失不敏感的场景
 */
```

运行说明

**（1）运行代码**

直接运行 `StateConsistencyExample` 类的 `main` 方法。

**（2）观察输出**

- 程序会周期性地保存检查点，保证状态的一致性
- 如果发生故障，可以从检查点恢复状态，继续处理数据
- 通过检查点机制，可以保证不同的一致性级别

**（3）一致性级别说明**

- **精确一次（EXACTLY-ONCE）**：需要设置检查点模式为 `EXACTLY_ONCE`，需要外部数据源支持数据重放（如 Kafka），需要外部存储系统支持幂等写入或事务写入
- **至少一次（AT-LEAST-ONCE）**：需要设置检查点模式为 `AT_LEAST_ONCE`，需要外部数据源支持数据重放，数据不会丢失，但可能重复处理
- **最多一次（AT-MOST-ONCE）**：不启用检查点或检查点失败时不重启，数据可能丢失，但不会重复处理

注意事项

1. **数据源要求**：要实现至少一次或精确一次语义，外部数据源必须支持数据重放（如 Kafka），否则只能保证最多一次语义
2. **输出端要求**：要实现精确一次语义，外部存储系统必须支持幂等写入或事务写入
3. **重启策略**：需要配置合适的重启策略，确保故障后能够自动恢复

### 5.3 端到端的状态一致性

我们已经知道检查点可以保证 Flink 内部状态的一致性，而且可以做到精确一次（exactly-once）。但在实际应用中，一般要保证从用户的角度看来，最终消费的数据是正确的。

**端到端一致性的关键：**

完整的流处理应用，应该包括了数据源、流处理器和外部存储系统三个部分。这个完整应用的一致性，就叫作"端到端（end-to-end）的状态一致性"，它取决于三个组件中最弱的那一环。

- **能否达到 at-least-once 一致性级别**：主要看数据源能够重放数据
- **能否达到 exactly-once 级别**：流处理器内部、数据源、外部存储都要有相应的保证机制

### 5.4 端到端精确一次（end-to-end exactly-once）

实际应用中，最难做到、也最希望做到的一致性语义，无疑就是端到端（end-to-end）的"精确一次"（exactly-once）。

**端到端一致性的关键点：**

1. **Flink 内部**：通过检查点机制可以保证故障恢复后数据不丢（在能够重放的前提下），并且只处理一次，所以已经可以做到 exactly-once 的一致性语义了

2. **输入端保证**：外部数据源必须拥有重放数据的能力。常见的做法就是对数据进行持久化保存，并且可以重设数据的读取位置。一个最经典的应用就是 Kafka。在 Flink 的 Source 任务中将数据读取的偏移量保存为状态，这样就可以在故障恢复时从检查点中读取出来，对数据源重置偏移量，重新获取数据

3. **输出端保证**：为了实现端到端 exactly-once，我们还需要对外部存储系统、以及 Sink 连接器有额外的要求。能够保证 exactly-once 一致性的写入方式有两种：
   - **幂等写入**：一个操作可以重复执行很多次，但只导致一次结果更改
   - **事务写入**：用一个事务来进行数据向外部系统的写入，这个事务是与检查点绑定在一起的

**输出端保证的实现方式：**

1. **预写日志（WAL）**：
   - 先把结果数据作为日志状态保存起来
   - 进行检查点保存时，也会将这些结果数据一并做持久化存储
   - 在收到检查点完成的通知时，将所有结果一次性写入外部系统
   - Flink 提供了 `GenericWriteAheadSink` 模板类来实现这种事务型的写入方式

2. **两阶段提交（2PC）**：
   - 当第一条数据到来时，或者收到检查点的分界线时，Sink 任务都会启动一个事务
   - 接下来接收到的所有数据，都通过这个事务写入外部系统；这时由于事务没有提交，所以数据尽管写入了外部系统，但是不可用，是"预提交"的状态
   - 当 Sink 任务收到 JobManager 发来检查点完成的通知时，正式提交事务，写入的结果就真正可用了
   - Flink 提供了 `TwoPhaseCommitSinkFunction` 接口，方便我们自定义实现两阶段提交的 SinkFunction

**Flink 和 Kafka 连接时的精确一次保证：**

Flink 官方实现的 Kafka 连接器中，提供了写入到 Kafka 的 `FlinkKafkaProducer`，它就实现了 `TwoPhaseCommitSinkFunction` 接口。我们写入 Kafka 的过程实际上是一个两段式的提交：处理完毕得到结果，写入 Kafka 时是基于事务的"预提交"；等到检查点保存完毕，才会提交事务进行"正式提交"。

**需要的配置：**

1. 必须启用检查点
2. 在 `FlinkKafkaProducer` 的构造函数中传入参数 `Semantic.EXACTLY_ONCE`
3. 配置 Kafka 读取数据的消费者的隔离级别为 `read_committed`
4. 配置事务超时时间：Flink 的 Kafka 连接器中配置的事务超时时间应该小于等于 Kafka 集群配置的事务最大超时时间

## 6. 容错机制总结

### 6.1 检查点与保存点对比

| 特性 | 检查点（Checkpoint） | 保存点（Savepoint） |
|------|---------------------|---------------------|
| 触发方式 | 自动触发（周期性） | 手动触发 |
| 用途 | 故障恢复 | 版本管理、更新应用、调整并行度等 |
| 存储位置 | 由 CheckpointStorage 配置 | 由 state.savepoints.dir 配置 |
| 元数据 | 基本元数据 | 额外的元数据（算子 ID 等） |
| 兼容性 | 要求拓扑结构不变 | 要求拓扑结构和数据类型不变 |

### 6.2 状态一致性级别对比

| 一致性级别 | 数据丢失 | 数据重复 | 实现难度 | 适用场景 |
|-----------|---------|---------|---------|---------|
| 最多一次（AT-MOST-ONCE） | 可能丢失 | 不会重复 | 简单 | 对数据丢失不敏感的场景 |
| 至少一次（AT-LEAST-ONCE） | 不会丢失 | 可能重复 | 中等 | 对重复处理不敏感的场景（如去重统计） |
| 精确一次（EXACTLY-ONCE） | 不会丢失 | 不会重复 | 困难 | 要求结果绝对正确的场景 |

### 6.3 容错机制使用建议

1. **检查点配置**：
   - 生产环境必须启用检查点
   - 检查点间隔时间要权衡处理性能和故障恢复速度
   - 检查点存储应使用高可用的分布式文件系统（HDFS、S3 等）

2. **保存点使用**：
   - 为每个算子手动指定 ID，方便从保存点恢复
   - 在更新应用前创建保存点，确保可以回滚
   - 定期创建保存点，用于版本管理和归档存储

3. **状态一致性**：
   - 根据业务需求选择合适的一致性级别
   - 实现端到端精确一次需要数据源和输出端都支持相应的机制
   - Kafka 是 Flink 实现端到端精确一次的最佳选择

4. **重启策略**：
   - 配置合适的重启策略，确保故障后能够自动恢复
   - 固定延迟重启策略适用于大多数场景

## 7. 快速参考

### 7.1 代码位置

- **Event 类**：`src/main/java/com/action/Event.java`
- **ClickSource 类**：`src/main/java/com/action/ClickSource.java`
- **检查点配置示例**：`src/main/java/com/action/faulttolerance/CheckpointConfigExample.java`
- **保存点使用示例**：`src/main/java/com/action/faulttolerance/SavepointExample.java`
- **状态一致性保证示例**：`src/main/java/com/action/faulttolerance/StateConsistencyExample.java`

### 7.2 关键 API

- `env.enableCheckpointing(interval)`：启用检查点，设置间隔时间
- `checkpointConfig.setCheckpointingMode(mode)`：设置检查点模式（EXACTLY_ONCE 或 AT_LEAST_ONCE）
- `checkpointConfig.setCheckpointStorage(path)`：设置检查点存储路径
- `env.setDefaultSavepointDirectory(path)`：设置默认保存点目录
- `stream.uid("id")`：为算子指定 ID，方便从保存点恢复
- `env.setRestartStrategy(strategy)`：设置重启策略

### 7.3 命令行工具

**创建保存点：**
```bash
bin/flink savepoint <jobId> [targetDirectory]
```

**停止作业并创建保存点：**
```bash
bin/flink stop --savepointPath [targetDirectory] <jobId>
```

**从保存点重启应用：**
```bash
bin/flink run -s <savepointPath> [runArgs]
```

## 8. 参考资料

- Flink 官方文档 - 检查点：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/checkpointing/
- Flink 官方文档 - 保存点：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/ops/state/savepoints/
- Flink 官方文档 - 容错保证：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/

[State TTL Migration Compatibility | Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state_migration/)
