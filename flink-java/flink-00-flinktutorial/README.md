<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink Chapter07-08 ProcessFunction & 多流转换指南](#flink-chapter07-08-processfunction--%E5%A4%9A%E6%B5%81%E8%BD%AC%E6%8D%A2%E6%8C%87%E5%8D%97)
  - [1 项目定位](#1-%E9%A1%B9%E7%9B%AE%E5%AE%9A%E4%BD%8D)
  - [2 环境与依赖](#2-%E7%8E%AF%E5%A2%83%E4%B8%8E%E4%BE%9D%E8%B5%96)
    - [2.1 运行环境](#21-%E8%BF%90%E8%A1%8C%E7%8E%AF%E5%A2%83)
    - [2.2 Maven 关键依赖](#22-maven-%E5%85%B3%E9%94%AE%E4%BE%9D%E8%B5%96)
  - [3 工程结构](#3-%E5%B7%A5%E7%A8%8B%E7%BB%93%E6%9E%84)
  - [4 案例拆解](#4-%E6%A1%88%E4%BE%8B%E6%8B%86%E8%A7%A3)
    - [4.1 ProcessFunction 基础用法（`ProcessFunctionTest`）](#41-processfunction-%E5%9F%BA%E7%A1%80%E7%94%A8%E6%B3%95processfunctiontest)
      - [4.1.1 代码核心](#411-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [4.1.2 实现要点](#412-%E5%AE%9E%E7%8E%B0%E8%A6%81%E7%82%B9)
      - [4.1.3 运行与观察](#413-%E8%BF%90%E8%A1%8C%E4%B8%8E%E8%A7%82%E5%AF%9F)
    - [4.2 处理时间定时器（`ProcessingTimeTimerTest`）](#42-%E5%A4%84%E7%90%86%E6%97%B6%E9%97%B4%E5%AE%9A%E6%97%B6%E5%99%A8processingtimetimertest)
      - [4.2.1 代码核心](#421-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [4.2.2 步骤拆解](#422-%E6%AD%A5%E9%AA%A4%E6%8B%86%E8%A7%A3)
      - [4.2.3 运行现象](#423-%E8%BF%90%E8%A1%8C%E7%8E%B0%E8%B1%A1)
    - [4.3 事件时间定时器（`EventTimeTimerTest`）](#43-%E4%BA%8B%E4%BB%B6%E6%97%B6%E9%97%B4%E5%AE%9A%E6%97%B6%E5%99%A8eventtimetimertest)
      - [4.3.1 代码核心](#431-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [4.3.2 关键知识点](#432-%E5%85%B3%E9%94%AE%E7%9F%A5%E8%AF%86%E7%82%B9)
      - [4.3.3 观察输出](#433-%E8%A7%82%E5%AF%9F%E8%BE%93%E5%87%BA)
    - [4.4 全局窗口 TopN（`ProcessAllWindowTopN`）](#44-%E5%85%A8%E5%B1%80%E7%AA%97%E5%8F%A3-topnprocessallwindowtopn)
      - [4.4.1 代码核心](#441-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [4.4.2 思路解析](#442-%E6%80%9D%E8%B7%AF%E8%A7%A3%E6%9E%90)
      - [4.4.3 使用建议](#443-%E4%BD%BF%E7%94%A8%E5%BB%BA%E8%AE%AE)
    - [4.5 Keyed 流 TopN（`KeyedProcessTopN`）](#45-keyed-%E6%B5%81-topnkeyedprocesstopn)
      - [4.5.1 代码核心](#451-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [4.5.2 流程解读](#452-%E6%B5%81%E7%A8%8B%E8%A7%A3%E8%AF%BB)
      - [4.5.3 调优建议](#453-%E8%B0%83%E4%BC%98%E5%BB%BA%E8%AE%AE)
  - [5 延伸学习](#5-%E5%BB%B6%E4%BC%B8%E5%AD%A6%E4%B9%A0)
  - [6 第8章 多流转换概览](#6-%E7%AC%AC8%E7%AB%A0-%E5%A4%9A%E6%B5%81%E8%BD%AC%E6%8D%A2%E6%A6%82%E8%A7%88)
    - [6.1 目标与语义](#61-%E7%9B%AE%E6%A0%87%E4%B8%8E%E8%AF%AD%E4%B9%89)
    - [6.2 案例清单](#62-%E6%A1%88%E4%BE%8B%E6%B8%85%E5%8D%95)
    - [6.3 Filter 拆流（`SplitStreamByFilter`）](#63-filter-%E6%8B%86%E6%B5%81splitstreambyfilter)
      - [6.3.1 代码核心](#631-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.3.2 要点速记](#632-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.4 OutputTag 侧输出拆流（`SplitStreamByOutputTag`）](#64-outputtag-%E4%BE%A7%E8%BE%93%E5%87%BA%E6%8B%86%E6%B5%81splitstreambyoutputtag)
      - [6.4.1 代码核心](#641-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.4.2 要点速记](#642-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.5 Connect 双流共享算子（`ConnectTest`）](#65-connect-%E5%8F%8C%E6%B5%81%E5%85%B1%E4%BA%AB%E7%AE%97%E5%AD%90connecttest)
      - [6.5.1 代码核心](#651-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.5.2 要点速记](#652-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.6 Union 合并同构流（`UnionTest`）](#66-union-%E5%90%88%E5%B9%B6%E5%90%8C%E6%9E%84%E6%B5%81uniontest)
      - [6.6.1 代码核心](#661-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.6.2 要点速记](#662-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.7 滚动窗口 Join（`WindowJoinTest`）](#67-%E6%BB%9A%E5%8A%A8%E7%AA%97%E5%8F%A3-joinwindowjointest)
      - [6.7.1 代码核心](#671-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.7.2 要点速记](#672-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.8 CoGroup 灵活匹配（`CoGroupTest`）](#68-cogroup-%E7%81%B5%E6%B4%BB%E5%8C%B9%E9%85%8Dcogrouptest)
      - [6.8.1 代码核心](#681-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.8.2 要点速记](#682-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.9 Interval Join（`IntervalJoinTest`）](#69-interval-joinintervaljointest)
      - [6.9.1 代码核心](#691-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.9.2 要点速记](#692-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
    - [6.10 双流实时对账（`BillCheckExample`）](#610-%E5%8F%8C%E6%B5%81%E5%AE%9E%E6%97%B6%E5%AF%B9%E8%B4%A6billcheckexample)
      - [6.10.1 代码核心](#6101-%E4%BB%A3%E7%A0%81%E6%A0%B8%E5%BF%83)
      - [6.10.2 要点速记](#6102-%E8%A6%81%E7%82%B9%E9%80%9F%E8%AE%B0)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink Chapter07-08 ProcessFunction & 多流转换指南

## 1 项目定位

`flink-00-flinktutorial` 模块用于串联整套 Flink 教程中的基础案例，Chapter07 聚焦 **ProcessFunction 与定时器**。本 README 汇总该章节的 5 个示例，方便快速理解不同语义下的处理函数能力与 TopN 业务写法。

## 2 环境与依赖

### 2.1 运行环境

- Flink 1.13.0（与 `pom.xml` 中 `${flink.version}` 保持一致）
- Java 8
- Scala 2.12（由于 demo 依赖 scala 版本后缀的 connector）

### 2.2 Maven 关键依赖

`flink-00-flinktutorial/pom.xml` 中保留了学习过程中常用的 connector，这里摘取 ProcessFunction 案例实际需要的核心部分：

```xml
<dependencies>
    <!-- Flink DataStream API -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
    </dependency>
    <!-- 客户端依赖，便于本地提交/调试 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients_${scala.binary.version}</artifactId>
    </dependency>
    <!-- Kafka、JDBC、Redis、Elasticsearch 等 connector 可按需启用 -->
    ...
</dependencies>
```

## 3 工程结构

```
flink-00-flinktutorial/
├── pom.xml
└── src/main/java/com/action/chapter07/
    ├── ProcessFunctionTest.java          # ProcessFunction 基础 API
    ├── ProcessingTimeTimerTest.java      # 处理时间定时器
    ├── EventTimeTimerTest.java           # 事件时间定时器
    ├── ProcessAllWindowTopN.java         # ProcessAllWindowFunction TopN
    └── KeyedProcessTopN.java             # KeyedProcessFunction TopN
```

## 4 案例拆解

### 4.1 ProcessFunction 基础用法（`ProcessFunctionTest`）

#### 4.1.1 代码核心

```java
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        // ClickSource 中生成的时间戳天然单调递增，这里直接复用单调水位线生成策略
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
                        // ProcessFunction 可以访问上下文(Context)，同时可自由决定数据输出条数
                        if (value.user.equals("Mary")) {
                            out.collect(value.user);
                        } else if (value.user.equals("Bob")) {
                            out.collect(value.user);
                            out.collect(value.user);
                        }
                        // 通过上下文可以方便地查看当前水位线
                        System.out.println(ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
```

#### 4.1.2 实现要点

1. **Context 能力**：`ProcessFunction` 的 `Context` 暴露当前时间戳、TimerService 等信息，可做精细化逻辑控制。
2. **多次输出**：一个输入事件可通过 `Collector` 发出多条结果，适合拆分、补发等场景。
3. **水位线观测**：示例里直接打印 `currentWatermark()`，帮助在调试阶段确认事件时间推进。

#### 4.1.3 运行与观察

```bash
cd flink-00-flinktutorial
mvn compile exec:java -Dexec.mainClass="com.action.chapter07.ProcessFunctionTest"
```

终端会持续打印 Mary/Bob 用户名与对应时刻的水位线，便于验证多次输出与上下文信息。

### 4.2 处理时间定时器（`ProcessingTimeTimerTest`）

#### 4.2.1 代码核心

```java
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 要用定时器，必须基于KeyedStream
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                        // 处理时间语义下注册的定时器与机器时间挂钩，不依赖水位线
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
```

#### 4.2.2 步骤拆解

1. **无需水位线**：处理时间以 TaskManager 的系统时钟为准，直接注册 `ProcessingTimeTimer`。
2. **KeyedStream 约束**：与事件时间一样，定时器只能在 `KeyedProcessFunction` 中使用，因此示例用 `keyBy(data -> true)` 将所有事件归到同一 Key。
3. **延迟触发**：示例注册 “当前处理时间 + 10 秒” 的定时器，验证回调机制。

#### 4.2.3 运行现象

- 控制台先打印“数据到达”日志，再在 10 秒后输出“定时器触发”。
- 即使源头有乱序或无时间戳，也不会影响触发时刻，因为完全依附机器时间。

### 4.3 事件时间定时器（`EventTimeTimerTest`）

#### 4.3.1 代码核心

```java
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

        // KeyedProcessFunction 的 timerService 只能作用在 KeyedStream 上，这里所有事件都归到 true 这一个 key 中
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark() + "\n -------分割线-------");
                        // 注册一个事件时间定时器：当水位线 >= 事件时间 + 10 秒时触发
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    // 自定义测试数据源：人为控制发送时间间隔，让水位线演进更加可观察
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿5秒钟
            Thread.sleep(5000L);

            // 发出10秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10秒+1ms后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {
        }
    }
}
```

#### 4.3.2 关键知识点

1. **水位线 + 定时器关系**：只有当水位线推进到 `事件时间 + 延迟` 时，事件时间定时器才会触发。
2. **自定义 Source 控水位**：示例通过三次发射数据 + 人为 `Thread.sleep`，清晰观察水位线阶梯式变化。
3. **KeyedStream 约束**：与处理时间一致，依旧需要 `keyBy` 后才能访问 TimerService。

#### 4.3.3 观察输出

- 每条输入都会打印“数据时间戳 + 当前水位线”。
- 当第二条/第三条数据推进水位线时，首次注册的定时器被触发，输出“定时器触发”日志，可直观验证 10 秒延迟。

### 4.4 全局窗口 TopN（`ProcessAllWindowTopN`）

#### 4.4.1 代码核心

```java
public class ProcessAllWindowTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        // 单调递增水位线策略：方便在本地演示滑动窗口触发顺序
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        SingleOutputStreamOperator<String> result = eventStream
                .map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.url;
                    }
                })
                // 不区分 key，直接在全局范围做滑动窗口 TopN
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
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

#### 4.4.2 思路解析

1. **ProcessAllWindowFunction**：与 Keyed 版本相比无需分组，直接在全局窗口中访问全部元素。
2. **HashMap 聚合**：窗口内先累加每个 URL 的访问次数，再转换为 `ArrayList<Tuple2<url,count>>` 以便排序。
3. **TopN 取值**：Demo 固定输出 Top2，同时打印窗口结束时间，便于核对滑动窗口（10s 大小 / 5s 步长）的覆盖范围。

#### 4.4.3 使用建议

- 适用于 **数据量较小或已经通过前置算子聚合** 的场景，避免单 Task 处理压力过大。
- 如果需要按 Key 做 TopN，请参考下一节的 `KeyedProcessTopN` 实现。

### 4.5 Keyed 流 TopN（`KeyedProcessTopN`）

#### 4.5.1 代码核心

```java
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

    // 自定义增量聚合；accumulator 直接记次数，节省窗口状态空间
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

    // 自定义全窗口函数：拿到聚合好的 count，再补充窗口起止信息
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
        // 定义一个列表状态，用于缓存同一个 windowEnd 下的所有 URL 统计结果
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
            // 注册 window end + 1ms后的定时器，等待该窗口的所有 count 数据到齐
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

            // 取前 n 名，构建输出结果
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

#### 4.5.2 流程解读

1. **增量聚合 + 全窗口**：`AggregateFunction` 负责局部计数，`ProcessWindowFunction` 仅补充窗口元数据，兼顾效率与可读性。
2. **按窗口结束时间二次 KeyBy**：将同一窗口的不同 URL 统计结果归并，再用 `KeyedProcessFunction` 排序。
3. **列表状态缓存**：`ListState` 暂存当前窗口的所有 `UrlViewCount`，定时器触发时统一排序输出，最后记得 `clear()` 释放。
4. **Watermark 对齐**：定时器注册在 `windowEnd + 1ms`，确保所有属于该窗口的事件时间都已到齐。

#### 4.5.3 调优建议

- TopN 数量通过构造函数传入，可视需求改为配置项。
- 如果某些窗口 URL 数量远大于 N，可考虑引入最小堆等数据结构降低排序成本。
- 列表状态默认保存在 TaskManager 内存/状态后端，生产环境需关注状态大小与 TTL。

## 5 延伸学习

1. **深挖 TimerService**：结合 `registerProcessingTimeTimer` 与 `registerEventTimeTimer` 处理混合语义需求。
2. **侧输出与告警**：在 `ProcessFunction` 中搭配 `OutputTag` 发送迟到数据或构造监控流。
3. **状态后端**：试着切换 RocksDB StateBackend，在高基数 TopN 场景下体验状态 spilling。
4. **CEP / Complex Event**：Chapter12 的 CEP 示例同样依赖底层 Process API，可结合本章知识进一步理解。

## 6 第8章 多流转换概览

### 6.1 目标与语义

Chapter08 强调如何在 DataStream API 中进行多流拆分、合并与 Join，对应以下常见语义：

- **多流拆分**：Filter 或侧输出 (`OutputTag`) 将不同用户/事件类型分到独立流。
- **多流合并**：`connect` 允许不同类型共享一个算子；`union` 合并同类型流并复用下游逻辑。
- **窗口/区间 Join**：`window join`、`coGroup`、`intervalJoin` 在时间维度上匹配两条流。
- **双流对账**：`CoProcessFunction` + 状态 + 定时器，实现跨流的补偿逻辑。

### 6.2 案例清单

```
src/main/java/com/action/chapter08/
├── SplitStreamByFilter.java         # Filter 拆流
├── SplitStreamByOutputTag.java      # OutputTag 侧输出拆流
├── ConnectTest.java                 # connect 双流共享算子
├── UnionTest.java                   # union 同类型流合并
├── WindowJoinTest.java              # tumbling window join
├── CoGroupTest.java                 # coGroup 灵活窗口 join
├── IntervalJoinTest.java            # interval join
└── BillCheckExample.java            # CoProcessFunction 实时对账
```

### 6.3 Filter 拆流（`SplitStreamByFilter`）

#### 6.3.1 代码核心

```java
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource());
        // 筛选 Mary 的浏览行为放入 MaryStream 流中
        DataStream<Event> MaryStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Mary");
            }
        });
        // 筛选 Bob 的浏览行为放入 BobStream 流中
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
                return !value.user.equals("Mary") && !value.user.equals("Bob") ;
            }
        });

        MaryStream.print("Mary pv");
        BobStream.print("Bob pv");
        elseStream.print("else pv");

        env.execute();

    }


}
```

#### 6.3.2 要点速记

1. **多次 Filter**：针对每个标签单独调用 `filter`，适用于拆分流数量不多且逻辑简单的情况。
2. **单独下游**：拆出的每条流可独立后续算子，互不影响。
3. **维护成本**：若后续需要拆更多分类，需新增 `filter`，可考虑改用 `OutputTag` 版。

### 6.4 OutputTag 侧输出拆流（`SplitStreamByOutputTag`）

#### 6.4.1 代码核心

```java
public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource());

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")){
                    // Mary 的事件走 MaryTag 侧输出
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")){
                    // Bob 的事件走 BobTag 侧输出
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    // 其他用户继续沿主流流转
                    out.collect(value);
                }
            }
        });

        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        processedStream.print("else");

        env.execute();
    }
}
```

#### 6.4.2 要点速记

1. **单算子集中拆分**：所有分流逻辑集中在 `ProcessFunction` 内，便于共享状态或共用复杂条件。
2. **Typed OutputTag**：侧输出流可以定义独立的泛型类型，示例中为 `Tuple3`，主流仍保持 `Event`。
3. **主流可继续处理**：非 Mary/Bob 的用户沿主流输出，可继续下游算子链。

### 6.5 Connect 双流共享算子（`ConnectTest`）

#### 6.5.1 代码核心

```java
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Long> stream2 = env.fromElements(1L,2L,3L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                // map1 只处理第一个流
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) {
                // map2 只处理第二个流
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }
}
```

#### 6.5.2 要点速记

1. **不同类型也能合流**：`connect` 不要求两条流类型一致，示例一个是 `Integer`，另一个是 `Long`。
2. **CoMapFunction 双口**：`map1`/`map2` 分别对应两个输入流，便于编写差异化逻辑。
3. **共享状态**：`connect` 后的算子可以共享状态（本例未演示），适合在 map1/map2 中复用。

### 6.6 Union 合并同构流（`UnionTest`）

#### 6.6.1 代码核心

```java
public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("hadoop102", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream1.print("stream1");

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop103", 7777)
                .map(data -> {
                    String[] field = data.split(",");
                    return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream2.print("stream2");

        // 合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        // 打印当前水位线，观察快慢水位线合并后的最小值推进
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();


        env.execute();
    }
}
```

#### 6.6.2 要点速记

1. **Union 限制**：只能合并同类型流，本例两条 Socket 流都是 `Event`。
2. **水位线取最小**：合流后的水位线以所有输入的最小值推进，示例通过 `ProcessFunction` 打印验证。
3. **便于共用下游**：合并后仅需维护一套后续算子，适合同构数据来源多的场景。

### 6.7 滚动窗口 Join（`WindowJoinTest`）

#### 6.7.1 代码核心

```java
public class WindowJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        DataStream<Tuple2<String, Long>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 3000L),
                        Tuple2.of("b", 3000L),
                        Tuple2.of("a", 4000L),
                        Tuple2.of("b", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        stream1
                .join(stream2)
                .where(r -> r.f0)
                .equalTo(r -> r.f0)
                // 5 秒滚动窗口，窗口内的相同 key 进行双流笛卡尔组合
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> left, Tuple2<String, Long> right) throws Exception {
                        return left + "=>" + right;
                    }
                })
                .print();

        env.execute();
    }
}
```

#### 6.7.2 要点速记

1. **严格窗口匹配**：只有落在同一个滚动窗口的事件才参与 join。
2. **一对多输出**：同窗口内同 key 的元素会被笛卡尔组合，输出多条记录。
3. **水位线驱动**：当窗口结束时间 ≤ 水位线时才会触发 join 计算。

### 6.8 CoGroup 灵活匹配（`CoGroupTest`）

#### 6.8.1 代码核心

```java
public class CoGroupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1000L),
                        Tuple2.of("b", 1000L),
                        Tuple2.of("a", 2000L),
                        Tuple2.of("b", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        DataStream<Tuple2<String, Long>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 3000L),
                        Tuple2.of("b", 3000L),
                        Tuple2.of("a", 4000L),
                        Tuple2.of("b", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                                return stringLongTuple2.f1;
                                            }
                                        }
                                )
                );

        stream1
                .coGroup(stream2)
                .where(r -> r.f0)
                .equalTo(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                        // CoGroup 中可以一次性拿到同一窗口内左右流的所有元素
                        collector.collect(iter1 + "=>" + iter2);
                    }
                })
                .print();

        env.execute();
    }
}
```

#### 6.8.2 要点速记

1. **可实现外连接**：在 `coGroup` 中可以检查 `iter1`/`iter2` 是否为空，实现左/右/全外连接逻辑。
2. **一次性获取 Iterable**：不同于 `join` 的逐条回调，更适合需要对集合整体做计算的场景。
3. **窗口一致**：同样依赖窗口 + 水位线，窗口定义与 Window Join 一致。

### 6.9 Interval Join（`IntervalJoinTest`）

#### 6.9.1 代码核心

```java
public class IntervalJoinTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.user))
                // between 下界是右流时间减去左流时间，示例表示右流最多早 5 秒、最晚晚 10 秒都算匹配
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        // ProcessJoinFunction 可访问匹配的左右元素以及 Join 边界信息 ctx
                        out.collect(right + " => " + left);
                    }
                })
                .print();

        env.execute();
    }}
```

#### 6.9.2 要点速记

1. **无窗口限制**：基于事件时间区间直接匹配，不需要显式窗口，适合存在 “订单+行为” 时间关联的场景。
2. **区间含义**：`between(lower, upper)` 表示右流事件时间 - 左流事件时间必须落在 `[lower, upper]`。
3. **水位线保障**：依旧依赖水位线，保证等待足够时间再触发 join，避免早触发造成遗漏。

### 6.10 双流实时对账（`BillCheckExample`）

#### 6.10.1 代码核心

```java
public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自 app 的支付日志（提前分配时间戳+水位线）
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

        // 来自第三方支付平台的支付日志
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
        appStream.connect(thirdpartStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    /**
     * 自定义 CoProcessFunction：
     * - processElement1：处理 app 事件。
     * - processElement2：处理第三方事件。
     * - onTimer：等待超时仍未匹配时输出告警。
     */
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );

            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 看另一条流中事件是否来过
            if (thirdPartyEventState.value() != null) {
                out.collect("对账成功：" + value + "  " + thirdPartyEventState.value());
                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null) {
                out.collect("对账成功：" + appEventState.value() + "  " + value);
                // 清空状态
                appEventState.clear();
            } else {
                // 更新状态
                thirdPartyEventState.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + "  " + "第三方支付平台信息未到");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + "  " + "app信息未到");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
```

#### 6.10.2 要点速记

1. **CoProcessFunction 双输入**：`processElement1/2` 分别对应两条流，结合 `keyBy` 可定位同一订单。
2. **ValueState 缓存**：当另一条流未到时先写入状态，等待定时器触发或另一条流补齐。
3. **超时告警**：注册 `事件时间 + 5s` 的定时器，一旦等待超时仍未匹配就输出失败信息。

## 7. 参考资料

- [Flink 官方文档 - ProcessFunction](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/process_function/)
- [Flink 官方文档 - 多流转换](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/overview/#多流转换)
- [Flink 官方文档 - 窗口 Join](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/joining/#窗口-join)
- [Flink 官方文档 - 区间 Join](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/operators/joining/#区间-joininterval-join)

