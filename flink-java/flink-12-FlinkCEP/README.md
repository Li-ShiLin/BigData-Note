<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink CEP 复杂事件处理演示](#flink-cep-%E5%A4%8D%E6%9D%82%E4%BA%8B%E4%BB%B6%E5%A4%84%E7%90%86%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 LoginEvent 类](#321-loginevent-%E7%B1%BB)
      - [3.2.2 OrderEvent 类](#322-orderevent-%E7%B1%BB)
  - [4. CEP 示例代码](#4-cep-%E7%A4%BA%E4%BE%8B%E4%BB%A3%E7%A0%81)
    - [4.1 登录失败检测示例（基础版）](#41-%E7%99%BB%E5%BD%95%E5%A4%B1%E8%B4%A5%E6%A3%80%E6%B5%8B%E7%A4%BA%E4%BE%8B%E5%9F%BA%E7%A1%80%E7%89%88)
    - [4.2 登录失败检测示例（改进版）](#42-%E7%99%BB%E5%BD%95%E5%A4%B1%E8%B4%A5%E6%A3%80%E6%B5%8B%E7%A4%BA%E4%BE%8B%E6%94%B9%E8%BF%9B%E7%89%88)
    - [4.3 订单超时检测示例](#43-%E8%AE%A2%E5%8D%95%E8%B6%85%E6%97%B6%E6%A3%80%E6%B5%8B%E7%A4%BA%E4%BE%8B)
    - [4.4 NFA 状态机示例](#44-nfa-%E7%8A%B6%E6%80%81%E6%9C%BA%E7%A4%BA%E4%BE%8B)
  - [5. 总结](#5-%E6%80%BB%E7%BB%93)
    - [5.1 Flink CEP 的特点](#51-flink-cep-%E7%9A%84%E7%89%B9%E7%82%B9)
    - [5.2 Pattern API 总结](#52-pattern-api-%E6%80%BB%E7%BB%93)
    - [5.3 处理函数总结](#53-%E5%A4%84%E7%90%86%E5%87%BD%E6%95%B0%E6%80%BB%E7%BB%93)
    - [5.4 应用场景](#54-%E5%BA%94%E7%94%A8%E5%9C%BA%E6%99%AF)
  - [6. Pattern API 详解](#6-pattern-api-%E8%AF%A6%E8%A7%A3)
    - [6.1 个体模式](#61-%E4%B8%AA%E4%BD%93%E6%A8%A1%E5%BC%8F)
    - [6.2 组合模式](#62-%E7%BB%84%E5%90%88%E6%A8%A1%E5%BC%8F)
    - [6.3 时间限制](#63-%E6%97%B6%E9%97%B4%E9%99%90%E5%88%B6)
    - [6.4 循环模式中的近邻条件](#64-%E5%BE%AA%E7%8E%AF%E6%A8%A1%E5%BC%8F%E4%B8%AD%E7%9A%84%E8%BF%91%E9%82%BB%E6%9D%A1%E4%BB%B6)
  - [7. 参考资料](#7-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink CEP 复杂事件处理演示

## 1. 项目作用

本项目演示了 Flink CEP（Complex Event Processing，复杂事件处理）的相关知识点，包括：

- CEP 的基本概念和应用场景
- Pattern API 的使用方法
- 个体模式和组合模式的定义
- 模式匹配和事件处理
- 超时检测和处理
- NFA 状态机实现

通过实际代码示例帮助开发者快速掌握 Flink CEP 的创建和使用方法。

## 2. 项目结构

```
flink-12-FlinkCEP/
├── src/main/java/com/action/
│   ├── LoginEvent.java                                    # 登录事件数据模型
│   ├── OrderEvent.java                                    # 订单事件数据模型
│   └── cep/
│       ├── LoginFailDetectExample.java                    # 登录失败检测示例（基础版）
│       ├── LoginFailDetectProExample.java                 # 登录失败检测示例（改进版）
│       ├── OrderTimeoutDetectExample.java                 # 订单超时检测示例
│       └── NFAExample.java                                # NFA 状态机示例
├── pom.xml                                                # Maven 配置
└── README.md                                              # 本文档
```

## 3. 核心实现

### 3.1 依赖配置

`pom.xml`：引入 Flink 核心依赖和 CEP 相关依赖

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

    <!-- ==================== Flink CEP 依赖 ==================== -->
    <!-- flink-cep: Flink复杂事件处理库，用于模式匹配和复杂事件检测 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-cep</artifactId>
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

#### 3.2.1 LoginEvent 类

`LoginEvent.java`：定义登录事件数据模型，包含用户ID、IP地址、事件类型和时间戳字段

```java
package com.action;

/**
 * 登录事件数据模型
 * 用于演示 Flink CEP 复杂事件处理相关功能
 */
public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
```

#### 3.2.2 OrderEvent 类

`OrderEvent.java`：定义订单事件数据模型，包含用户ID、订单ID、事件类型和时间戳字段

```java
package com.action;

/**
 * 订单事件数据模型
 * 用于演示 Flink CEP 复杂事件处理相关功能
 */
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
```

## 4. CEP 示例代码

### 4.1 登录失败检测示例（基础版）

文件路径：`src/main/java/com/action/cep/LoginFailDetectExample.java`

功能说明：演示如何使用 Flink CEP 检测连续三次登录失败事件。这是最基础的实现方式，使用三个单例模式通过 `next()` 连接，表示严格近邻关系。

核心概念：

- **Pattern API**：Flink CEP 的核心 API，用于定义复杂事件匹配规则。
- **个体模式（Individual Pattern）**：每个简单事件的匹配规则，通过 `.where()` 方法定义过滤条件。
- **组合模式（Combining Pattern）**：将多个个体模式组合起来，形成完整的匹配规则。
- **严格近邻（Strict Contiguity）**：使用 `.next()` 表示两个事件必须严格连续，中间不能有其他事件。
- **PatternSelectFunction**：用于从匹配的复杂事件中提取信息并转换为输出结果。

代码实现：

```java
package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 登录失败检测示例（基础版）
 * 演示如何使用 Flink CEP 检测连续三次登录失败事件
 *
 * 功能说明：
 * 1. 定义 Pattern，检测连续三个登录失败事件
 * 2. 将 Pattern 应用到流上，检测匹配的复杂事件
 * 3. 提取匹配的复杂事件，包装成报警信息输出
 */
public class LoginFailDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取登录事件流，并提取时间戳、生成水位线 ====================
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LoginEvent>() {
                                            @Override
                                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                                return loginEvent.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.userId); // 按照用户ID进行分组

        // ==================== 2. 定义 Pattern，连续的三个登录失败事件 ====================
        // 使用 begin() 定义初始模式，next() 表示严格近邻关系（中间不能有其他事件）
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first")    // 以第一个登录失败事件开始
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")    // 接着是第二个登录失败事件（严格近邻）
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")     // 接着是第三个登录失败事件（严格近邻）
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                });

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出 ====================
        patternStream
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        // 从 Map 中提取匹配的事件，key 是模式名称，value 是匹配的事件列表
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("second").get(0);
                        LoginEvent third = map.get("third").get(0);
                        return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp
                                + ", " + second.timestamp + ", " + third.timestamp;
                    }
                })
                .print("warning");

        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `LoginFailDetectExample` 类的 `main` 方法。

**（2）观察输出**

输出结果如下：

```
warning> user_1 连续三次登录失败！登录时间：2000, 3000, 5000
```

可以看到，user_1 连续三次登录失败被检测到了；而 user_2 尽管也有三次登录失败，但中间有一次登录成功，所以不会被匹配到。

**（3）代码逻辑**

- 使用 `begin()` 定义初始模式，每个模式都有一个名称（"first"、"second"、"third"）
- 使用 `next()` 表示严格近邻关系，确保三个登录失败事件是连续发生的
- 使用 `where()` 方法定义过滤条件，只接受 `eventType` 为 "fail" 的事件
- 使用 `PatternSelectFunction` 从匹配结果中提取信息并转换为输出

注意事项：

1. **严格近邻**：`next()` 表示严格近邻关系，中间不能有其他事件，所以 user_2 的匹配被登录成功事件打断
2. **按键分组**：使用 `keyBy()` 按照用户ID分组，确保每个用户的匹配是独立的
3. **事件时间**：需要为流分配时间戳和生成水位线，以支持事件时间语义

### 4.2 登录失败检测示例（改进版）

文件路径：`src/main/java/com/action/cep/LoginFailDetectProExample.java`

功能说明：演示如何使用循环模式和 `PatternProcessFunction` 检测连续三次登录失败事件。这种方式比基础版更加简洁，使用 `times(3).consecutive()` 定义循环模式，易于扩展到更多次数的检测。

核心概念：

- **循环模式（Looping Pattern）**：通过量词（如 `times()`、`oneOrMore()`）定义的可以匹配多个事件的模式。
- **量词（Quantifiers）**：用于指定模式匹配的次数，如 `times(3)` 表示匹配3次。
- **consecutive()**：为循环模式增加严格近邻条件，保证所有匹配事件是严格连续的。
- **PatternProcessFunction**：官方推荐的处理方式，功能更加强大和灵活，可以访问上下文信息。

代码实现：

```java
package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 登录失败检测示例（改进版）
 * 演示如何使用循环模式和 PatternProcessFunction 检测连续三次登录失败事件
 *
 * 功能说明：
 * 1. 使用 times(3).consecutive() 定义循环模式，检测连续三个登录失败事件
 * 2. 使用 PatternProcessFunction 处理匹配的复杂事件
 * 3. 这种方式比基础版更加简洁，易于扩展到更多次数的检测
 */
public class LoginFailDetectProExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取登录事件流，并提取时间戳、生成水位线 ====================
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LoginEvent>() {
                                            @Override
                                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                                return loginEvent.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.userId); // 按照用户ID进行分组

        // ==================== 2. 定义 Pattern，使用循环模式检测连续三个登录失败事件 ====================
        // times(3) 表示匹配3次，consecutive() 表示严格连续（中间不能有其他事件）
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail")    // 第一个登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .times(3).consecutive();    // 指定是严格紧邻的三次登录失败

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 使用 PatternProcessFunction 处理匹配的复杂事件 ====================
        // PatternProcessFunction 是官方推荐的处理方式，功能更加强大和灵活
        SingleOutputStreamOperator<String> warningStream = patternStream
                .process(new PatternProcessFunction<LoginEvent, String>() {
                    @Override
                    public void processMatch(Map<String, List<LoginEvent>> match, Context ctx, Collector<String> out) throws Exception {
                        // 提取三次登录失败事件
                        // 由于使用了循环模式，所有匹配的事件都在同一个 List 中
                        LoginEvent firstFailEvent = match.get("fail").get(0);
                        LoginEvent secondFailEvent = match.get("fail").get(1);
                        LoginEvent thirdFailEvent = match.get("fail").get(2);

                        out.collect(firstFailEvent.userId + " 连续三次登录失败！登录时间：" +
                                firstFailEvent.timestamp + ", " +
                                secondFailEvent.timestamp + ", " +
                                thirdFailEvent.timestamp);
                    }
                });

        // ==================== 5. 打印输出 ====================
        warningStream.print("warning");

        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `LoginFailDetectProExample` 类的 `main` 方法。

**（2）观察输出**

输出结果与基础版完全一样：

```
warning> user_1 连续三次登录失败！登录时间：2000, 3000, 5000
```

**（3）代码逻辑**

- 使用 `times(3).consecutive()` 定义循环模式，比基础版更加简洁
- 使用 `PatternProcessFunction` 处理匹配事件，可以访问上下文信息
- 循环模式匹配的所有事件都在同一个 List 中，通过索引访问

注意事项：

1. **循环模式**：`times(3)` 表示匹配3次，`consecutive()` 确保是严格连续的
2. **事件提取**：循环模式匹配的所有事件都在同一个 List 中，通过 `get(0)`、`get(1)`、`get(2)` 访问
3. **扩展性**：如果需要检测连续100次登录失败，只需要将 `times(3)` 改为 `times(100)` 即可

### 4.3 订单超时检测示例

文件路径：`src/main/java/com/action/cep/OrderTimeoutDetectExample.java`

功能说明：演示如何使用 Flink CEP 检测订单创建后15分钟内未支付的情况。这是一个典型的超时检测场景，需要使用 `within()` 方法设置时间限制，并通过 `TimedOutPartialMatchHandler` 处理超时的部分匹配。

核心概念：

- **宽松近邻（Relaxed Contiguity）**：使用 `followedBy()` 表示两个事件之间可以有其他事件，只关心事件发生的顺序。
- **时间限制**：使用 `within()` 方法设置模式序列中第一个事件到最后一个事件之间的最大时间间隔。
- **超时部分匹配**：当模式序列在规定时间内没有完全匹配时，会产生超时的部分匹配事件。
- **TimedOutPartialMatchHandler**：用于处理超时的部分匹配事件的接口，必须与 `PatternProcessFunction` 结合使用。
- **侧输出流**：用于输出超时事件，通过 `OutputTag` 标识。

代码实现：

```java
package com.action.cep;

import com.action.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单超时检测示例
 * 演示如何使用 Flink CEP 检测订单创建后15分钟内未支付的情况
 *
 * 功能说明：
 * 1. 定义 Pattern：订单创建事件后，在15分钟内应该有支付事件
 * 2. 使用 within() 方法设置时间限制
 * 3. 使用 TimedOutPartialMatchHandler 处理超时的部分匹配
 * 4. 通过侧输出流输出超时订单信息
 */
public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取订单事件流，并提取时间戳、生成水位线 ====================
        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(order -> order.orderId);    // 按照订单ID分组

        // ==================== 2. 定义 Pattern ====================
        // 首先是下单事件，之后是支付事件；中间可以修改订单，使用宽松近邻
        // within() 限制在15分钟之内完成支付
        Pattern<OrderEvent, ?> pattern = Pattern
                .<OrderEvent>begin("create")    // 首先是下单事件
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")    // 之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));    // 限制在15分钟之内

        // ==================== 3. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream ====================
        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);

        // ==================== 4. 定义一个侧输出流标签，用于标识超时侧输出流 ====================
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        // ==================== 5. 处理匹配的复杂事件 ====================
        // 使用 PatternProcessFunction 并实现 TimedOutPartialMatchHandler 接口
        // 这样可以同时处理正常匹配和超时部分匹配的情况
        SingleOutputStreamOperator<String> payedOrderStream = patternStream
                .process(new OrderPayPatternProcessFunction());

        // ==================== 6. 将正常匹配和超时部分匹配的处理结果流打印输出 ====================
        payedOrderStream.print("payed");
        payedOrderStream.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    /**
     * 实现自定义的 PatternProcessFunction，需实现 TimedOutPartialMatchHandler 接口
     * 用于处理正常匹配和超时部分匹配的情况
     */
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String>
            implements TimedOutPartialMatchHandler<OrderEvent> {

        // ==================== 处理正常匹配事件 ====================
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            // 提取支付事件
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("订单 " + payEvent.orderId + " 已支付！");
        }

        // ==================== 处理超时未支付事件 ====================
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            // 提取创建事件
            OrderEvent createEvent = match.get("create").get(0);
            // 通过侧输出流输出超时信息
            ctx.output(new OutputTag<String>("timeout") {
            }, "订单 " + createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId);
        }
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `OrderTimeoutDetectExample` 类的 `main` 方法。

**（2）观察输出**

输出结果如下：

```
payed> 订单 order_1 已支付！
payed> 订单 order_3 已支付！
timeout> 订单 order_2 超时未支付！用户为：user_2
```

可以看到，订单 order_1 和 order_3 都在15分钟内完成了支付，而订单 order_2 未能支付，因此侧输出流输出了一条报警信息。

**（3）代码逻辑**

- 使用 `followedBy()` 表示宽松近邻关系，中间可以有其他事件（如修改订单）
- 使用 `within(Time.minutes(15))` 设置时间限制，必须在15分钟内完成支付
- 正常匹配的事件通过主流输出，超时的部分匹配通过侧输出流输出
- 实现 `TimedOutPartialMatchHandler` 接口来处理超时事件

注意事项：

1. **时间限制**：`within()` 方法设置的时间限制是模式序列中第一个事件到最后一个事件之间的最大时间间隔
2. **超时处理**：超时的部分匹配事件只能获取到已经匹配的事件，未匹配的事件可能为 null
3. **侧输出流**：超时事件通过侧输出流输出，需要定义 `OutputTag` 来标识
4. **按键分组**：按照订单ID分组，确保每个订单的匹配是独立的

### 4.4 NFA 状态机示例

文件路径：`src/main/java/com/action/cep/NFAExample.java`

功能说明：演示如何使用状态机（NFA）的方式实现连续三次登录失败检测。这是一个不使用 CEP 库的替代实现方式，展示了 CEP 底层的工作原理。

核心概念：

- **状态机（State Machine）**：通过状态和状态转移来跟踪匹配进度的机制。
- **状态转移**：根据输入事件和当前状态，决定下一个状态的规则。
- **NFA（非确定有限状态自动机）**：Flink CEP 底层使用的状态机实现。
- **ValueState**：用于保存当前用户对应的状态。

代码实现：

```java
package com.action.cep;

import com.action.LoginEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * NFA 状态机示例
 * 演示如何使用状态机（NFA）的方式实现连续三次登录失败检测
 *
 * 功能说明：
 * 1. 使用状态机的方式实现复杂事件检测，不依赖 CEP 库
 * 2. 通过状态转移来跟踪匹配进度
 * 3. 当检测到匹配的复杂事件时，输出报警信息
 *
 * 注意：这是一个不使用 CEP 库的替代实现方式，展示了 CEP 底层的工作原理
 */
public class NFAExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 获取登录事件流，这里与时间无关，就不生成水位线了 ====================
        KeyedStream<LoginEvent, String> stream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .keyBy(r -> r.userId); // 按照用户ID分组

        // ==================== 2. 将数据依次输入状态机进行处理 ====================
        DataStream<String> alertStream = stream
                .flatMap(new StateMachineMapper());

        alertStream.print("warning");

        env.execute();
    }

    /**
     * 状态机映射器
     * 使用状态机的方式检测连续三次登录失败
     */
    @SuppressWarnings("serial")
    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {

        // ==================== 声明当前用户对应的状态 ====================
        private ValueState<State> currentState;

        @Override
        public void open(Configuration conf) {
            // ==================== 获取状态对象 ====================
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent event, Collector<String> out) throws Exception {
            // ==================== 获取状态，如果状态为空，置为初始状态 ====================
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // ==================== 基于当前状态，输入当前事件时跳转到下一状态 ====================
            State nextState = state.transition(event.eventType);

            if (nextState == State.Matched) {
                // ==================== 如果检测到匹配的复杂事件，输出报警信息 ====================
                out.collect(event.userId + " 连续三次登录失败");
                // 需要跳转回S2状态，这里直接不更新状态就可以了
            } else if (nextState == State.Terminal) {
                // ==================== 如果到了终止状态，就重置状态，准备重新开始 ====================
                currentState.update(State.Initial);
            } else {
                // ==================== 如果还没结束，更新状态（状态跳转），继续读取事件 ====================
                currentState.update(nextState);
            }
        }
    }

    /**
     * 状态机实现
     * 定义状态和状态转移规则
     */
    public enum State {

        Terminal,    // 匹配失败，当前匹配终止

        Matched,    // 匹配成功

        // S2状态：已经匹配了两次登录失败，再匹配一次失败就成功，匹配成功就终止
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),

        // S1状态：已经匹配了一次登录失败，再匹配一次失败进入S2，匹配成功就终止
        S1(new Transition("fail", S2), new Transition("success", Terminal)),

        // 初始状态：匹配失败进入S1，匹配成功就终止
        Initial(new Transition("fail", S1), new Transition("success", Terminal));

        private final Transition[] transitions;    // 状态转移规则

        // ==================== 状态的构造方法，可以传入一组状态转移规则来定义状态 ====================
        State(Transition... transitions) {
            this.transitions = transitions;
        }

        // ==================== 状态的转移方法，根据当前输入事件类型，从定义好的转移规则中找到下一个状态 ====================
        public State transition(String eventType) {
            for (Transition t : transitions) {
                if (t.getEventType().equals(eventType)) {
                    return t.getTargetState();
                }
            }

            // ==================== 如果没有找到转移规则，说明已经结束，回到初始状态 ====================
            return Initial;
        }
    }

    /**
     * 定义状态转移类，包括两个属性：当前事件类型和目标状态
     */
    public static class Transition implements Serializable {
        private static final long serialVersionUID = 1L;

        // ==================== 触发状态转移的当前事件类型 ====================
        private final String eventType;

        // ==================== 转移的目标状态 ====================
        private final State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = checkNotNull(eventType);
            this.targetState = checkNotNull(targetState);
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `NFAExample` 类的 `main` 方法。

**（2）观察输出**

输出结果与 CEP 实现完全一样：

```
warning> user_1 连续三次登录失败
```

**（3）代码逻辑**

- 使用枚举类型 `State` 定义状态机的状态和状态转移规则
- 使用 `ValueState` 保存每个用户的当前状态
- 根据输入事件和当前状态，通过 `transition()` 方法决定下一个状态
- 当状态转移到 `Matched` 时，输出报警信息
- 当状态转移到 `Terminal` 时，重置状态到 `Initial`

注意事项：

1. **状态机原理**：Flink CEP 底层使用 NFA（非确定有限状态自动机）实现，本示例展示了其工作原理
2. **状态转移**：每个状态都定义了状态转移规则，根据输入事件类型决定下一个状态
3. **状态重置**：匹配成功或失败后需要重置状态，准备下一轮检测
4. **实现复杂度**：使用状态机实现复杂事件检测比较繁琐，而 CEP 库将这些逻辑封装起来，大大简化了开发

## 5. 总结

### 5.1 Flink CEP 的特点

- **声明式编程**：使用 Pattern API 进行声明式编程，代码简洁易读
- **功能强大**：支持复杂的事件模式匹配，包括严格近邻、宽松近邻、时间限制等
- **易于扩展**：可以方便地扩展到更复杂的匹配场景
- **性能优化**：底层使用 NFA 状态机实现，性能高效

### 5.2 Pattern API 总结

| 方法 | 说明 | 示例 |
|------|------|------|
| `begin()` | 定义初始模式 | `Pattern.begin("start")` |
| `next()` | 严格近邻，中间不能有其他事件 | `pattern.next("middle")` |
| `followedBy()` | 宽松近邻，中间可以有其他事件 | `pattern.followedBy("end")` |
| `followedByAny()` | 非确定性宽松近邻 | `pattern.followedByAny("end")` |
| `where()` | 定义过滤条件 | `pattern.where(condition)` |
| `times()` | 指定匹配次数 | `pattern.times(3)` |
| `oneOrMore()` | 匹配一次或多次 | `pattern.oneOrMore()` |
| `consecutive()` | 严格连续匹配 | `pattern.times(3).consecutive()` |
| `within()` | 设置时间限制 | `pattern.within(Time.minutes(15))` |

### 5.3 处理函数总结

| 接口 | 说明 | 使用场景 |
|------|------|---------|
| `PatternSelectFunction` | 简单选择提取 | 简单的匹配事件提取 |
| `PatternFlatSelectFunction` | 扁平化选择提取 | 需要多次输出的场景 |
| `PatternProcessFunction` | 通用处理函数 | 官方推荐，功能最强大 |
| `TimedOutPartialMatchHandler` | 超时处理接口 | 需要处理超时事件的场景 |

### 5.4 应用场景

- **风险控制**：检测用户的异常行为模式，如连续登录失败、刷单等
- **用户画像**：实时跟踪用户行为轨迹，检测特定行为习惯
- **运维监控**：灵活配置多指标、多依赖的监控模式
- **订单监控**：检测订单超时、支付异常等情况

## 6. Pattern API 详解

### 6.1 个体模式

个体模式（Individual Pattern）是组成复杂匹配规则的基本单元。每个个体模式都需要：

- **连接词**：`begin()`、`next()`、`followedBy()` 等，用于定义模式的开始和模式之间的关系
- **过滤条件**：通过 `.where()` 方法定义，使用 `SimpleCondition` 或 `IterativeCondition`
- **量词**：可选，用于指定模式匹配的次数，如 `times()`、`oneOrMore()`、`optional()`

### 6.2 组合模式

组合模式（Combining Pattern）是将多个个体模式组合起来形成的完整匹配规则。模式之间的关系包括：

- **严格近邻（Strict Contiguity）**：使用 `next()`，两个事件必须严格连续
- **宽松近邻（Relaxed Contiguity）**：使用 `followedBy()`，两个事件之间可以有其他事件
- **非确定性宽松近邻**：使用 `followedByAny()`，可以重复使用已匹配的事件

### 6.3 时间限制

使用 `within()` 方法可以为模式序列设置时间限制，只有在这期间成功匹配的复杂事件才是有效的。

### 6.4 循环模式中的近邻条件

循环模式默认使用宽松近邻关系。如果需要严格连续，可以使用 `consecutive()` 方法。

## 7. 参考资料

- Flink 官方文档 - CEP：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/libs/cep/
- Flink 官方文档 - Pattern API：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/libs/cep/#the-pattern-api

