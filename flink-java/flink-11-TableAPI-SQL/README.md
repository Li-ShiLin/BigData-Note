<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink Table API 和 SQL 演示](#flink-table-api-%E5%92%8C-sql-%E6%BC%94%E7%A4%BA)
  - [1. 项目作用](#1-%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [2. 项目结构](#2-%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [3. 核心实现](#3-%E6%A0%B8%E5%BF%83%E5%AE%9E%E7%8E%B0)
    - [3.1 依赖配置](#31-%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [3.2 数据模型](#32-%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B)
      - [3.2.1 Event 类](#321-event-%E7%B1%BB)
      - [3.2.2 ClickSource 类](#322-clicksource-%E7%B1%BB)
  - [4. Table API 和 SQL 示例](#4-table-api-%E5%92%8C-sql-%E7%A4%BA%E4%BE%8B)
    - [4.1 Table API 和 SQL 快速上手示例](#41-table-api-%E5%92%8C-sql-%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B%E7%A4%BA%E4%BE%8B)
    - [4.2 Table API 方式查询示例](#42-table-api-%E6%96%B9%E5%BC%8F%E6%9F%A5%E8%AF%A2%E7%A4%BA%E4%BE%8B)
    - [4.3 表和流转换示例](#43-%E8%A1%A8%E5%92%8C%E6%B5%81%E8%BD%AC%E6%8D%A2%E7%A4%BA%E4%BE%8B)
  - [5. 时间属性和窗口](#5-%E6%97%B6%E9%97%B4%E5%B1%9E%E6%80%A7%E5%92%8C%E7%AA%97%E5%8F%A3)
    - [5.1 追加查询示例：窗口聚合](#51-%E8%BF%BD%E5%8A%A0%E6%9F%A5%E8%AF%A2%E7%A4%BA%E4%BE%8B%E7%AA%97%E5%8F%A3%E8%81%9A%E5%90%88)
    - [5.2 累积窗口示例](#52-%E7%B4%AF%E7%A7%AF%E7%AA%97%E5%8F%A3%E7%A4%BA%E4%BE%8B)
    - [5.3 滑动窗口示例](#53-%E6%BB%91%E5%8A%A8%E7%AA%97%E5%8F%A3%E7%A4%BA%E4%BE%8B)
    - [5.4 开窗聚合示例](#54-%E5%BC%80%E7%AA%97%E8%81%9A%E5%90%88%E7%A4%BA%E4%BE%8B)
  - [6. 聚合查询](#6-%E8%81%9A%E5%90%88%E6%9F%A5%E8%AF%A2)
    - [6.1 分组聚合示例](#61-%E5%88%86%E7%BB%84%E8%81%9A%E5%90%88%E7%A4%BA%E4%BE%8B)
    - [6.2 Top N 示例](#62-top-n-%E7%A4%BA%E4%BE%8B)
    - [6.3 窗口 Top N 示例](#63-%E7%AA%97%E5%8F%A3-top-n-%E7%A4%BA%E4%BE%8B)
  - [7. 联结查询](#7-%E8%81%94%E7%BB%93%E6%9F%A5%E8%AF%A2)
    - [7.1 常规联结查询示例](#71-%E5%B8%B8%E8%A7%84%E8%81%94%E7%BB%93%E6%9F%A5%E8%AF%A2%E7%A4%BA%E4%BE%8B)
    - [7.2 间隔联结查询示例](#72-%E9%97%B4%E9%9A%94%E8%81%94%E7%BB%93%E6%9F%A5%E8%AF%A2%E7%A4%BA%E4%BE%8B)
  - [8. 自定义函数（UDF）](#8-%E8%87%AA%E5%AE%9A%E4%B9%89%E5%87%BD%E6%95%B0udf)
    - [8.1 标量函数示例](#81-%E6%A0%87%E9%87%8F%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
    - [8.2 表聚合函数示例](#82-%E8%A1%A8%E8%81%9A%E5%90%88%E5%87%BD%E6%95%B0%E7%A4%BA%E4%BE%8B)
  - [9. 总结](#9-%E6%80%BB%E7%BB%93)
    - [9.1 Table API 和 SQL 的特点](#91-table-api-%E5%92%8C-sql-%E7%9A%84%E7%89%B9%E7%82%B9)
    - [9.2 查询类型总结](#92-%E6%9F%A5%E8%AF%A2%E7%B1%BB%E5%9E%8B%E6%80%BB%E7%BB%93)
    - [9.3 窗口类型总结](#93-%E7%AA%97%E5%8F%A3%E7%B1%BB%E5%9E%8B%E6%80%BB%E7%BB%93)
  - [10. 参考资料](#10-%E5%8F%82%E8%80%83%E8%B5%84%E6%96%99)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Flink Table API 和 SQL 演示

## 1. 项目作用

本项目演示了 Flink 中 Table API 和 SQL 的相关知识点，包括：

- Table API 和 SQL 的基本使用
- 表和流的转换
- 时间属性和窗口
- 聚合查询（分组聚合、窗口聚合、开窗聚合）
- Top N 查询
- 联结查询
- 自定义函数（UDF：标量函数、表聚合函数）

通过实际代码示例帮助开发者快速掌握 Flink Table API 和 SQL 的创建和使用方法。

## 2. 项目结构

```
flink-11-TableAPI-SQL/
├── src/main/java/com/action/
│   ├── Event.java                                       # 事件数据模型
│   ├── ClickSource.java                                 # 自定义数据源
│   └── tableapi/
│       ├── TableExample.java                            # Table API 和 SQL 快速上手示例
│       ├── TableAPIExample.java                         # Table API 方式查询示例
│       ├── TableToStreamExample.java                    # 表和流转换示例
│       ├── AppendQueryExample.java                      # 追加查询示例（窗口聚合）
│       ├── CumulateWindowExample.java                   # 累积窗口示例
│       ├── HopWindowExample.java                        # 滑动窗口示例
│       ├── OverWindowExample.java                       # 开窗聚合示例
│       ├── RowIntervalOverWindowExample.java            # 行间隔开窗聚合示例
│       ├── GroupAggregationExample.java                 # 分组聚合示例
│       ├── TopNExample.java                             # Top N 示例
│       ├── WindowTopNExample.java                       # 窗口 Top N 示例
│       ├── RegularJoinExample.java                      # 常规联结查询示例
│       ├── IntervalJoinExample.java                    # 间隔联结查询示例
│       ├── UdfScalarFunctionExample.java                # 自定义标量函数示例
│       └── UdfTableAggregateFunctionExample.java        # 自定义表聚合函数示例
├── pom.xml                                              # Maven 配置
└── README.md                                            # 本文档
```

## 3. 核心实现

### 3.1 依赖配置

`pom.xml`：引入 Flink 核心依赖和 Table API 相关依赖

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

    <!-- ==================== Flink Table API 和 SQL 依赖 ==================== -->
    <!-- flink-table-api-java-bridge: Table API 和 DataStream API 的桥接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
    </dependency>

    <!-- flink-table-planner-blink: Table API 计划器，提供运行时环境 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner-blink_${scala.binary.version}</artifactId>
    </dependency>

    <!-- flink-streaming-scala: Scala 流处理支持（Table API 内部实现需要） -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
    </dependency>

    <!-- flink-csv: CSV 格式支持 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-csv</artifactId>
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
 * 用于演示 Flink Table API 和 SQL 相关功能
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
 * 用于演示 Flink Table API 和 SQL 相关功能
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

## 4. Table API 和 SQL 示例

### 4.1 Table API 和 SQL 快速上手示例

文件路径：`src/main/java/com/action/tableapi/TableExample.java`

功能说明：演示如何使用 Table API 和 SQL 进行数据处理。这是最基础的示例，展示了从数据流到表、执行 SQL 查询、再转换回数据流的完整流程。

核心概念：

- **表环境（TableEnvironment）**：Table API 和 SQL 的运行时环境，负责注册表、执行 SQL 查询、注册 UDF 等。
- **表（Table）**：Table API 中的核心接口，代表一个关系型表。
- **虚拟表（Virtual Table）**：在表环境中注册的表，可以在 SQL 中直接引用。
- **SQL 查询**：使用 `sqlQuery()` 方法执行 SQL 语句，返回一个 Table 对象。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 和 SQL 快速上手示例
 * 演示如何使用 Table API 和 SQL 进行数据处理
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 将数据流转换成表
 * 3. 使用 SQL 查询提取 url 和 user 字段
 * 4. 将结果表转换成数据流并打印输出
 */
public class TableExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 获取流执行环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 获取表环境 ====================
        // 创建流式表环境，用于在流处理场景中使用 Table API 和 SQL
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表 ====================
        // 将 DataStream 转换为 Table，表名会自动从 Event 类的字段中推断
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // ==================== 5. 使用 SQL 查询数据 ====================
        // 执行 SQL 查询，从表中提取 url 和 user 字段
        // 注意：这里可以直接在 SQL 中使用 Table 对象名，Flink 会自动注册为虚拟表
        Table visitTable = tableEnv.sqlQuery("SELECT url, user FROM " + eventTable);

        // ==================== 6. 将表转换成数据流并打印输出 ====================
        // 将查询结果表转换回 DataStream，并打印输出
        tableEnv.toDataStream(visitTable).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `TableExample` 类的 `main` 方法。

**（2）观察输出**

控制台会打印出每条数据的 url 和 user 字段，输出形式如下：

```
+I[./home, Alice]
+I[./cart, Bob]
+I[./prod?id=1, Alice]
+I[./home, Cary]
+I[./prod?id=3, Bob]
+I[./prod?id=7, Alice]
```

其中 `+I` 表示 INSERT 操作，表示这是插入到表中的新数据。

**（3）代码逻辑**

- 使用 `StreamTableEnvironment.create(env)` 创建流式表环境
- 使用 `fromDataStream()` 将 DataStream 转换为 Table
- 使用 `sqlQuery()` 执行 SQL 查询，可以直接在 SQL 中使用 Table 对象名
- 使用 `toDataStream()` 将 Table 转换回 DataStream 进行打印输出

注意事项：

1. **表环境类型**：流处理场景使用 `StreamTableEnvironment`，批处理场景使用 `BatchTableEnvironment`
2. **自动注册虚拟表**：在 SQL 中直接使用 Table 对象名时，Flink 会自动将其注册为虚拟表
3. **仅追加流**：简单查询的结果表只有 INSERT 操作，可以使用 `toDataStream()` 转换

### 4.2 Table API 方式查询示例

文件路径：`src/main/java/com/action/tableapi/TableAPIExample.java`

功能说明：演示如何使用 Table API 方式进行查询，与 SQL 方式等效。Table API 是嵌入在 Java 语言中的 DSL，通过链式调用 Table 的方法来定义查询操作。

核心概念：

- **Table API**：嵌入在 Java 和 Scala 语言内的查询 API，核心是 Table 接口类。
- **表达式（Expression）**：使用 `$()` 方法指定表中的字段，类似于 SQL 中的列名。
- **链式调用**：Table API 支持链式调用，每一步方法调用的返回结果都是一个 Table。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 方式查询示例
 * 演示如何使用 Table API 进行数据处理，与 SQL 方式等效
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 将数据流转换成表
 * 3. 使用 Table API 方式提取 url 和 user 字段
 * 4. 将结果表转换成数据流并打印输出
 */
public class TableAPIExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 获取流执行环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 获取表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表 ====================
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // ==================== 5. 使用 Table API 方式提取数据 ====================
        // 使用 Table API 的 select() 方法选择字段
        // $() 方法用于指定表中的字段，类似于 SQL 中的列名
        Table clickTable = eventTable.select($("url"), $("user"));

        // ==================== 6. 将表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(clickTable).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `TableAPIExample` 类的 `main` 方法。

**（2）观察输出**

输出结果与 SQL 方式完全一样，每条数据包含 url 和 user 字段。

**（3）代码逻辑**

- Table API 使用链式调用方式，每一步返回一个新的 Table 对象
- `$("url")` 和 `$("user")` 是表达式，用于指定表中的字段
- Table API 和 SQL 可以混合使用，得到的结果完全一致

注意事项：

1. **功能限制**：Table API 目前支持的功能相对较少，不如 SQL 通用
2. **混合使用**：Table API 和 SQL 可以很方便地结合在一起使用
3. **表达式导入**：需要导入 `import static org.apache.flink.table.api.Expressions.$;`

### 4.3 表和流转换示例

文件路径：`src/main/java/com/action/tableapi/TableToStreamExample.java`

功能说明：演示如何将表转换成数据流，包括仅追加流（Append-only Stream）和更新日志流（Changelog Stream）两种方式。

核心概念：

- **仅追加流（Append-only Stream）**：结果表中只有 INSERT 操作，可以使用 `toDataStream()` 转换。
- **更新日志流（Changelog Stream）**：结果表中包含 INSERT、UPDATE、DELETE 操作，必须使用 `toChangelogStream()` 转换。
- **追加查询（Append Query）**：查询结果只会插入新数据，不会更新已有数据。
- **更新查询（Update Query）**：查询结果会更新已有数据，例如分组聚合查询。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 表和流转换示例
 * 演示如何将表转换成数据流，包括仅追加流和更新日志流
 *
 * 功能说明：
 * 1. 演示追加查询（Append Query）：使用 toDataStream() 转换
 * 2. 演示更新查询（Update Query）：使用 toChangelogStream() 转换
 * 3. 展示两种查询的区别和适用场景
 */
public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 获取流环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 获取表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表并注册为虚拟表 ====================
        // 创建虚拟视图，方便在 SQL 中引用
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 5. 追加查询示例：查询 Alice 的访问 url 列表 ====================
        // 这是一个简单的条件查询，结果表中只有插入操作，没有更新操作
        // 因此可以使用 toDataStream() 直接转换
        Table aliceVisitTable = tableEnv.sqlQuery(
                "SELECT url, user FROM EventTable WHERE user = 'Alice'"
        );

        // ==================== 6. 更新查询示例：统计每个用户的点击次数 ====================
        // 这是一个分组聚合查询，结果表中会有更新操作
        // 因此必须使用 toChangelogStream() 转换，记录更新日志
        Table urlCountTable = tableEnv.sqlQuery(
                "SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user"
        );

        // ==================== 7. 将表转换成数据流，在控制台打印输出 ====================
        // 追加查询结果：使用 toDataStream()，输出只有 INSERT 操作
        tableEnv.toDataStream(aliceVisitTable).print("alice visit");

        // 更新查询结果：使用 toChangelogStream()，输出包含 INSERT、UPDATE_BEFORE、UPDATE_AFTER 操作
        tableEnv.toChangelogStream(urlCountTable).print("count");

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `TableToStreamExample` 类的 `main` 方法。

**（2）观察输出**

- **追加查询输出**（alice visit）：
```
alice visit> +I[./home, Alice]
alice visit> +I[./prod?id=1, Alice]
alice visit> +I[./prod?id=7, Alice]
```
每条数据前缀 `+I` 表示 INSERT 操作。

- **更新查询输出**（count）：
```
count> +I[Alice, 1]
count> +I[Bob, 1]
count> -U[Alice, 1]
count> +U[Alice, 2]
count> +I[Cary, 1]
count> -U[Bob, 1]
count> +U[Bob, 2]
count> -U[Alice, 2]
count> +U[Alice, 3]
```
- `+I` 表示 INSERT（插入）
- `-U` 表示 UPDATE_BEFORE（更新前）
- `+U` 表示 UPDATE_AFTER（更新后）

**（3）代码逻辑**

- 追加查询：简单条件查询，结果表只有 INSERT 操作，使用 `toDataStream()` 转换
- 更新查询：分组聚合查询，结果表有更新操作，使用 `toChangelogStream()` 转换
- 更新日志流记录了完整的变更历史，可以用于外部系统的增量更新

注意事项：

1. **转换方法选择**：根据查询类型选择合适的转换方法，避免运行时异常
2. **更新日志流**：更新查询必须使用 `toChangelogStream()`，否则会抛出异常
3. **RowKind**：更新日志流中的每条数据都有 RowKind 标识，表示操作类型

## 5. 时间属性和窗口

### 5.1 追加查询示例：窗口聚合

文件路径：`src/main/java/com/action/tableapi/AppendQueryExample.java`

功能说明：演示如何使用窗口表值函数（Windowing TVF）进行窗口聚合。窗口聚合是追加查询，结果表只有 INSERT 操作。

核心概念：

- **事件时间属性**：使用 `.rowtime()` 将字段指定为事件时间属性。
- **窗口表值函数（Windowing TVF）**：从 Flink 1.13 版本开始使用，用于定义窗口。
- **滚动窗口（TUMBLE）**：长度固定、时间对齐、无重叠的窗口。
- **追加查询（Append Query）**：窗口聚合的结果只会追加到结果表，不会更新已有数据。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 追加查询示例：窗口聚合
 * 演示如何使用窗口表值函数（Windowing TVF）进行窗口聚合
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用滚动窗口（TUMBLE）进行窗口聚合
 * 3. 统计每个用户在每个窗口内的访问次数
 * 4. 窗口聚合是追加查询，结果表只有 INSERT 操作
 */
public class AppendQueryExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        // 使用 .rowtime() 将 timestamp 字段指定为事件时间属性，并重命名为 ts
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")  // 将timestamp指定为事件时间，并命名为 ts
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 设置1小时滚动窗口，执行 SQL统计查询 ====================
        // 使用窗口表值函数 TUMBLE() 定义滚动窗口
        // DESCRIPTOR(ts) 指定时间属性字段
        // INTERVAL '1' HOUR 指定窗口大小为 1 小时
        // GROUP BY 中需要包含 window_start 和 window_end，这是窗口 TVF 自动添加的字段
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +     // 窗口结束时间
                                "COUNT(url) AS cnt " +    // 统计url访问次数
                                "FROM TABLE( " +
                                "TUMBLE( TABLE EventTable, " +     // 1小时滚动窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY user, window_start, window_end "
                );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        // 窗口聚合是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `AppendQueryExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
+I[Alice, 1970-01-01T01:00, 3]
+I[Bob, 1970-01-01T01:00, 1]
+I[Cary, 1970-01-01T02:00, 2]
+I[Bob, 1970-01-01T02:00, 1]
```

所有输出都以 `+I` 为前缀，表示都是以 INSERT 操作追加到结果表中的。

**（3）代码逻辑**

- 使用 `.rowtime()` 将 timestamp 字段指定为事件时间属性
- 使用 `TUMBLE()` 窗口表值函数定义滚动窗口
- 窗口聚合的结果只会追加到结果表，不会更新已有数据
- 因此可以直接使用 `toDataStream()` 转换

注意事项：

1. **时间属性定义**：必须在 DataStream 上先分配时间戳和生成水位线，然后在 Table 中指定为事件时间属性
2. **窗口 TVF**：从 Flink 1.13 版本开始使用窗口表值函数，替代了之前的分组窗口函数
3. **GROUP BY 字段**：窗口聚合的 GROUP BY 中必须包含 `window_start` 和 `window_end`

### 5.2 累积窗口示例

文件路径：`src/main/java/com/action/tableapi/CumulateWindowExample.java`

功能说明：演示如何使用累积窗口（CUMULATE）进行周期性统计。累积窗口会在统计周期内进行累积计算，每隔一段时间输出一次当前累积值。

核心概念：

- **累积窗口（CUMULATE）**：窗口表值函数中的一种，用于周期性累积统计。
- **累积步长（step）**：每隔多长时间输出一次当前累积值。
- **最大窗口长度（max window size）**：统计周期，最终目的就是统计这段时间内的数据。
- **应用场景**：按天统计 PV，但每隔 1 小时输出一次当天到目前为止的 PV 值。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 累积窗口示例
 * 演示如何使用累积窗口（CUMULATE）进行周期性统计
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用累积窗口（CUMULATE）进行统计
 * 3. 累积窗口会在统计周期内进行累积计算，每隔一段时间输出一次当前累积值
 * 4. 例如：按天统计 PV，但每隔 1 小时输出一次当天到目前为止的 PV 值
 */
public class CumulateWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 设置累积窗口，执行 SQL统计查询 ====================
        // CUMULATE() 函数定义累积窗口
        // 第三个参数为累积步长（step）：INTERVAL '30' MINUTE，表示每 30 分钟输出一次
        // 第四个参数为最大窗口长度（max window size）：INTERVAL '1' HOUR，表示统计周期为 1 小时
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +
                                "COUNT(url) AS cnt " +
                                "FROM TABLE( " +
                                "CUMULATE( TABLE EventTable, " +     // 定义累积窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '30' MINUTE, " +  // 累积步长：30 分钟
                                "INTERVAL '1' HOUR)) " +    // 最大窗口长度：1 小时
                                "GROUP BY user, window_start, window_end "
                );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `CumulateWindowExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
+I[Alice, 1970-01-01T00:30, 2]
+I[Bob, 1970-01-01T00:30, 1]
+I[Alice, 1970-01-01T01:00, 3]
+I[Bob, 1970-01-01T01:00, 1]
+I[Bob, 1970-01-01T01:30, 1]
+I[Cary, 1970-01-01T02:00, 2]
+I[Bob, 1970-01-01T02:00, 1]
```

可以看到，在第一个半小时窗口内，Alice 有 2 次访问，Bob 有 1 次访问；在第一个小时窗口结束时，Alice 累积到 3 次访问。

**（3）代码逻辑**

- 累积窗口会在统计周期内进行累积计算
- 每隔累积步长时间输出一次当前累积值
- 窗口关闭时输出最终的累积结果

注意事项：

1. **参数顺序**：CUMULATE() 函数的第三个参数是步长，第四个参数是最大窗口长度
2. **累积特性**：累积窗口会在之前的基础上叠加，直到达到最大窗口长度
3. **追加查询**：累积窗口聚合也是追加查询，可以使用 `toDataStream()` 转换

### 5.3 滑动窗口示例

文件路径：`src/main/java/com/action/tableapi/HopWindowExample.java`

功能说明：演示如何使用滑动窗口（HOP）进行窗口聚合。滑动窗口可以通过设置滑动步长来控制统计输出的频率。

核心概念：

- **滑动窗口（HOP）**：长度固定、有重叠的窗口，通过滑动步长控制输出频率。
- **滑动步长（slide）**：窗口每次滑动的时间间隔。
- **窗口大小（size）**：窗口的时间长度。
- **参数顺序**：HOP() 函数的第三个参数是步长，第四个参数是窗口大小。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 滑动窗口示例
 * 演示如何使用滑动窗口（HOP）进行窗口聚合
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用滑动窗口（HOP）进行窗口聚合
 * 3. 滑动窗口可以通过设置滑动步长来控制统计输出的频率
 * 4. 窗口大小和滑动步长可以不同，实现更灵活的统计需求
 */
public class HopWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 设置滑动窗口，执行 SQL统计查询 ====================
        // HOP() 函数定义滑动窗口
        // 第三个参数为滑动步长（slide）：INTERVAL '5' MINUTE，表示每 5 分钟滑动一次
        // 第四个参数为窗口大小（size）：INTERVAL '1' HOUR，表示窗口大小为 1 小时
        // 注意：参数顺序是步长在前，窗口大小在后
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +
                                "COUNT(url) AS cnt " +
                                "FROM TABLE( " +
                                "HOP( TABLE EventTable, " +     // 定义滑动窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '5' MINUTE, " +  // 滑动步长：5 分钟
                                "INTERVAL '1' HOUR)) " +    // 窗口大小：1 小时
                                "GROUP BY user, window_start, window_end "
                );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `HopWindowExample` 类的 `main` 方法。

**（2）观察输出**

输出结果会按照滑动步长（5 分钟）的频率输出，每个窗口大小为 1 小时。

**（3）代码逻辑**

- 滑动窗口会按照步长滑动，产生重叠的窗口
- 步长小于窗口大小时，窗口会有重叠
- 步长等于窗口大小时，滑动窗口就变成了滚动窗口

注意事项：

1. **参数顺序**：HOP() 函数的第三个参数是步长，第四个参数是窗口大小
2. **窗口重叠**：当步长小于窗口大小时，窗口会有重叠，数据会被多个窗口统计
3. **追加查询**：滑动窗口聚合也是追加查询，可以使用 `toDataStream()` 转换

### 5.4 开窗聚合示例

文件路径：`src/main/java/com/action/tableapi/OverWindowExample.java`

功能说明：演示如何使用 OVER 窗口进行开窗聚合。开窗聚合是"多对多"的关系，每行数据都会得到一个聚合结果。

核心概念：

- **OVER 窗口**：基于当前行扩展出的一段数据范围，用于开窗聚合。
- **开窗聚合**：与窗口聚合不同，开窗聚合是"多对多"的关系，每行数据都会得到一个聚合结果。
- **PARTITION BY**：按指定字段分组，每个分组内独立计算。
- **ORDER BY**：按时间属性排序，必须指定。
- **RANGE BETWEEN**：定义开窗范围，可以是时间范围或行数范围。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 开窗聚合示例
 * 演示如何使用 OVER 窗口进行开窗聚合
 * <p>
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用 OVER 窗口对每行数据计算聚合值
 * 3. 开窗聚合是"多对多"的关系，每行数据都会得到一个聚合结果
 * 4. 支持基于时间范围和行数的开窗范围
 */
public class OverWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 执行开窗聚合查询 ====================
        // 使用 OVER 窗口对每行数据计算聚合值
        // PARTITION BY user：按用户分组
        // ORDER BY ts：按时间属性排序（必须）
        // RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW：开窗范围为当前行之前 1 小时
        Table result = tableEnv.sqlQuery(
                "SELECT user, ts, " +
                        "COUNT(url) OVER (" +
                        "PARTITION BY user " +
                        "ORDER BY ts " +
                        "RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW" +
                        ") AS cnt " +
                        "FROM EventTable"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        // 开窗聚合是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `OverWindowExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
+I[Alice, 1970-01-01T00:00:01.000, 1]
+I[Bob, 1970-01-01T00:00:01.000, 1]
+I[Alice, 1970-01-01T00:25:00.000, 2]
+I[Alice, 1970-01-01T00:55:00.000, 3]
+I[Bob, 1970-01-01T01:01:00.000, 1]
+I[Cary, 1970-01-01T01:30:00.000, 1]
+I[Cary, 1970-01-01T01:59:00.000, 2]
```

可以看到，每行数据都会得到一个聚合结果。例如，Alice 在 00:00:01 时，计数为 1；在 00:25:00 时，计数为 2（包含当前行和之前 1 小时内的数据）。

**（3）代码逻辑**

- 开窗聚合使用 OVER 窗口，基于当前行扩展出数据范围
- PARTITION BY 按用户分组，每个分组内独立计算
- ORDER BY 按时间属性排序，必须指定
- RANGE BETWEEN 定义开窗范围，可以是时间范围或行数范围

注意事项：

1. **ORDER BY 必须**：OVER 窗口必须指定 ORDER BY，且必须是时间属性
2. **多对多关系**：开窗聚合是"多对多"的关系，每行数据都会得到一个聚合结果
3. **追加查询**：开窗聚合是追加查询，可以使用 `toDataStream()` 转换
4. **开窗范围**：可以使用 RANGE（时间范围）或 ROWS（行数范围）定义开窗范围

## 6. 聚合查询

### 6.1 分组聚合示例

文件路径：`src/main/java/com/action/tableapi/GroupAggregationExample.java`

功能说明：演示如何使用 GROUP BY 进行分组聚合统计。分组聚合是更新查询，结果表会有更新操作。

核心概念：

- **分组聚合（Group Aggregation）**：通过 GROUP BY 子句指定分组的键，对数据按照某个字段进行分组统计。
- **更新查询（Update Query）**：分组聚合的结果表会有更新操作，新数据到来时会更新已有数据。
- **更新日志流**：分组聚合的结果必须使用 `toChangelogStream()` 转换。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 分组聚合示例
 * 演示如何使用 GROUP BY 进行分组聚合统计
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 按照用户进行分组
 * 3. 统计每个用户的点击次数
 * 4. 分组聚合是更新查询，结果表会有更新操作
 */
public class GroupAggregationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 执行分组聚合查询 ====================
        // 按照 user 字段进行分组，统计每个用户的点击次数
        // COUNT(url) 统计每个分组中 url 的个数
        Table urlCountTable = tableEnv.sqlQuery(
                "SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user"
        );

        // ==================== 5. 将表转换成更新日志流并打印输出 ====================
        // 分组聚合是更新查询，结果表会有更新操作
        // 必须使用 toChangelogStream() 转换，记录更新日志
        tableEnv.toChangelogStream(urlCountTable).print("用户点击统计");

        // ==================== 6. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `GroupAggregationExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
用户点击统计> +I[Alice, 1]
用户点击统计> +I[Bob, 1]
用户点击统计> -U[Alice, 1]
用户点击统计> +U[Alice, 2]
用户点击统计> +I[Cary, 1]
用户点击统计> -U[Bob, 1]
用户点击统计> +U[Bob, 2]
用户点击统计> -U[Alice, 2]
用户点击统计> +U[Alice, 3]
```

可以看到，当 Alice 的第一个点击事件到来时，插入 `[Alice, 1]`；当第二个点击事件到来时，先撤回 `[Alice, 1]`，再插入 `[Alice, 2]`。

**（3）代码逻辑**

- 分组聚合会按照指定的键对数据进行分组
- 每个分组维护一个聚合状态，新数据到来时更新状态
- 结果表会有更新操作，必须使用 `toChangelogStream()` 转换

注意事项：

1. **更新查询**：分组聚合是更新查询，必须使用 `toChangelogStream()` 转换
2. **状态管理**：分组聚合需要维护状态，随着分组键的增加，状态也会增长
3. **状态 TTL**：可以通过配置状态生存时间（TTL）来限制状态大小

### 6.2 Top N 示例

文件路径：`src/main/java/com/action/tableapi/TopNExample.java`

功能说明：演示如何使用 OVER 窗口和 ROW_NUMBER() 函数实现 Top N 查询。Top N 是更新查询，结果表会有更新操作。

核心概念：

- **OVER 窗口**：基于当前行扩展出的一段数据范围，用于开窗聚合。
- **ROW_NUMBER()**：窗口函数，为每行数据计算一个排序后的行号。
- **Top N**：通过 ROW_NUMBER() 计算行号，然后筛选行号小于等于 N 的数据。
- **嵌套查询**：Top N 必须使用嵌套查询，内层计算行号，外层筛选结果。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Top N 示例
 * 演示如何使用 OVER 窗口和 ROW_NUMBER() 函数实现 Top N 查询
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 使用 OVER 窗口和 ROW_NUMBER() 函数为每行数据计算行号
 * 3. 筛选出行号小于等于 N 的数据，得到 Top N 结果
 * 4. Top N 是更新查询，结果表会有更新操作
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 执行 Top N 查询 ====================
        // 统计每个用户的访问事件中，按照字符长度排序的前两个 url
        // 使用嵌套查询：
        // 1. 内层查询：使用 ROW_NUMBER() OVER 窗口为每行数据计算行号
        //    - PARTITION BY `user`：按用户分组（user 是保留关键字，需要使用反引号转义）
        //    - ORDER BY CHAR_LENGTH(url) desc：按 url 字符长度降序排列
        // 2. 外层查询：筛选 row_num <= 2 的数据
        // 注意：timestamp 和 user 都是 Flink SQL 的保留关键字，需要使用反引号转义
        Table result = tableEnv.sqlQuery(
                "SELECT `user`, url, `timestamp`, row_num " +
                        "FROM (" +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER (" +
                        "PARTITION BY `user` " +
                        "ORDER BY CHAR_LENGTH(url) desc" +
                        ") AS row_num " +
                        "FROM EventTable " +
                        ") " +
                        "WHERE row_num <= 2"
        );

        // ==================== 5. 将表转换成更新日志流并打印输出 ====================
        // Top N 是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(result).print("Top N 结果");

        // ==================== 6. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `TopNExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
Top N 结果> +I[Alice, ./prod?id=1, 1970-01-01T00:00:05.000, 1]
Top N 结果> +I[Bob, ./cart, 1970-01-01T00:00:01.000, 1]
Top N 结果> +I[Alice, ./home, 1970-01-01T00:00:01.000, 2]
Top N 结果> +I[Cary, ./home, 1970-01-01T00:00:01.000, 1]
Top N 结果> +I[Bob, ./prod?id=3, 1970-01-01T00:01:30.000, 2]
```

可以看到，每个用户按照 url 字符长度降序排列，取前两个。

**（3）代码逻辑**

- 使用 ROW_NUMBER() OVER 窗口为每行数据计算行号
- 按照指定字段排序，行号表示排序后的位置
- 筛选行号小于等于 N 的数据，得到 Top N 结果

注意事项：

1. **嵌套查询**：Top N 必须使用嵌套查询，行号是内层查询的结果
2. **更新查询**：Top N 是更新查询，必须使用 `toChangelogStream()` 转换
3. **排序字段**：在 Top N 场景中，OVER 窗口的 ORDER BY 可以指定任意字段，不限于时间字段
4. **保留关键字**：如果字段名是 Flink SQL 的保留关键字（如 `user`、`timestamp`），在 SQL 中需要使用反引号转义

### 6.3 窗口 Top N 示例

文件路径：`src/main/java/com/action/tableapi/WindowTopNExample.java`

功能说明：演示如何结合窗口聚合和 OVER 窗口实现窗口 Top N 查询。窗口 Top N 是追加查询，结果表只有 INSERT 操作。

核心概念：

- **窗口聚合**：先使用窗口表值函数进行窗口聚合，统计每个用户在每个窗口内的访问次数。
- **窗口 Top N**：对窗口聚合结果使用 OVER 窗口和 ROW_NUMBER() 进行 Top N 排序。
- **追加查询**：窗口 Top N 是追加查询，结果表只有 INSERT 操作。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 窗口 Top N 示例
 * 演示如何结合窗口聚合和 OVER 窗口实现窗口 Top N 查询
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用滚动窗口进行窗口聚合，统计每个用户在每个窗口内的访问次数
 * 3. 使用 OVER 窗口和 ROW_NUMBER() 函数对窗口聚合结果进行 Top N 排序
 * 4. 窗口 Top N 是追加查询，结果表只有 INSERT 操作
 */
public class WindowTopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 定义子查询，进行窗口聚合 ====================
        // 基于 ts 时间字段定义 1 小时滚动窗口，统计每个用户的访问次数
        // 提取窗口信息 window_start 和 window_end，方便后续排序
        String subQuery =
                "SELECT window_start, window_end, user, COUNT(url) as cnt " +
                        "FROM TABLE ( " +
                        "TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
                        "GROUP BY window_start, window_end, user ";

        // ==================== 6. 定义 Top N 的外层查询 ====================
        // 对窗口聚合的结果表中每一行数据进行 OVER 聚合统计行号
        // 以窗口信息进行分组，按访问次数 cnt 进行排序
        // 筛选行号小于等于 2 的数据，得到每个窗口内访问次数最多的前两个用户
        String topNQuery =
                "SELECT * " +
                        "FROM (" +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER ( " +
                        "PARTITION BY window_start, window_end " +
                        "ORDER BY cnt desc " +
                        ") AS row_num " +
                        "FROM (" + subQuery + ")) " +
                        "WHERE row_num <= 2";

        // ==================== 7. 执行 SQL 得到结果表 ====================
        Table result = tableEnv.sqlQuery(topNQuery);

        // ==================== 8. 将结果表转换成数据流并打印输出 ====================
        // 窗口 Top N 是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 9. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `WindowTopNExample` 类的 `main` 方法。

**（2）观察输出**

输出结果形式如下：

```
+I[1970-01-01T00:00, 1970-01-01T01:00, Alice, 3, 1]
+I[1970-01-01T00:00, 1970-01-01T01:00, Bob, 1, 2]
+I[1970-01-01T01:00, 1970-01-01T02:00, Cary, 2, 1]
+I[1970-01-01T01:00, 1970-01-01T02:00, Bob, 1, 2]
```

可以看到，第一个 1 小时窗口中，Alice 有 3 次访问排名第一，Bob 有 1 次访问排名第二。

**（3）代码逻辑**

- 先进行窗口聚合，统计每个用户在每个窗口内的访问次数
- 然后对窗口聚合结果使用 OVER 窗口和 ROW_NUMBER() 进行 Top N 排序
- 窗口 Top N 的结果只会输出一次，不会更新

注意事项：

1. **追加查询**：窗口 Top N 是追加查询，可以直接使用 `toDataStream()` 转换
2. **窗口信息**：窗口聚合结果包含 window_start 和 window_end，用于后续分组排序
3. **嵌套查询**：窗口 Top N 需要嵌套查询，内层进行窗口聚合，外层进行 Top N 排序

## 7. 联结查询

### 7.1 常规联结查询示例

文件路径：`src/main/java/com/action/tableapi/RegularJoinExample.java`

功能说明：演示如何使用常规联结（Regular Join）进行表连接。常规联结是 SQL 中原生定义的 Join 方式，支持内联结和外联结。

核心概念：

- **常规联结（Regular Join）**：SQL 中原生定义的 Join 方式，通过 JOIN 关键字和 ON 子句进行连接。
- **内联结（INNER JOIN）**：返回两表中符合联结条件的所有行的组合。
- **外联结（OUTER JOIN）**：包括左外（LEFT JOIN）、右外（RIGHT JOIN）和全外（FULL OUTER JOIN）。
- **更新查询**：常规联结是更新查询，结果表会有更新操作。

代码实现：

```java
package com.action.tableapi;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 常规联结查询示例
 * 演示如何使用常规联结（Regular Join）进行表连接
 *
 * 功能说明：
 * 1. 创建两条数据流，分别代表订单表和商品表
 * 2. 使用 INNER JOIN 进行内联结
 * 3. 使用 LEFT JOIN 进行左外联结
 * 4. 常规联结是更新查询，结果表会有更新操作
 */
public class RegularJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 创建第一条流：订单表 ====================
        // 订单表包含：订单ID、商品ID、订单时间
        DataStream<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("order-1", "product-1", 1000L),
                Tuple3.of("order-2", "product-2", 2000L),
                Tuple3.of("order-3", "product-1", 3000L)
        );

        // ==================== 3. 创建第二条流：商品表 ====================
        // 商品表包含：商品ID、商品名称、商品价格
        DataStream<Tuple3<String, String, Double>> productStream = env.fromElements(
                Tuple3.of("product-1", "商品A", 99.9),
                Tuple3.of("product-2", "商品B", 199.9),
                Tuple3.of("product-3", "商品C", 299.9)
        );

        // ==================== 4. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("OrderTable", orderStream,
                $("orderId"), $("productId"), $("orderTime"));
        tableEnv.createTemporaryView("ProductTable", productStream,
                $("productId"), $("productName"), $("price"));

        // ==================== 5. 执行内联结查询 ====================
        // INNER JOIN：返回两表中符合联结条件的所有行的组合
        // 联结条件：OrderTable.productId = ProductTable.productId
        Table innerJoinResult = tableEnv.sqlQuery(
                "SELECT o.orderId, o.productId, p.productName, p.price " +
                        "FROM OrderTable o " +
                        "INNER JOIN ProductTable p " +
                        "ON o.productId = p.productId"
        );

        // ==================== 6. 执行左外联结查询 ====================
        // LEFT JOIN：返回左侧表中所有行，以及右侧表中匹配的行
        // 如果右侧表中没有匹配的行，则右侧表的字段为 NULL
        Table leftJoinResult = tableEnv.sqlQuery(
                "SELECT o.orderId, o.productId, p.productName, p.price " +
                        "FROM OrderTable o " +
                        "LEFT JOIN ProductTable p " +
                        "ON o.productId = p.productId"
        );

        // ==================== 7. 将结果表转换成更新日志流并打印输出 ====================
        // 常规联结是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(innerJoinResult).print("内联结结果");
        tableEnv.toChangelogStream(leftJoinResult).print("左外联结结果");

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `RegularJoinExample` 类的 `main` 方法。

**（2）观察输出**

- **内联结结果**：只返回订单表和商品表中都有匹配的行。
- **左外联结结果**：返回订单表中所有行，商品表中没有匹配的行对应字段为 NULL。

**（3）代码逻辑**

- 常规联结使用 JOIN 关键字和 ON 子句进行连接
- 内联结只返回匹配的行，外联结可以返回不匹配的行
- 常规联结是更新查询，必须使用 `toChangelogStream()` 转换

注意事项：

1. **等值条件**：目前仅支持"等值条件"作为联结条件
2. **更新查询**：常规联结是更新查询，必须使用 `toChangelogStream()` 转换
3. **笛卡尔积**：联结操作会产生笛卡尔积，需要注意性能问题

### 7.2 间隔联结查询示例

文件路径：`src/main/java/com/action/tableapi/IntervalJoinExample.java`

功能说明：演示如何使用间隔联结（Interval Join）进行基于时间的表连接。间隔联结是流处理中特有的联结方式。

核心概念：

- **间隔联结（Interval Join）**：返回符合约束条件的两条流中数据的笛卡尔积，约束条件包括常规联结条件和时间间隔限制。
- **时间间隔限制**：通过时间字段的表达式来指明两者需要满足的间隔限制。
- **仅追加表**：间隔联结只支持具有时间属性的"仅追加"表。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 间隔联结查询示例
 * 演示如何使用间隔联结（Interval Join）进行基于时间的表连接
 *
 * 功能说明：
 * 1. 创建两条数据流，分别代表订单流和点击流
 * 2. 使用间隔联结，将订单与它对应的点击行为连接起来
 * 3. 间隔联结只支持具有时间属性的"仅追加"表
 * 4. 时间间隔限制：订单时间在点击时间前后的一定时间范围内
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 创建第一条流：订单流 ====================
        // 订单流包含：用户、订单ID、订单时间
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

        // ==================== 3. 创建第二条流：点击流 ====================
        // 点击流包含：用户、URL、点击时间
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

        // ==================== 4. 将数据流转换成表，并指定时间属性 ====================
        // 对于 Tuple3 类型，字段名是 f0, f1, f2，需要使用字段名并添加别名
        Table orderTable = tableEnv.fromDataStream(orderStream,
                $("f0").as("user"), $("f1").as("orderId"), $("f2").rowtime().as("ot"));
        Table clickTable = tableEnv.fromDataStream(clickStream,
                $("user"), $("url"), $("timestamp").rowtime().as("ct"));

        // ==================== 5. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("OrderTable", orderTable);
        tableEnv.createTemporaryView("ClickTable", clickTable);

        // ==================== 6. 执行间隔联结查询 ====================
        // 间隔联结不需要用 JOIN 关键字，直接在 FROM 后将要联结的两表列出来
        // 联结条件：o.`user` = c.`user`（等值条件）
        // 注意：user 是 Flink SQL 的保留关键字，需要使用反引号转义
        // 时间间隔限制：o.ot BETWEEN c.ct - INTERVAL '5' SECOND AND c.ct + INTERVAL '10' SECOND
        // 表示订单时间在点击时间前后 [-5s, +10s] 的范围内
        // 注意：将 ct 转换为 TIMESTAMP，避免结果表中有多个 rowtime 字段
        // 当转换为 DataStream 时，只能有一个 rowtime 字段作为事件时间戳
        String sql = "SELECT c.`user`, c.url, o.orderId, o.ot, CAST(c.ct AS TIMESTAMP) AS ct " +
                "FROM OrderTable o, ClickTable c " +
                "WHERE o.`user` = c.`user` " +
                "AND o.ot BETWEEN c.ct - INTERVAL '5' SECOND AND c.ct + INTERVAL '10' SECOND";
        Table result = tableEnv.sqlQuery(sql);

        // ==================== 7. 将结果表转换成数据流并打印输出 ====================
        // 间隔联结是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `IntervalJoinExample` 类的 `main` 方法。

**（2）观察输出**

输出结果中，只包含订单时间在点击时间前后指定时间范围内的匹配数据。

**（3）代码逻辑**

- 间隔联结不需要使用 JOIN 关键字，直接在 FROM 后列出两个表
- 联结条件用 WHERE 子句定义，包括等值条件和时间间隔限制
- 时间间隔可以用 BETWEEN ... AND ... 表达式定义

注意事项：

1. **仅追加表**：间隔联结只支持具有时间属性的"仅追加"表
2. **时间属性**：必须为两条流都定义时间属性
3. **追加查询**：间隔联结是追加查询，可以使用 `toDataStream()` 转换
4. **保留关键字**：如果字段名是 Flink SQL 的保留关键字（如 `user`），需要使用反引号转义
5. **多个 rowtime 字段**：当结果表中有多个 rowtime 字段时，需要将多余的转换为 TIMESTAMP，只保留一个 rowtime 字段作为事件时间戳

## 8. 自定义函数（UDF）

### 8.1 标量函数示例

文件路径：`src/main/java/com/action/tableapi/UdfScalarFunctionExample.java`

功能说明：演示如何实现和使用自定义标量函数。标量函数是"一对一"的转换，输入一个值，输出一个值。

核心概念：

- **标量函数（Scalar Function）**：将输入的标量值转换成一个新的标量值。
- **ScalarFunction**：自定义标量函数需要继承的抽象类。
- **eval() 方法**：求值方法，必须是 public，名字必须是 eval，可以重载多次。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 自定义标量函数（UDF）示例
 * 演示如何实现和使用自定义标量函数
 *
 * 功能说明：
 * 1. 自定义一个标量函数 HashFunction，用于计算对象的哈希值
 * 2. 在表环境中注册自定义函数
 * 3. 在 SQL 中调用自定义函数
 * 4. 标量函数是"一对一"的转换，输入一个值，输出一个值
 */
public class UdfScalarFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 注册自定义标量函数 ====================
        // createTemporarySystemFunction() 方法创建临时系统函数，函数名是全局的
        tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);

        // ==================== 5. 在 SQL 中调用自定义函数 ====================
        // 调用方式与内置系统函数完全一样
        // 注意：user 是 Flink SQL 的保留关键字，需要使用反引号转义
        Table result = tableEnv.sqlQuery(
                "SELECT `user`, HashFunction(`user`) as hashValue FROM EventTable"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 自定义标量函数：计算哈希值
     * 继承 ScalarFunction 抽象类，实现 eval() 求值方法
     * eval() 方法必须是 public，名字必须是 eval，可以重载多次
     */
    public static class HashFunction extends ScalarFunction {
        // 接受字符串类型输入，返回 INT 型输出
        // 注意：如果使用 Object 类型，需要使用 @DataTypeHint 注解指定类型
        // 这里直接使用 String 类型，因为 SQL 中调用的是 HashFunction(user)，user 是字符串类型
        public int eval(String s) {
            return s != null ? s.hashCode() : 0;
        }
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `UdfScalarFunctionExample` 类的 `main` 方法。

**（2）观察输出**

输出结果中，每行数据都会有一个 hashValue 字段，表示该用户的哈希值。

**（3）代码逻辑**

- 自定义标量函数需要继承 `ScalarFunction` 抽象类
- 实现 `eval()` 方法，方法名必须是 eval，必须是 public
- 在表环境中注册函数后，可以在 SQL 中像系统函数一样调用

注意事项：

1. **方法名**：求值方法必须命名为 `eval()`，无法直接 override
2. **方法重载**：`eval()` 方法可以重载多次，支持不同的参数类型
3. **类型标注**：
   - 如果使用具体类型（如 `String`、`Integer`），Flink 可以自动推断类型
   - 如果使用 `Object` 类型，需要使用 `@DataTypeHint` 注解指定类型，否则会报类型推断错误
   - 例如：`@DataTypeHint("STRING")` 或 `@DataTypeHint(DataTypes.STRING())`
4. **保留关键字**：如果字段名是 Flink SQL 的保留关键字（如 `user`、`timestamp`），在 SQL 中需要使用反引号转义

### 8.2 表聚合函数示例

文件路径：`src/main/java/com/action/tableapi/UdfTableAggregateFunctionExample.java`

功能说明：演示如何实现和使用自定义表聚合函数。表聚合函数是"多对多"的转换，多行数据聚合成多行数据。

核心概念：

- **表聚合函数（Table Aggregate Function）**：将多行数据聚合成多行数据，输出结果可以是多行。
- **TableAggregateFunction**：自定义表聚合函数需要继承的抽象类。
- **累加器（Accumulator）**：用于存储聚合过程中的中间状态。
- **createAccumulator()**：创建累加器的方法。
- **accumulate()**：累加计算方法，每来一行数据都会调用。
- **emitValue()**：输出最终计算结果，通过 Collector 发送多行数据。

代码实现：

```java
package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 自定义表聚合函数（UDF）示例
 * 演示如何实现和使用自定义表聚合函数
 *
 * 功能说明：
 * 1. 自定义一个表聚合函数 Top2，用于查询一组数中最大的两个
 * 2. 在表环境中注册自定义函数
 * 3. 在 Table API 中使用 flatAggregate() 方法调用表聚合函数
 * 4. 表聚合函数是"多对多"的转换，多行数据聚合成多行数据
 */
public class UdfTableAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        // 为了演示，我们将 timestamp 字段作为数值
        tableEnv.createTemporaryView("MyTable", eventStream,
                $("user").as("myField"),
                $("timestamp").as("value"));

        // ==================== 4. 注册表聚合函数 ====================
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // ==================== 5. 在 Table API 中调用表聚合函数 ====================
        // 使用 flatAggregate() 方法调用表聚合函数
        // 对 MyTable 中数据按 myField 字段进行分组聚合，统计 value 值最大的两个
        // 并将聚合结果的两个字段重命名为 value 和 rank
        Table result = tableEnv.from("MyTable")
                .groupBy($("myField"))
                .flatAggregate(call("Top2", $("value")).as("value", "rank"))
                .select($("myField"), $("value"), $("rank"));

        // ==================== 6. 将结果表转换成更新日志流并打印输出 ====================
        // 表聚合函数是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }

    /**
     * 聚合累加器的类型定义
     * 包含最大的第一和第二两个数据
     */
    public static class Top2Accumulator {
        public Long first;
        public Long second;
    }

    /**
     * 自定义表聚合函数：查询一组数中最大的两个
     * 继承 TableAggregateFunction<T, ACC>，T 是输出结果类型，ACC 是累加器类型
     * 必须实现的方法：
     * - createAccumulator()：创建累加器
     * - accumulate()：累加计算方法，每来一行数据都会调用
     * - emitValue()：输出最终计算结果，通过 Collector 发送多行数据
     */
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {
        /**
         * 创建累加器
         */
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Long.MIN_VALUE;    // 为方便比较，初始值给最小值
            acc.second = Long.MIN_VALUE;
            return acc;
        }

        /**
         * 累加计算方法，每来一个数据调用一次，判断是否更新累加器
         * 方法名必须为 accumulate，必须是 public
         */
        public void accumulate(Top2Accumulator acc, Long value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        /**
         * 输出最终计算结果
         * 输出 (数值，排名) 的二元组，输出两行数据
         * 方法名必须为 emitValue，必须是 public
         * 通过 Collector 的 collect() 方法发送多行数据
         */
        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Long, Integer>> out) {
            if (acc.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}
```

运行说明：

**（1）运行代码**

直接运行 `UdfTableAggregateFunctionExample` 类的 `main` 方法。

**（2）观察输出**

输出结果中，每个分组会输出最大的两个值及其排名。

**（3）代码逻辑**

- 自定义表聚合函数需要继承 `TableAggregateFunction<T, ACC>` 抽象类
- 必须实现 `createAccumulator()`、`accumulate()` 和 `emitValue()` 方法
- 使用 `flatAggregate()` 方法在 Table API 中调用表聚合函数
- 通过 `Collector` 的 `collect()` 方法发送多行数据

注意事项：

1. **累加器类型**：需要定义一个累加器类来存储聚合过程中的中间状态
2. **方法名**：`accumulate()` 和 `emitValue()` 方法名必须固定，必须是 public
3. **多行输出**：表聚合函数可以输出多行数据，通过 `Collector` 发送
4. **更新查询**：表聚合函数是更新查询，必须使用 `toChangelogStream()` 转换
5. **Table API 调用**：表聚合函数只能在 Table API 中使用 `flatAggregate()` 方法调用，不能在 SQL 中直接使用

## 9. 总结

### 9.1 Table API 和 SQL 的特点

- **声明式编程**：使用 SQL 或 Table API 进行声明式编程，代码简洁易读
- **批流统一**：Table API 和 SQL 支持批处理和流处理，同一套代码可以处理两种场景
- **功能强大**：支持窗口、聚合、联结等复杂查询操作
- **易于集成**：可以方便地与外部系统集成，支持多种连接器

### 9.2 查询类型总结

| 查询类型 | 特点 | 转换方法 | 示例 |
|---------|------|---------|------|
| 追加查询 | 结果表只有 INSERT 操作 | `toDataStream()` | 简单条件查询、窗口聚合 |
| 更新查询 | 结果表有 UPDATE 操作 | `toChangelogStream()` | 分组聚合、Top N |

### 9.3 窗口类型总结

| 窗口类型 | 函数 | 特点 | 应用场景 |
|---------|------|------|---------|
| 滚动窗口 | `TUMBLE()` | 长度固定、无重叠 | 周期性统计 |
| 滑动窗口 | `HOP()` | 长度固定、有重叠 | 滑动统计 |
| 累积窗口 | `CUMULATE()` | 累积计算 | 周期性累积统计 |

## 10. 参考资料

- Flink 官方文档 - Table API 和 SQL：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/overview/
- Flink 官方文档 - 时间属性：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/concepts/time_attributes/
- Flink 官方文档 - 窗口表值函数：https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/sql/queries/window-tvf/

