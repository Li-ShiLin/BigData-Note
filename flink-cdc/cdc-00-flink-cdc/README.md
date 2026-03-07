<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
- [2.项目结构](#2%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
- [3.案例1：FlinkCDC_DataStream（DataStream 方式）](#3%E6%A1%88%E4%BE%8B1flinkcdc_datastreamdatastream-%E6%96%B9%E5%BC%8F)
    - [3.1 功能说明](#31-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [3.2 完整代码](#32-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [3.3 说明](#33-%E8%AF%B4%E6%98%8E)
- [4.案例2：FlinkCDC_SQL（SQL 方式）](#4%E6%A1%88%E4%BE%8B2flinkcdc_sqlsql-%E6%96%B9%E5%BC%8F)
    - [4.1 功能说明](#41-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [4.2 完整代码](#42-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [4.3 DataStream 与 SQL 方式对比](#43-datastream-%E4%B8%8E-sql-%E6%96%B9%E5%BC%8F%E5%AF%B9%E6%AF%94)
- [5.构建与提交](#5%E6%9E%84%E5%BB%BA%E4%B8%8E%E6%8F%90%E4%BA%A4)
    - [5.1 构建](#51-%E6%9E%84%E5%BB%BA)
    - [5.2 提交（按主类选择）](#52-%E6%8F%90%E4%BA%A4%E6%8C%89%E4%B8%BB%E7%B1%BB%E9%80%89%E6%8B%A9)
- [6.pom.xml 依赖说明](#6pomxml-%E4%BE%9D%E8%B5%96%E8%AF%B4%E6%98%8E)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Flink CDC 基础模块，演示 **DataStream API** 与 **SQL API** 两种方式从 MySQL 读取 CDC 变更数据。

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8（Flink、Flink CDC 依赖）                 |
| MySQL | 已安装并开启 binlog（ROW 格式）                   |
| Flink | 1.13+ / 1.14+ 集群（用于提交 Flink CDC 任务）     |
| HDFS  | 用于 Checkpoint 存储（DataStream 案例需要）       |
| Maven | 3.x（构建项目）                               |

**节点约定（可按实际修改）：**

- **server02**：MySQL 源端
- **server01**：Flink JobManager、HDFS NameNode、任务提交节点

---

## 2.项目结构

```
cdc-00-flink-cdc/
├── pom.xml
├── README.md
└── src/main/java/com/action/flinkcdc/
    ├── FlinkCDC_DataStream.java   # 案例1：DataStream 方式 + Checkpoint
    └── FlinkCDC_SQL.java          # 案例2：SQL 方式（无 Checkpoint）
```

---

## 3.案例1：FlinkCDC_DataStream（DataStream 方式）

### 3.1 功能说明

`FlinkCDC_DataStream` 使用 **MySqlSource** DataStream API 从 MySQL 读取 binlog 变更数据：

- **启动模式**：`StartupOptions.initial()`，先全量快照再跟 binlog
- **Checkpoint**：开启 Checkpoint，将 binlog 位点等状态持久化到 HDFS
- **输出格式**：Debezium 标准 JSON

### 3.2 完整代码

```java
package com.action.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream {

    public static void main(String[] args) throws Exception {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启CheckPoint
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://server01:8020/flinkCDC/ck");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //3.使用FlinkCDC构建MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("server02")
                .port(3306)
                .username("root")
                .password("PS666666")
                .databaseList("test")
                .tableList("test.t1") //在写表时，需要带上库名。如果什么都不写，则表示监控所有的表
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //4.读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        //5.打印
        mysqlDS.print();

        //6.启动
        env.execute();

    }
}
```

### 3.3 说明

- **Checkpoint 端口**：代码中为 `hdfs://server01:8020`，若环境 `core-site.xml` 中 `fs.defaultFS` 为 `hdfs://server01:9000`
  ，需改为 `9000`，否则会报 `Connection refused`。
- 若 HDFS 不可用，可临时改为 `file:///tmp/flinkCDC/ck` 做本地测试。

---

## 4.案例2：FlinkCDC_SQL（SQL 方式）

### 4.1 功能说明

`FlinkCDC_SQL` 使用 **Flink Table API / SQL** 声明式建表，从 MySQL 读取 CDC 数据：

- **建表方式**：`CREATE TABLE ... WITH ('connector' = 'mysql-cdc', ...)`
- **无 Checkpoint**：本示例未显式开启 Checkpoint，适合简单演示
- **输出格式**：表格形式（+I/-U/+U/-D），非 JSON

### 4.2 完整代码

```java
package com.action.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {

    public static void main(String[] args) {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用FlinkCDC SQL方式建表
        tableEnv.executeSql("" +
                "create table t1(\n" +
                "    id string primary key NOT ENFORCED,\n" +
                "    name string" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'server02',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'PS666666',\n" +
                " 'database-name' = 'test',\n" +
                " 'table-name' = 't1'\n" +
                ")");

        //3.查询并打印
        Table table = tableEnv.sqlQuery("select * from t1");
        table.execute().print();

    }
}
```

### 4.3 DataStream 与 SQL 方式对比

| 对比项    | DataStream          | SQL                   |
|--------|---------------------|-----------------------|
| 建表/源定义 | MySqlSource builder | CREATE TABLE ... WITH |
| 输出格式   | Debezium JSON       | 表格（+I/-U/+U/-D）       |
| 代码风格   | imperative          | 声明式                   |

---

## 5.构建与提交

### 5.1 构建

```bash
mvn clean package -DskipTests
```

生成：`cdc-00-flink-cdc-1.0-SNAPSHOT-jar-with-dependencies.jar`（胖包）

### 5.2 提交（按主类选择）

```bash
# 案例1 DataStream（需 HDFS 已启动）
bin/flink run -c com.action.flinkcdc.FlinkCDC_DataStream /path/to/cdc-00-flink-cdc-1.0-SNAPSHOT-jar-with-dependencies.jar

# 案例2 SQL
bin/flink run -c com.action.flinkcdc.FlinkCDC_SQL /path/to/cdc-00-flink-cdc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## 6.pom.xml 依赖说明

本模块依赖：flink-java、flink-streaming-java、flink-clients、flink-table-planner、flink-table-runtime、flink-table-api-java-bridge、flink-connector-base、flink-connector-mysql-cdc、mysql-connector-java。版本由父工程
`dependencyManagement` 统一管理。
