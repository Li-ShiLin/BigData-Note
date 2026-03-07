<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
- [2.项目结构](#2%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
- [3.StartupOptions 启动模式说明](#3startupoptions-%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F%E8%AF%B4%E6%98%8E)
- [4.案例1：FlinkCDC_DataStream_Initial（initial() 启动模式）](#4%E6%A1%88%E4%BE%8B1flinkcdc_datastream_initialinitial-%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F)
    - [4.1 功能说明](#41-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [4.2 完整代码](#42-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [4.3 MySQL 测试准备](#43-mysql-%E6%B5%8B%E8%AF%95%E5%87%86%E5%A4%87)
    - [4.4 运行效果说明](#44-%E8%BF%90%E8%A1%8C%E6%95%88%E6%9E%9C%E8%AF%B4%E6%98%8E)
    - [4.5 Debezium JSON 核心结构（initial 模式）](#45-debezium-json-%E6%A0%B8%E5%BF%83%E7%BB%93%E6%9E%84initial-%E6%A8%A1%E5%BC%8F)
- [5.案例2：FlinkCDC_DataStream_Latest（latest() 启动模式）](#5%E6%A1%88%E4%BE%8B2flinkcdc_datastream_latestlatest-%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F)
    - [5.1 功能说明](#51-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [5.2 latest() 与 initial() 的区别](#52-latest-%E4%B8%8E-initial-%E7%9A%84%E5%8C%BA%E5%88%AB)
    - [5.3 完整代码](#53-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [5.4 MySQL 测试准备](#54-mysql-%E6%B5%8B%E8%AF%95%E5%87%86%E5%A4%87)
    - [5.5 运行效果说明](#55-%E8%BF%90%E8%A1%8C%E6%95%88%E6%9E%9C%E8%AF%B4%E6%98%8E)
- [6.案例3：FlinkCDC_DataStream_Earliest（earliest() 启动模式）](#6%E6%A1%88%E4%BE%8B3flinkcdc_datastream_earliestearliest-%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F)
    - [6.1 功能说明](#61-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [6.2 三种启动模式对比](#62-%E4%B8%89%E7%A7%8D%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F%E5%AF%B9%E6%AF%94)
    - [6.3 earliest() 与 initial() 的区别](#63-earliest-%E4%B8%8E-initial-%E7%9A%84%E5%8C%BA%E5%88%AB)
    - [6.4 完整代码](#64-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [6.5 MySQL 测试准备](#65-mysql-%E6%B5%8B%E8%AF%95%E5%87%86%E5%A4%87)
    - [6.6 运行效果说明](#66-%E8%BF%90%E8%A1%8C%E6%95%88%E6%9E%9C%E8%AF%B4%E6%98%8E)
- [7.案例4：FlinkCDCDataStreamTest（带 Checkpoint 的完整示例）](#7%E6%A1%88%E4%BE%8B4flinkcdcdatastreamtest%E5%B8%A6-checkpoint-%E7%9A%84%E5%AE%8C%E6%95%B4%E7%A4%BA%E4%BE%8B)
    - [7.1 功能说明](#71-%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
    - [7.2 完整代码](#72-%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
    - [7.3 前置依赖](#73-%E5%89%8D%E7%BD%AE%E4%BE%9D%E8%B5%96)
- [8.构建与提交](#8%E6%9E%84%E5%BB%BA%E4%B8%8E%E6%8F%90%E4%BA%A4)
    - [8.1 构建](#81-%E6%9E%84%E5%BB%BA)
    - [8.2 上传 JAR 到 Linux](#82-%E4%B8%8A%E4%BC%A0-jar-%E5%88%B0-linux)
    - [8.3 提交任务](#83-%E6%8F%90%E4%BA%A4%E4%BB%BB%E5%8A%A1)
    - [8.4 Web 控制台](#84-web-%E6%8E%A7%E5%88%B6%E5%8F%B0)
- [9.pom.xml 依赖说明](#9pomxml-%E4%BE%9D%E8%B5%96%E8%AF%B4%E6%98%8E)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

基于 **Flink CDC DataStream API** 的 MySQL 变更数据捕获示例。本模块演示使用 `MySqlSource` 从 MySQL 读取 binlog 变更数据，并对比
`initial()`、`latest()`、`earliest()` 三种启动模式的效果差异。

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8（Flink、Flink CDC 依赖）                 |
| MySQL | 已安装并开启 binlog（ROW 格式）                   |
| Flink | 1.13+ / 1.14+ 集群（用于提交 Flink CDC 任务）     |
| Maven | 3.x（构建项目）                               |

**节点约定（可按实际修改）：**

- **server02**：MySQL 源端（或按代码中的 `hostname` 配置）
- **server01**：Flink JobManager、任务提交节点

---

## 2.项目结构

```
cdc-01-FlinkCDC-DataStream/
├── pom.xml
├── README.md
└── src/main/java/com/action/flinkcdcDataStream/
    ├── FlinkCDC_DataStream_Initial.java    # 案例1：initial() 启动模式
    ├── FlinkCDC_DataStream_Latest.java     # 案例2：latest() 启动模式
    ├── FlinkCDC_DataStream_Earliest.java   # 案例3：earliest() 启动模式
    └── FlinkCDCDataStreamTest.java         # 案例4：带 Checkpoint 的完整示例
```

---

## 3.StartupOptions 启动模式说明

| 启动模式         | 含义                  | 是否读取历史数据     | 适用场景     |
|--------------|---------------------|--------------|----------|
| `initial()`  | 启动时先做全量快照，再跟 binlog | 是（全量快照）      | 需要完整数据同步 |
| `earliest()` | 从 binlog 最早位置开始读取   | 是（历史 binlog） | 需要历史变更追溯 |
| `latest()`   | 从 binlog 最新位置开始监听   | 否            | 只需实时变更监听 |

---

## 4.案例1：FlinkCDC_DataStream_Initial（initial() 启动模式）

### 4.1 功能说明

`FlinkCDC_DataStream_Initial` 使用 `StartupOptions.initial()` 启动模式：

- 首次启动时，对监控的数据库表执行**全量数据快照**，将表中已存在的数据以 Debezium 标准 JSON 格式输出（op="r"）
- 快照完成后，继续监听 MySQL binlog，实时捕获后续的 INSERT、UPDATE、DELETE 操作（op="c/u/d"）
- 输出格式：Debezium 标准 JSON，包含 before、after、source、op、ts_ms 等字段

### 4.2 完整代码

```java
package com.action.flinkcdcDataStream;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream_Initial {

    public static void main(String[] args) throws Exception {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启CheckPoint

        //3.使用FlinkCDC构建MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("server02")
                .port(3306)
                .username("root")
                .password("PS666666")
                .databaseList("test")
                .tableList("test.t1") //在写表时，需要带上库名。如果什么都不写，则表示监控所有的表
                .startupOptions(StartupOptions.initial())
                .serverTimeZone("UTC")  // MySQL 5.7.38 使用 UTC，需与 JVM 时区一致或显式指定
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

/*测试流程及效果如下*/
/*
1.在MySQL中创建测试表并插入数据
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE test;

CREATE TABLE IF NOT EXISTS t1 (
  id   VARCHAR(255) NOT NULL COMMENT '主键',
  name VARCHAR(255) DEFAULT NULL COMMENT '姓名',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='表t1';


-- 先插入一些历史数据
INSERT INTO t1 VALUES('1001','zhangsan');
INSERT INTO t1 VALUES('1002','lisi');
INSERT INTO t1 VALUES('1003','wangwu');
INSERT INTO t1 VALUES('1004','sun4');

-- 执行一些历史更新和删除操作
UPDATE t1 SET name='wangwu-updated' WHERE id='1003';
DELETE FROM t1 WHERE id='1004';
*/


/*
2.程序启动后，因指定了StartupOptions.initial()启动模式，Flink CDC 首先对MySQL 的 test 库 t1 表执行全量数据快照读取，将表中已存在的 3 条业务数据（id:1001/1002/1003）以Debezium 标准 JSON 格式逐条输出
{"before":null,"after":{"id":"1003","name":"wangwu-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1770465776722,"transaction":null}
{"before":null,"after":{"id":"1001","name":"zhangsan"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1770465776721,"transaction":null}
{"before":null,"after":{"id":"1002","name":"lisi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1770465776722,"transaction":null}
*/


// 3.随后，Flink CDC 继续监听MySQL 的 binlog 日志，当我们对 test 库 t1 表执行 INSERT/UPDATE/DELETE 操作时，Flink CDC 能够实时捕获这些变更，并将变更数据以Debezium 标准 JSON 格式输出
/*
3.1 新增数据:   INSERT INTO t1 (id, name) VALUES ('1005', 'zhaoliu');
{"before":null,"after":{"id":"1005","name":"zhaoliu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770465896000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":6704,"row":0,"thread":2,"query":null},"op":"c","ts_ms":1770465897231,"transaction":null}


3.2 新增数据:   INSERT INTO t1 (id, name) VALUES ('1006', 'sunqi');
{"before":null,"after":{"id":"1006","name":"sunqi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770465919000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":6973,"row":0,"thread":2,"query":null},"op":"c","ts_ms":1770465919975,"transaction":null}


3.3 更新数据:   UPDATE t1 SET name='lisi-updated' WHERE id='1002';
{"before":{"id":"1002","name":"lisi"},"after":{"id":"1002","name":"lisi-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770465955000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":7240,"row":0,"thread":2,"query":null},"op":"u","ts_ms":1770465955813,"transaction":null}

3.4 更新数据:   UPDATE t1 SET name='wangwu' WHERE id='1003';
{"before":{"id":"1003","name":"wangwu-updated"},"after":{"id":"1003","name":"wangwu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770466042000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":7528,"row":0,"thread":2,"query":null},"op":"u","ts_ms":1770466043133,"transaction":null}

3.5 删除数据:   DELETE FROM t1 WHERE id='1001';
{"before":{"id":"1001","name":"zhangsan"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770466064000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":7820,"row":0,"thread":2,"query":null},"op":"d","ts_ms":1770466064460,"transaction":null}


3.6 删除数据:   DELETE FROM t1 WHERE id='1005';
{"before":{"id":"1005","name":"zhaoliu"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770466096000,"snapshot":"false","db":"test","sequence":null,"table":"t1","server_id":1,"gtid":null,"file":"mysql-bin.000004","pos":8090,"row":0,"thread":2,"query":null},"op":"d","ts_ms":1770466096661,"transaction":null}
*/


/*4.AI总结
Flink CDC 通过捕获MySQL 的 binlog 日志，实现了对MySQL 数据库变更的实时监听和处理。
它能够捕获INSERT、UPDATE 和 DELETE 等操作，并将变更数据以Debezium 标准 JSON 格式输出，方便后续的数据处理和分析工作。
Flink CDC 的这种能力使得它在实时数据集成和流处理场景中非常有用，能够帮助企业实现对数据库变更的实时响应和处理。

一、Debezium标准JSON核心结构
顶层固定字段：
- before：数据变更前的内容
- after：数据变更后的内容
- source：数据源元信息（含库表、Binlog、操作时间等）
- op：操作类型标识（r/c/u/d）
- ts_ms：Flink CDC处理事件的时间戳
- transaction：事务信息（本次无事务，为null）

source元信息通用特征：
- 固定：db=test、table=t1（监控目标）、connector=mysql
- 增量操作时：包含有效Binlog信息（server_id/file/pos）和MySQL实际操作时间戳
- 全量快照时：Binlog相关字段为0/空，无实际Binlog解析信息

二、不同操作类型的JSON字段规律
+------------+--------------+--------------+----------------------------------+
| 操作类型   | before       | after        | source 特征                        |
+------------+--------------+--------------+----------------------------------+
| 全量快照(r)| null         | 全量业务数据 | Binlog字段为0/空，无解析信息           |
+------------+--------------+--------------+----------------------------------+
| 插入(c)    | null         | 新插入数据   | 含有效Binlog信息和操作时间戳          |
+------------+--------------+--------------+----------------------------------+
| 更新(u)    | 修改前旧数据 | 修改后新数据 | 含有效Binlog信息和操作时间戳           |
+------------+--------------+--------------+----------------------------------+
| 删除(d)    | 删除前旧数据 | null         | 含有效Binlog信息和操作时间戳         |
+------------+--------------+--------------+----------------------------------+

所有操作的JSON均清晰区分业务数据与元信息，字段含义明确、规律统一，便于解析和后续业务处理。
*/
```

### 4.3 MySQL 测试准备

在 MySQL 中创建测试表并插入数据：

```sql
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- 切换到目标数据库
USE test;

-- 先删除旧表（如有），避免表结构冲突
DROP TABLE IF EXISTS t1;

-- 创建表 t1（InnoDB引擎，utf8mb4编码，主键约束+字段注释）
CREATE TABLE IF NOT EXISTS t1 (
    id VARCHAR(255) NOT NULL COMMENT '主键',
    name VARCHAR(255) DEFAULT NULL COMMENT '姓名',
    PRIMARY KEY (id)
) ENGINE = InnoDB 
  DEFAULT CHARSET = utf8mb4 
  COLLATE = utf8mb4_unicode_ci 
  COMMENT = '表t1';

-- 批量插入初始数据（显式指定字段名，提升代码健壮性）
INSERT INTO t1 (id, name) VALUES 
    ('1001', 'zhangsan'),
    ('1002', 'lisi'),
    ('1003', 'wangwu'),
    ('1004', 'sun4');

-- 更新指定记录
UPDATE t1 
SET name = 'wangwu-updated' 
WHERE id = '1003';

-- 删除指定记录
DELETE FROM t1 
WHERE id = '1004';
```

### 4.4 运行效果说明

**4.4.1 全量快照阶段**

程序启动后，因指定了 `StartupOptions.initial()` 启动模式，Flink CDC 首先对 MySQL 的 `test` 库 `t1` 表执行**全量数据快照**
读取，将表中已存在的 3 条业务数据（id:1001/1002/1003）以 Debezium 标准 JSON 格式逐条输出。示例输出（op="r" 表示快照读取）：

```json
{
  "before": null,
  "after": {
    "id": "1003",
    "name": "wangwu-updated"
  },
  "source": {
    "version": "1.9.7.Final",
    "connector": "mysql",
    "name": "mysql_binlog_source",
    "ts_ms": 0,
    "snapshot": "false",
    "db": "test",
    "sequence": null,
    "table": "t1",
    "server_id": 0,
    "gtid": null,
    "file": "",
    "pos": 0,
    "row": 0,
    "thread": null,
    "query": null
  },
  "op": "r",
  "ts_ms": 1770465776722,
  "transaction": null
}
```

**4.4.2 增量监听阶段**

随后，Flink CDC 继续监听 MySQL 的 binlog 日志。当对 `test` 库 `t1` 表执行 INSERT/UPDATE/DELETE 操作时，Flink CDC
能够实时捕获这些变更，并将变更数据以 Debezium 标准 JSON 格式输出。示例：

- 新增数据：`INSERT INTO t1 (id, name) VALUES ('1005', 'zhaoliu');` → op="c"
- 更新数据：`UPDATE t1 SET name='lisi-updated' WHERE id='1002';` → op="u"
- 删除数据：`DELETE FROM t1 WHERE id='1001';` → op="d"

### 4.5 Debezium JSON 核心结构（initial 模式）

| 顶层字段        | 含义                         |
|-------------|----------------------------|
| before      | 数据变更前的内容                   |
| after       | 数据变更后的内容                   |
| source      | 数据源元信息（库表、Binlog、操作时间等）    |
| op          | 操作类型：r=快照读取，c=插入，u=更新，d=删除 |
| ts_ms       | Flink CDC 处理事件的时间戳         |
| transaction | 事务信息（无事务时为 null）           |

| 操作类型    | before | after  |
|---------|--------|--------|
| 全量快照(r) | null   | 全量业务数据 |
| 插入(c)   | null   | 新插入数据  |
| 更新(u)   | 修改前旧数据 | 修改后新数据 |
| 删除(d)   | 删除前旧数据 | null   |

---

## 5.案例2：FlinkCDC_DataStream_Latest（latest() 启动模式）

### 5.1 功能说明

`FlinkCDC_DataStream_Latest` 使用 `StartupOptions.latest()` 启动模式：

- **不读取历史数据**：程序启动后不会执行全量数据快照，不会输出表中已存在的历史数据
- **只监听后续变更**：从最新的 binlog 位置开始监听，只捕获程序启动后的数据变更操作
- **启动速度快**：由于跳过了全量快照阶段，程序启动速度更快
- **适用场景**：适用于只需要实时监听数据变更、不需要历史数据的场景

### 5.2 latest() 与 initial() 的区别

| 启动模式      | 是否读取历史数据 | 启动速度     | 适用场景     |
|-----------|----------|----------|----------|
| initial() | 是（全量快照）  | 较慢（需快照）  | 需要完整数据同步 |
| latest()  | 否（跳过快照）  | 较快（直接监听） | 只需实时变更监听 |

**注意**：latest() 模式下不会出现 op="r"（全量快照读取）类型的操作。

### 5.3 完整代码

```java
package com.action.flinkcdcDataStream;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream_Latest {

    public static void main(String[] args) throws Exception {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启CheckPoint

        //3.使用FlinkCDC构建MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("server02")
                .port(3306)
                .username("root")
                .password("PS666666")
                .databaseList("test")
                .tableList("test.t2") //在写表时，需要带上库名。如果什么都不写，则表示监控所有的表
                .startupOptions(StartupOptions.latest())
                .serverTimeZone("UTC")  // MySQL 5.7.38 使用 UTC，需与 JVM 时区一致或显式指定
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

/*测试流程及效果如下*/
/*
1.在MySQL中创建测试表并插入数据
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

use test;
CREATE TABLE IF NOT EXISTS t2 (
    id VARCHAR(50),
    name VARCHAR(50)
);

INSERT INTO t2 VALUES('1001','zhangsan');
INSERT INTO t2 VALUES('1002','lisi');
INSERT INTO t2 VALUES('1003','wangwu');
*/


/*
2.程序启动后，因指定了StartupOptions.latest()启动模式，Flink CDC 不会读取MySQL 的 test 库 t2 表中已存在的历史数据（全量快照），
  而是直接从最新的 binlog 位置开始监听，因此程序启动后不会输出任何历史数据（id:1001/1002/1003 不会被输出）。
  这是 latest() 模式与 initial() 模式的主要区别：latest() 只监听后续的变更，不读取历史数据。
*/


// 3.随后，Flink CDC 监听MySQL 的 binlog 日志，当我们对 test 库 t2 表执行 INSERT/UPDATE/DELETE 操作时，Flink CDC 能够实时捕获这些变更，并将变更数据以Debezium 标准 JSON 格式输出
/*
3.1 新增数据:   INSERT INTO t2 (id, name) VALUES ('1004', 'zhaoliu');
{"before":null,"after":{"id":"1004","name":"zhaoliu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306746000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":341,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770306747027,"transaction":null}


3.2 新增数据:   INSERT INTO t2 (id, name) VALUES ('1005', 'sunqi');
{"before":null,"after":{"id":"1005","name":"sunqi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306784000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":610,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770306784955,"transaction":null}


3.3 更新数据:   UPDATE t2 SET name='lisi-updated' WHERE id='1002';
{"before":{"id":"1002","name":"lisi"},"after":{"id":"1002","name":"lisi-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306817000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":877,"row":0,"thread":21,"query":null},"op":"u","ts_ms":1770306817525,"transaction":null}

3.4 更新数据:   UPDATE t2 SET name='wangwu-updated' WHERE id='1003';
{"before":{"id":"1003","name":"wangwu"},"after":{"id":"1003","name":"wangwu-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306841000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":1165,"row":0,"thread":21,"query":null},"op":"u","ts_ms":1770306841213,"transaction":null}

3.5 删除数据:   DELETE FROM t2 WHERE id='1001';
{"before":{"id":"1001","name":"zhangsan"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306869000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":1457,"row":0,"thread":21,"query":null},"op":"d","ts_ms":1770306869644,"transaction":null}

3.6 删除数据:   DELETE FROM t2 WHERE id='1005';
{"before":{"id":"1005","name":"sunqi"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770306917000,"snapshot":"false","db":"test","sequence":null,"table":"t2","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":1727,"row":0,"thread":21,"query":null},"op":"d","ts_ms":1770306917896,"transaction":null}
*/


/*4.AI总结
Flink CDC 通过捕获MySQL 的 binlog 日志，实现了对MySQL 数据库变更的实时监听和处理。
它能够捕获INSERT、UPDATE 和 DELETE 等操作，并将变更数据以Debezium 标准 JSON 格式输出，方便后续的数据处理和分析工作。
Flink CDC 的这种能力使得它在实时数据集成和流处理场景中非常有用，能够帮助企业实现对数据库变更的实时响应和处理。

一、latest() 启动模式的特点
- 不读取历史数据：程序启动后不会执行全量数据快照，不会输出表中已存在的历史数据
- 只监听后续变更：从最新的 binlog 位置开始监听，只捕获程序启动后的数据变更操作
- 启动速度快：由于跳过了全量快照阶段，程序启动速度更快
- 适用场景：适用于只需要实时监听数据变更，不需要历史数据的场景

二、latest() 与 initial() 模式的区别
+------------------+------------------+------------------+------------------+
| 启动模式         | 是否读取历史数据 | 启动速度         | 适用场景         |
+------------------+------------------+------------------+------------------+
| initial()        | 是（全量快照）   | 较慢（需快照）   | 需要完整数据同步 |
+------------------+------------------+------------------+------------------+
| latest()         | 否（跳过快照）   | 较快（直接监听） | 只需实时变更监听 |
+------------------+------------------+------------------+------------------+

三、Debezium标准JSON核心结构
顶层固定字段：
- before：数据变更前的内容
- after：数据变更后的内容
- source：数据源元信息（含库表、Binlog、操作时间等）
- op：操作类型标识（c/u/d，注意：latest() 模式下不会出现 r 类型，因为没有全量快照）
- ts_ms：Flink CDC处理事件的时间戳
- transaction：事务信息（本次无事务，为null）

source元信息通用特征：
- 固定：db=test、table=t2（监控目标）、connector=mysql
- 增量操作时：包含有效Binlog信息（server_id/file/pos）和MySQL实际操作时间戳
- latest() 模式下：所有操作都是增量操作，source 中均包含有效的 Binlog 信息

四、不同操作类型的JSON字段规律（latest() 模式）
+------------+--------------+--------------+----------------------------------+
| 操作类型   | before       | after        | source 特征                        |
+------------+--------------+--------------+----------------------------------+
| 插入(c)    | null         | 新插入数据   | 含有效Binlog信息和操作时间戳          |
+------------+--------------+--------------+----------------------------------+
| 更新(u)    | 修改前旧数据 | 修改后新数据 | 含有效Binlog信息和操作时间戳           |
+------------+--------------+--------------+----------------------------------+
| 删除(d)    | 删除前旧数据 | null         | 含有效Binlog信息和操作时间戳         |
+------------+--------------+--------------+----------------------------------+

注意：latest() 模式下不会出现 op="r"（全量快照读取）类型的操作，因为不会执行全量快照。

所有操作的JSON均清晰区分业务数据与元信息，字段含义明确、规律统一，便于解析和后续业务处理。
*/
```

### 5.4 MySQL 测试准备

```sql
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- 切换到目标数据库
USE test;

-- 创建表 t2（IF NOT EXISTS 避免重复创建）
CREATE TABLE IF NOT EXISTS t2 (
    id VARCHAR(50),
    name VARCHAR(50)
);

-- 批量插入测试数据（显式指定字段名，提升代码健壮性）
INSERT INTO t2 (id, name) VALUES 
    ('1001', 'zhangsan'),
    ('1002', 'lisi'),
    ('1003', 'wangwu');
```

### 5.5 运行效果说明

程序启动后，因指定了 `StartupOptions.latest()` 启动模式，Flink CDC **不会读取** MySQL 的 `test` 库 `t2`
表中已存在的历史数据（全量快照），而是直接从最新的 binlog 位置开始监听。因此程序启动后不会输出任何历史数据（id:1001/1002/1003
不会被输出）。

随后，当对 `test` 库 `t2` 表执行 INSERT/UPDATE/DELETE 操作时，Flink CDC 能够实时捕获这些变更，并将变更数据以 Debezium 标准
JSON 格式输出（op="c/u/d"）。

---

## 6.案例3：FlinkCDC_DataStream_Earliest（earliest() 启动模式）

### 6.1 功能说明

`FlinkCDC_DataStream_Earliest` 使用 `StartupOptions.earliest()` 启动模式：

- **从最早 binlog 位置读取**：程序启动后会从 binlog 的最早可用位置开始读取
- **读取历史 binlog 数据**：会读取所有可用的历史 binlog 中的变更记录（如果 binlog 文件还存在）
- **不执行全量快照**：与 initial() 模式不同，不会执行全量数据快照，而是通过 binlog 重建历史变更
- **完整历史追溯**：能够追溯所有历史变更操作，包括程序启动前的数据变更
- **适用场景**：适用于需要完整历史变更追溯，且 binlog 保留时间较长的场景

### 6.2 三种启动模式对比

| 启动模式       | 是否读取历史数据    | 数据来源        | 启动速度     | 适用场景     |
|------------|-------------|-------------|----------|----------|
| initial()  | 是（全量快照）     | 表快照+binlog  | 较慢（需快照）  | 需要完整数据同步 |
| earliest() | 是（历史binlog） | 历史binlog+实时 | 较慢（读历史）  | 需要历史变更追溯 |
| latest()   | 否（跳过快照）     | 实时binlog    | 较快（直接监听） | 只需实时变更监听 |

### 6.3 earliest() 与 initial() 的区别

- **initial() 模式**：先执行全量数据快照（读取表中当前所有数据，op="r"），然后监听后续 binlog
- **earliest() 模式**：不执行全量快照，直接从最早的 binlog 位置开始读取历史 binlog 中的变更记录（op="c/u/d"）

**注意**：earliest() 模式读取的是历史 binlog 中的变更记录，而不是表中的当前数据快照。如果 binlog 已被清理或不存在，则不会输出历史数据。

### 6.4 完整代码

```java
package com.action.flinkcdcDataStream;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream_Earliest {

    public static void main(String[] args) throws Exception {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启CheckPoint

        //3.使用FlinkCDC构建MySQLSource
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("server02")
                .port(3306)
                .username("root")
                .password("PS666666")
                .databaseList("test")
                .tableList("test.t3") //在写表时，需要带上库名。如果什么都不写，则表示监控所有的表
                .startupOptions(StartupOptions.earliest())
                .serverTimeZone("UTC")  // MySQL 5.7.38 使用 UTC，需与 JVM 时区一致或显式指定
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

/*测试流程及效果如下*/
/*
1.在MySQL中创建测试表并插入数据
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

use test;
CREATE TABLE IF NOT EXISTS t3 (
    id VARCHAR(50),
    name VARCHAR(50)
);

-- 先插入一些历史数据（这些操作会被记录到 binlog 中）
INSERT INTO t3 VALUES('1001','zhangsan');
INSERT INTO t3 VALUES('1002','lisi');
INSERT INTO t3 VALUES('1003','wangwu');

-- 执行一些历史更新和删除操作（这些操作也会被记录到 binlog 中）
UPDATE t3 SET name='lisi-updated' WHERE id='1002';
DELETE FROM t3 WHERE id='1001';
*/



/*
2.程序启动后，因指定了StartupOptions.earliest()启动模式，Flink CDC 会从MySQL binlog 的最早位置开始读取，

2.1 这意味着会读取所有可用的历史 binlog 数据（如果 binlog 文件还存在且未被清理）。
            与 initial() 模式的区别：
                - initial() 模式：先执行全量数据快照（读取表中当前所有数据，op="r"），然后监听后续 binlog
                - earliest() 模式：不执行全量快照，直接从最早的 binlog 位置开始读取历史 binlog 中的变更记录（op="c/u/d"）

            与 latest() 模式的区别：
                - latest() 模式：从最新的 binlog 位置开始监听，不读取历史 binlog 数据
                - earliest() 模式：从最早的 binlog 位置开始读取，会读取所有历史 binlog 数据

            注意：earliest() 模式读取的是历史 binlog 中的变更记录，而不是表中的当前数据快照。
            如果 binlog 已被清理或不存在，则不会输出历史数据。

2.2 程序启动后，Flink CDC 会按照 binlog 的时间顺序输出历史变更记录，然后继续监听后续的实时变更
-历史插入记录（从 binlog 中读取）:   INSERT INTO t3 VALUES('1001','zhangsan');
-历史插入记录（从 binlog 中读取）:   INSERT INTO t3 VALUES('1002','lisi');
-历史插入记录（从 binlog 中读取）:   INSERT INTO t3 VALUES('1003','wangwu');
-历史更新记录（从 binlog 中读取）:   UPDATE t3 SET name='lisi-updated' WHERE id='1002';
-历史删除记录（从 binlog 中读取）:   DELETE FROM t3 WHERE id='1001';


{"before":null,"after":{"id":"1001","name":"zhangsan"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770307453000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":2613,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180639,"transaction":null}
{"before":null,"after":{"id":"1002","name":"lisi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770307453000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":2881,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180640,"transaction":null}
{"before":null,"after":{"id":"1003","name":"wangwu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770307454000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":3145,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180640,"transaction":null}
{"before":null,"after":{"id":"1001","name":"zhangsan"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770307636000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":3411,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180641,"transaction":null}
{"before":null,"after":{"id":"1001","name":"zhangsan"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308129000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":4072,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180647,"transaction":null}
{"before":null,"after":{"id":"1002","name":"lisi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308130000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":4340,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180648,"transaction":null}
{"before":null,"after":{"id":"1003","name":"wangwu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308131000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":4604,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308180648,"transaction":null}
{"before":{"id":"1002","name":"lisi"},"after":{"id":"1002","name":"lisi-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308139000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":4870,"row":0,"thread":21,"query":null},"op":"u","ts_ms":1770308180649,"transaction":null}
{"before":{"id":"1001","name":"zhangsan"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308140000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":5154,"row":0,"thread":21,"query":null},"op":"d","ts_ms":1770308180649,"transaction":null}
*/


// 3.读取完历史 binlog 后，Flink CDC 继续监听MySQL 的 binlog 日志，实时捕获后续的 INSERT/UPDATE/DELETE 操作
/*
3.1 新增数据:   INSERT INTO t3 (id, name) VALUES ('1004', 'zhaoliu');
{"before":null,"after":{"id":"1004","name":"zhaoliu"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308331000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":5422,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308331743,"transaction":null}


3.2 新增数据:   INSERT INTO t3 (id, name) VALUES ('1005', 'sunqi');
{"before":null,"after":{"id":"1005","name":"sunqi"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308355000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":5689,"row":0,"thread":21,"query":null},"op":"c","ts_ms":1770308355387,"transaction":null}


3.3 更新数据:   UPDATE t3 SET name='wangwu-updated' WHERE id='1003';
{"before":{"id":"1003","name":"wangwu"},"after":{"id":"1003","name":"wangwu-updated"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308375000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":5954,"row":0,"thread":21,"query":null},"op":"u","ts_ms":1770308376068,"transaction":null}



3.4 删除数据:   DELETE FROM t3 WHERE id='1005';
{"before":{"id":"1005","name":"sunqi"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1770308399000,"snapshot":"false","db":"test","sequence":null,"table":"t3","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":6242,"row":0,"thread":21,"query":null},"op":"d","ts_ms":1770308400021,"transaction":null}
*/


/*4.AI总结
Flink CDC 通过捕获MySQL 的 binlog 日志，实现了对MySQL 数据库变更的实时监听和处理。
它能够捕获INSERT、UPDATE 和 DELETE 等操作，并将变更数据以Debezium 标准 JSON 格式输出，方便后续的数据处理和分析工作。
Flink CDC 的这种能力使得它在实时数据集成和流处理场景中非常有用，能够帮助企业实现对数据库变更的实时响应和处理。

一、earliest() 启动模式的特点
- 从最早 binlog 位置读取：程序启动后会从 binlog 的最早可用位置开始读取
- 读取历史 binlog 数据：会读取所有可用的历史 binlog 中的变更记录（如果 binlog 文件还存在）
- 不执行全量快照：与 initial() 模式不同，不会执行全量数据快照，而是通过 binlog 重建历史变更
- 完整历史追溯：能够追溯所有历史变更操作，包括程序启动前的数据变更
- 启动时间较长：如果历史 binlog 数据量很大，启动时间会较长
- 适用场景：适用于需要完整历史变更追溯，且 binlog 保留时间较长的场景

二、三种启动模式的对比
+------------------+------------------+------------------+------------------+------------------+
| 启动模式         | 是否读取历史数据 | 数据来源         | 启动速度         | 适用场景         |
+------------------+------------------+------------------+------------------+------------------+
| initial()        | 是（全量快照）   | 表快照+binlog    | 较慢（需快照）   | 需要完整数据同步 |
+------------------+------------------+------------------+------------------+------------------+
| earliest()       | 是（历史binlog） | 历史binlog+实时   | 较慢（读历史）   | 需要历史变更追溯 |
+------------------+------------------+------------------+------------------+------------------+
| latest()         | 否（跳过快照）   | 实时binlog       | 较快（直接监听） | 只需实时变更监听 |
+------------------+------------------+------------------+------------------+------------------+

三、earliest() 与 initial() 的区别
- initial() 模式：
  * 先执行全量数据快照，读取表中当前所有数据（op="r"）
  * 然后监听后续 binlog 变更（op="c/u/d"）
  * 快照数据不包含历史变更信息，只反映当前表状态
  
- earliest() 模式：
  * 不执行全量快照，直接从最早的 binlog 位置开始读取
  * 读取历史 binlog 中的变更记录（op="c/u/d"），按时间顺序输出
  * 然后继续监听后续实时 binlog 变更
  * 能够完整追溯所有历史变更操作

四、Debezium标准JSON核心结构
顶层固定字段：
- before：数据变更前的内容
- after：数据变更后的内容
- source：数据源元信息（含库表、Binlog、操作时间等）
- op：操作类型标识（c/u/d，注意：earliest() 模式下不会出现 r 类型，因为不执行全量快照）
- ts_ms：Flink CDC处理事件的时间戳
- transaction：事务信息（本次无事务，为null）

source元信息通用特征：
- 固定：db=test、table=t3（监控目标）、connector=mysql
- 所有操作：包含有效Binlog信息（server_id/file/pos）和MySQL实际操作时间戳
- earliest() 模式下：历史 binlog 记录和实时变更记录都包含有效的 Binlog 信息

五、不同操作类型的JSON字段规律（earliest() 模式）
+------------+--------------+--------------+----------------------------------+
| 操作类型   | before       | after        | source 特征                        |
+------------+--------------+--------------+----------------------------------+
| 插入(c)    | null         | 新插入数据   | 含有效Binlog信息和操作时间戳          |
+------------+--------------+--------------+----------------------------------+
| 更新(u)    | 修改前旧数据 | 修改后新数据 | 含有效Binlog信息和操作时间戳           |
+------------+--------------+--------------+----------------------------------+
| 删除(d)    | 删除前旧数据 | null         | 含有效Binlog信息和操作时间戳         |
+------------+--------------+--------------+----------------------------------+

注意：
1. earliest() 模式下不会出现 op="r"（全量快照读取）类型的操作，因为不执行全量快照
2. 历史 binlog 记录和实时变更记录的 JSON 格式完全一致，都包含有效的 Binlog 信息
3. 历史记录按照 binlog 的时间顺序输出，然后继续输出实时变更记录
4. 如果 binlog 已被清理或不存在，则不会输出历史数据，只输出后续实时变更

所有操作的JSON均清晰区分业务数据与元信息，字段含义明确、规律统一，便于解析和后续业务处理。
*/
```

### 6.5 MySQL 测试准备

```sql
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- 切换到目标数据库
USE test;

-- 创建表 t3（IF NOT EXISTS 避免重复创建）
CREATE TABLE IF NOT EXISTS t3 (
    id VARCHAR(50),
    name VARCHAR(50)
);

-- 先插入一些历史数据（这些操作会被记录到 binlog 中）
INSERT INTO t3 (id, name) VALUES 
    ('1001', 'zhangsan'),
    ('1002', 'lisi'),
    ('1003', 'wangwu');

-- 执行一些历史更新操作（会被记录到 binlog 中）
UPDATE t3 
SET name = 'lisi-updated' 
WHERE id = '1002';

-- 执行删除操作（会被记录到 binlog 中）
DELETE FROM t3 
WHERE id = '1001';
```

### 6.6 运行效果说明

程序启动后，Flink CDC 会从 MySQL binlog 的最早位置开始读取，按照 binlog 的时间顺序输出历史变更记录（INSERT/UPDATE/DELETE
对应的 op="c/u/d"），然后继续监听后续的实时变更。

**注意**：earliest() 模式下不会出现 op="r"（全量快照读取）类型的操作。

---

## 7.案例4：FlinkCDCDataStreamTest（带 Checkpoint 的完整示例）

### 7.1 功能说明

`FlinkCDCDataStreamTest` 是带 Checkpoint 配置的完整示例，演示如何实现**断点续传**：

- **Checkpoint**：将 Flink CDC 读取 binlog 的位置信息以状态方式保存在 Checkpoint 中，任务重启时可从上次位点继续消费
- **启动模式**：使用 `StartupOptions.initial()`，首次启动先全量快照再跟 binlog
- **状态后端**：使用 `HashMapStateBackend`，Checkpoint 存储到 HDFS（`hdfs://server01:9000/flinkCDC/ck`）
- **断点续传**：需从 Checkpoint 或 Savepoint 启动程序才能实现断点续传

### 7.2 完整代码

```java
package com.action.flinkcdcDataStream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 2.2 DataStream方式的应用 - Flink CDC 从 MySQL 读取 binlog 变更数据。
 * 文档：README 2.2.2 编写代码
 */
public class FlinkCDCDataStreamTest {

    public static void main(String[] args) throws Exception {
        // TODO 1. 准备流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2. 开启检查点   Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,
        // 需要从Checkpoint或者Savepoint启动程序
        // 2.1 开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置超时时间为 1 分钟
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 2.3 设置两次重启的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 2.4 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.5 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1L), Time.minutes(1L)
        ));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://server01:9000/flinkCDC/ck");
        // 2.7 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "vagrant");

        // TODO 3. 创建 Flink-MySQL-CDC 的 Source
        // initial:Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        // earliest:Never to perform snapshot on the monitored database tables upon first startup, just read from the beginning of the binlog. This should be used with care, as it is only valid when the binlog is guaranteed to contain the entire history of the database.
        // latest:Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        // specificOffset:Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        // timestamp:Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp.The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("server01")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.t1") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // TODO 4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MysqlSource");

        // TODO 5.打印输出
        mysqlDS.print();

        // TODO 6.执行任务
        env.execute();
    }
}
```

### 7.3 前置依赖

- 需先启动 **HDFS** 与 **Flink 集群**
- MySQL 主机、账号、密码需与代码中的 `hostname`、`username`、`password` 一致（本示例中 hostname 为 `server01`，可根据实际修改）

---

## 8.构建与提交

### 8.1 构建

在项目根目录或本模块目录下执行：

```bash
mvn clean package -DskipTests
```

将生成两个 JAR：

- `cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT.jar` — 瘦包
- `cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar` — 胖包（含 Flink CDC 等依赖，用于提交）

### 8.2 上传 JAR 到 Linux

```bash
# Windows 上传到 Linux（请按实际路径修改私钥和 JAR 路径）
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "D:\github\flink-cdc\cdc-01-FlinkCDC-DataStream\target\cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar" vagrant@192.168.56.11:/home/vagrant/
```

### 8.3 提交任务

**重要**：必须先启动 Flink 集群，再执行 `flink run`。

```bash
# 1. 启动 Flink 集群
cd $FLINK_HOME
bin/start-cluster.sh

# 2. 提交对应案例（主类名按需替换）
# 案例1 Initial
bin/flink run -c com.action.flinkcdcDataStream.FlinkCDC_DataStream_Initial /home/vagrant/cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar

# 案例2 Latest
bin/flink run -c com.action.flinkcdcDataStream.FlinkCDC_DataStream_Latest /home/vagrant/cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar

# 案例3 Earliest
bin/flink run -c com.action.flinkcdcDataStream.FlinkCDC_DataStream_Earliest /home/vagrant/cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar

# 案例4 Checkpoint（需 HDFS 已启动）
bin/flink run -c com.action.flinkcdcDataStream.FlinkCDCDataStreamTest /home/vagrant/cdc-01-FlinkCDC-DataStream-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 8.4 Web 控制台

- 本地访问：`http://localhost:8081`
- 远程访问：`http://192.168.56.11:8081`（按实际 IP 修改）

---

## 9.pom.xml 依赖说明

本模块继承父工程 `flink-cdc`，核心依赖如下：

| 依赖                        | 说明                   |
|---------------------------|----------------------|
| flink-java                | Flink 核心 API         |
| flink-streaming-java      | Flink DataStream API |
| flink-clients             | Flink 客户端（提交任务）      |
| flink-connector-base      | Flink 连接器基础          |
| flink-connector-mysql-cdc | Flink CDC MySQL 连接器  |
| mysql-connector-java      | MySQL JDBC 驱动        |

构建使用 `maven-assembly-plugin` 打包为 fat jar，便于独立部署。
