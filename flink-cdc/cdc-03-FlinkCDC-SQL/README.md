<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
- [2.项目结构](#2%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
- [3.功能说明](#3%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
- [4.完整代码](#4%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
- [5.MySQL 测试准备](#5mysql-%E6%B5%8B%E8%AF%95%E5%87%86%E5%A4%87)
- [6.运行效果说明](#6%E8%BF%90%E8%A1%8C%E6%95%88%E6%9E%9C%E8%AF%B4%E6%98%8E)
    - [6.1 全量快照阶段](#61-%E5%85%A8%E9%87%8F%E5%BF%AB%E7%85%A7%E9%98%B6%E6%AE%B5)
    - [6.2 增量监听阶段](#62-%E5%A2%9E%E9%87%8F%E7%9B%91%E5%90%AC%E9%98%B6%E6%AE%B5)
    - [6.3 启动模式说明](#63-%E5%90%AF%E5%8A%A8%E6%A8%A1%E5%BC%8F%E8%AF%B4%E6%98%8E)
- [7.SQL 方式与 DataStream 方式对比](#7sql-%E6%96%B9%E5%BC%8F%E4%B8%8E-datastream-%E6%96%B9%E5%BC%8F%E5%AF%B9%E6%AF%94)
- [8.构建与提交](#8%E6%9E%84%E5%BB%BA%E4%B8%8E%E6%8F%90%E4%BA%A4)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

基于 **Flink CDC SQL** 的 MySQL 变更数据捕获示例。使用声明式 `CREATE TABLE ... WITH ('connector' = 'mysql-cdc', ...)`
建表，以表格形式输出 CDC 数据。

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8                                     |
| MySQL | 已安装并开启 binlog（ROW 格式）                   |
| Flink | 1.13+ / 1.14+ 集群                        |
| Maven | 3.x                                     |

**节点约定**：server02（MySQL）、server01（Flink JobManager、任务提交）

---

## 2.项目结构

```
cdc-03-FlinkCDC-SQL/
├── pom.xml
├── README.md
└── src/main/java/com/action/flinkcdc/
    └── FLINKCDC_SQL_INITIAL.java
```

---

## 3.功能说明

- **FlinkCDC SQL**：使用 `CREATE TABLE ... WITH ('connector' = 'mysql-cdc', ...)` 声明式建表
- **启动模式**：`scan.startup.mode = 'initial'`，先全量快照再跟 binlog
- **输出格式**：表格形式（+I/-U/+U/-D），非 Debezium JSON
- **无 Checkpoint**：本示例未显式开启 Checkpoint，适合简单演示

---

## 4.完整代码

```java
package com.action.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FLINKCDC_SQL_INITIAL {

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
                " 'table-name' = 't1',\n" +
                " 'server-time-zone' = 'UTC',\n" +
                " 'scan.startup.mode' = 'initial'\n" +
                ")");

        //3.查询并打印
        Table table = tableEnv.sqlQuery("select * from t1");
        table.execute().print();

    }
}
```

---

## 5.MySQL 测试准备

```sql
CREATE DATABASE IF NOT EXISTS test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE test;
DROP TABLE IF EXISTS t1;
CREATE TABLE IF NOT EXISTS t1 (
  id   VARCHAR(255) NOT NULL COMMENT '主键',
  name VARCHAR(255) DEFAULT NULL COMMENT '姓名',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='表t1';

INSERT INTO t1 VALUES('1001','zhangsan');
INSERT INTO t1 VALUES('1002','lisi');
INSERT INTO t1 VALUES('1003','wangwu');
INSERT INTO t1 VALUES('1004','sun4');
UPDATE t1 SET name='wangwu-updated' WHERE id='1003';
DELETE FROM t1 WHERE id='1004';
```

---

## 6.运行效果说明

### 6.1 全量快照阶段

因指定 `scan.startup.mode = 'initial'`，程序启动时先对 MySQL 的 `test.t1` 执行全量快照，以表格形式输出当前表中的 3
条数据（id:1001/1002/1003）：

```
+----+--------------------------------+--------------------------------+
| op |                             id |                           name |
+----+--------------------------------+--------------------------------+
| +I |                           1001 |                       zhangsan |
| +I |                           1003 |                 wangwu-updated |
| +I |                           1002 |                           lisi |
+----+--------------------------------+--------------------------------+
```

### 6.2 增量监听阶段

全量快照完成后，继续监听 binlog。对表执行 INSERT/UPDATE/DELETE 时，以表格形式实时输出：

- 新增：`+I`
- 更新：`-U`（旧值）+ `+U`（新值）
- 删除：`-D`

### 6.3 启动模式说明

| 参数值               | 含义                       |
|-------------------|--------------------------|
| `initial`         | 先全量快照，再跟 binlog（本示例）     |
| `earliest-offset` | 从最早 binlog 位置开始读取        |
| `latest-offset`   | 从最新 binlog 位置开始（默认，不读历史） |

---

## 7.SQL 方式与 DataStream 方式对比

| 对比项   | SQL 方式                          | DataStream 方式              |
|-------|---------------------------------|----------------------------|
| 启动模式  | `scan.startup.mode` = 'initial' | `StartupOptions.initial()` |
| 输出格式  | 表格（+I/-U/+U/-D）                 | Debezium JSON              |
| 全量快照  | 输出当前表数据                         | before=null, after=数据      |
| 代码简洁度 | 声明式，更简洁                         | 相对复杂                       |

---

## 8.构建与提交

构建

```bash
mvn clean package -DskipTests
```

生成：`cdc-03-FlinkCDC-SQL-1.0-SNAPSHOT-jar-with-dependencies.jar`（胖包）

提交

```bash
cd $FLINK_HOME
bin/flink run -c com.action.flinkcdc.FLINKCDC_SQL_INITIAL /path/to/cdc-03-FlinkCDC-SQL-1.0-SNAPSHOT-jar-with-dependencies.jar
```
