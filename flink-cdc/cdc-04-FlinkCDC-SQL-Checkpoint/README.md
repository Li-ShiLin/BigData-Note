<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
- [2.项目结构](#2%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
- [3.功能说明](#3%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
- [4.完整代码](#4%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
- [5.构建与提交](#5%E6%9E%84%E5%BB%BA%E4%B8%8E%E6%8F%90%E4%BA%A4)
- [6.从 Checkpoint / Savepoint 恢复（断点续传）](#6%E4%BB%8E-checkpoint--savepoint-%E6%81%A2%E5%A4%8D%E6%96%AD%E7%82%B9%E7%BB%AD%E4%BC%A0)
    - [6.1 Savepoint 恢复](#61-savepoint-%E6%81%A2%E5%A4%8D)
    - [6.2 Checkpoint 恢复](#62-checkpoint-%E6%81%A2%E5%A4%8D)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

基于 **FlinkCDC SQL** 的有状态、可断点续传示例。通过开启 Checkpoint，将 CDC 读取位点等状态持久化到 HDFS，任务重启时可从上次位点继续消费。

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8                                     |
| MySQL | 已安装并开启 binlog（ROW 格式）                   |
| HDFS  | 用于 Checkpoint 存储，fs.defaultFS 需与代码中端口一致 |
| Flink | 1.13+ / 1.14+ 集群                        |
| Maven | 3.x                                     |

**节点约定**：server01（Flink JobManager、HDFS NameNode）、server02（MySQL）

---

## 2.项目结构

```
cdc-04-FlinkCDC-SQL-Checkpoint/
├── pom.xml
├── README.md
└── src/main/java/com/action/flinkcdc/
    └── FlinkCDC_SQL_Checkpoint.java
```

---

## 3.功能说明

- **FlinkCDC SQL**：使用 `CREATE TABLE ... WITH ('connector' = 'mysql-cdc', ...)` 声明式建表
- **启动模式**：`scan.startup.mode = 'initial'`，先全量快照再跟 binlog
- **Checkpoint**：每 5 秒触发，存 HDFS（`hdfs://server01:9000/flinkCDC/ck`），取消时保留最后一次，便于恢复

---

## 4.完整代码

```java
package com.action.flinkcdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于 FlinkCDC SQL 的有状态、可断点续传示例。
 * 通过开启 Checkpoint，将 CDC 读取位点等状态持久化到 HDFS，任务重启时可从上次位点继续消费。
 */
public class FlinkCDC_SQL_Checkpoint {

    public static void main(String[] args) throws Exception {

        // 1. 获取 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 开启 Checkpoint：CDC 位点等状态写入 CK，支持断点续传
        env.enableCheckpointing(5000L);   // 每 5 秒触发一次 Checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(10000L);   // 单次 CK 超时 10 秒
        env.getCheckpointConfig().setCheckpointStorage("hdfs://server01:9000/flinkCDC/ck");   // CK 存 HDFS，端口与 fs.defaultFS 一致
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);   // 精确一次
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);   // 同一时刻最多 1 个 CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);   // 取消时保留最后一次 CK，便于从该 CK 恢复

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. 使用 FlinkCDC SQL 方式建表（initial：先全量快照，再跟 binlog）
        tableEnv.executeSql("" +
                "CREATE TABLE t1 (\n" +
                "    id STRING,\n" +
                "    name STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'server02',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'PS666666',\n" +
                "    'database-name' = 'test',\n" +
                "    'table-name' = 't1',\n" +
                "    'server-time-zone' = 'UTC',\n" +
                "    'scan.startup.mode' = 'initial'\n" +
                ")");

        // 4. 查询并打印（流式结果，会持续输出）
        Table table = tableEnv.sqlQuery("SELECT * FROM t1");
        table.execute().print();

        // 5. 启动（流式作业不会自动退出，需手动取消；从 Checkpoint/Savepoint 恢复时位点会续传）
        env.execute("FlinkCDC-SQL-Checkpoint");
    }
}
```

---

## 5.构建与提交

**构建**

```bash
mvn clean package -DskipTests
```

生成：`cdc-04-FlinkCDC-SQL-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar`（胖包）

**提交**

确保 HDFS、Flink 集群已启动后执行：

```bash
cd $FLINK_HOME
bin/flink run -c com.action.flinkcdc.FlinkCDC_SQL_Checkpoint /path/to/cdc-04-FlinkCDC-SQL-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## 6.从 Checkpoint / Savepoint 恢复（断点续传）

### 6.1 Savepoint 恢复

取消前执行：

```bash
bin/flink savepoint <JobID> hdfs://server01:9000/flinkCDC/save
```

恢复：

```bash
bin/flink run -s hdfs://server01:9000/flinkCDC/save/savepoint-xxx -c com.action.flinkcdc.FlinkCDC_SQL_Checkpoint /path/to/cdc-04-FlinkCDC-SQL-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 6.2 Checkpoint 恢复

已开启取消时保留，取消后在 HDFS 查找：

```
hdfs://server01:9000/flinkCDC/ck/<JobID>/chk-<序号>
```

恢复：

```bash
bin/flink run -s hdfs://server01:9000/flinkCDC/ck/<JobID>/chk-<序号> -c com.action.flinkcdc.FlinkCDC_SQL_Checkpoint /path/to/cdc-04-FlinkCDC-SQL-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```
