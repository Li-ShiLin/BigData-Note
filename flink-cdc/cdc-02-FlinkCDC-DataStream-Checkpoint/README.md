<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [1.环境与前置条件](#1%E7%8E%AF%E5%A2%83%E4%B8%8E%E5%89%8D%E7%BD%AE%E6%9D%A1%E4%BB%B6)
- [2.项目结构](#2%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
- [3.功能说明](#3%E5%8A%9F%E8%83%BD%E8%AF%B4%E6%98%8E)
- [4.完整代码](#4%E5%AE%8C%E6%95%B4%E4%BB%A3%E7%A0%81)
- [5.MySQL 测试准备](#5mysql-%E6%B5%8B%E8%AF%95%E5%87%86%E5%A4%87)
- [6.HDFS 与 Flink 启动](#6hdfs-%E4%B8%8E-flink-%E5%90%AF%E5%8A%A8)
    - [6.1 启动 HDFS](#61-%E5%90%AF%E5%8A%A8-hdfs)
    - [6.2 启动 Flink 集群](#62-%E5%90%AF%E5%8A%A8-flink-%E9%9B%86%E7%BE%A4)
- [7.构建与提交](#7%E6%9E%84%E5%BB%BA%E4%B8%8E%E6%8F%90%E4%BA%A4)
- [8.Web 控制台与故障排查](#8web-%E6%8E%A7%E5%88%B6%E5%8F%B0%E4%B8%8E%E6%95%85%E9%9A%9C%E6%8E%92%E6%9F%A5)
    - [8.1 Web 控制台](#81-web-%E6%8E%A7%E5%88%B6%E5%8F%B0)
    - [8.2 Checkpoint 报错](#82-checkpoint-%E6%8A%A5%E9%94%99)
- [9.从 Checkpoint / Savepoint 恢复（断点续传）](#9%E4%BB%8E-checkpoint--savepoint-%E6%81%A2%E5%A4%8D%E6%96%AD%E7%82%B9%E7%BB%AD%E4%BC%A0)
    - [9.1 方式一：Savepoint（推荐）](#91-%E6%96%B9%E5%BC%8F%E4%B8%80savepoint%E6%8E%A8%E8%8D%90)
    - [9.2 方式二：从 Checkpoint 恢复](#92-%E6%96%B9%E5%BC%8F%E4%BA%8C%E4%BB%8E-checkpoint-%E6%81%A2%E5%A4%8D)
    - [9.3 Savepoint 与 Checkpoint 对比](#93-savepoint-%E4%B8%8E-checkpoint-%E5%AF%B9%E6%AF%94)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

基于 **Flink CDC DataStream API** 的有状态、可断点续传示例。通过开启 Checkpoint，将 CDC 读取位点等状态持久化到
HDFS，任务重启时可从上次 binlog 位点继续消费。

---

## 1.环境与前置条件

| 组件    | 说明                                      |
|-------|-----------------------------------------|
| 操作系统  | Linux（CentOS 7+ / Ubuntu 16.04+），x86_64 |
| JDK   | 1.8                                     |
| MySQL | 已安装并开启 binlog（ROW 格式）                   |
| HDFS  | 用于 Checkpoint 存储，fs.defaultFS 需与代码中端口一致 |
| Flink | 1.14+ 集群                                |
| Maven | 3.x                                     |

**节点约定**：server01（Flink JobManager、HDFS NameNode）、server02（MySQL）

---

## 2.项目结构

```
cdc-02-FlinkCDC-DataStream-Checkpoint/
├── pom.xml
├── README.md
└── src/main/java/com/action/flinkcdc/
    └── FlinkCDC_DataStream_Initial_Checkpoint.java
```

---

## 3.功能说明

- **DataStream + Checkpoint**：使用 MySqlSource，开启 Checkpoint，将 binlog 位点保存到 HDFS
- **启动模式**：`StartupOptions.initial()`，先全量快照再跟 binlog
- **断点续传**：从 Checkpoint 或 Savepoint 恢复启动

---

## 4.完整代码

```java
package com.action.flinkcdc;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

public class FlinkCDC_DataStream_Initial_Checkpoint {

    public static void main(String[] args) throws Exception {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.开启 Checkpoint：Flink-CDC 将读取 binlog 的位置信息以状态方式保存在 CK 中，断点续传需从 Checkpoint 或 Savepoint 恢复启动
        env.enableCheckpointing(5000L);   // 每 5000ms 触发一次 Checkpoint，定期持久化状态（含 CDC binlog 位点）
        env.getCheckpointConfig().setCheckpointTimeout(10000L);   // 单次 Checkpoint 超时时间 10 秒，超时未完成则本次 CK 失败
        env.getCheckpointConfig().setCheckpointStorage("hdfs://server01:9000/flinkCDC/ck");   // CK 存储路径：端口需与 core-site.xml 中 fs.defaultFS 一致（当前为 9000）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);   // 精确一次语义，保证每条记录只被处理一次
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);   // 同一时刻最多 1 个 CK 进行，避免多 CK 并发加重 I/O
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);   // 任务取消时保留最后一次 CK，便于从该 CK 恢复

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
```

---

## 5.MySQL 测试准备

```sql
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE
DATABASE IF NOT EXISTS test
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- 切换到目标数据库
USE
test;

-- 先删除旧表（如有），避免表结构冲突
DROP TABLE IF EXISTS t1;

-- 创建表 t1（InnoDB引擎，utf8mb4编码，主键约束+字段注释）
CREATE TABLE IF NOT EXISTS t1
(
    id
    VARCHAR
(
    255
) NOT NULL COMMENT '主键',
    name VARCHAR
(
    255
) DEFAULT NULL COMMENT '姓名',
    PRIMARY KEY
(
    id
)
    ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_unicode_ci
    COMMENT = '表t1';

-- 批量插入初始数据（显式指定字段名，提升代码健壮性）
INSERT INTO t1 (id, name)
VALUES ('1001', 'zhangsan'),
       ('1002', 'lisi'),
       ('1003', 'wangwu'),
       ('1004', 'sun4');

-- 更新指定记录
UPDATE t1
SET name = 'wangwu-updated'
WHERE id = '1003';

-- 删除指定记录
DELETE
FROM t1
WHERE id = '1004';
```

---

## 6.HDFS 与 Flink 启动

### 6.1 启动 HDFS

```bash
start-dfs.sh && start-yarn.sh
jps  # 预期：NameNode、SecondaryNameNode、DataNode、ResourceManager、NodeManager
hdfs dfsadmin -report
```

```bash
# 1.启动 HDFS（分布式文件系统）
# 启动HDFS（包含NameNode、DataNode、SecondaryNameNode）
start-dfs.sh && start-yarn.sh

# 验证HDFS进程（server01执行jps）
jps
# 预期进程：NameNode、SecondaryNameNode、Jps

# 检查HDFS状态
hdfs dfsadmin -report
# 检查Web UI（如果已启用）
# http://server01:50070

# 验证从节点进程（server02、server03执行jps）
ssh server02 jps  # 预期进程：DataNode、Jps
ssh server03 jps  # 预期进程：DataNode、Jps
# 或者
ssh server02 "source /etc/profile && jps"
ssh server03 "source /etc/profile && jps"


# 2.启动 YARN（资源管理器）
# 启动YARN（包含ResourceManager、NodeManager）
start-yarn.sh
# 验证YARN进程（server01执行jps）
jps
# 预期进程：NameNode、SecondaryNameNode、ResourceManager、Jps
# 验证从节点进程（server02、server03执行jps）
ssh server02 jps  # 预期进程：DataNode、NodeManager、Jps
ssh server03 jps  # 预期进程：DataNode、NodeManager、Jps
# 或者
ssh server02 "source /etc/profile && jps"
ssh server03 "source /etc/profile && jps"


# 关闭命令
# 停止整个集群
stop-yarn.sh && stop-dfs.sh
# 再重新启动
start-dfs.sh && start-yarn.sh
```

### 6.2 启动 Flink 集群

```bash
cd $FLINK_HOME
bin/start-cluster.sh
jps  # 预期：StandaloneSessionClusterEntrypoint、TaskManagerRunner
```

**重要**：必须先启动 Flink 集群，再执行 `flink run`，否则会报 `Connection refused: localhost/127.0.0.1:8081`。

---

## 7.构建与提交

**构建**

```bash
mvn clean package -DskipTests
```

生成：`cdc-02-FlinkCDC-DataStream-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar`（胖包）

**上传 JAR**

```bash
# Windows 上传到 Linux
scp -i "E:\vm\vagrant\server01\.vagrant\machines\default\virtualbox\private_key" "D:\github\flink-cdc\cdc-02-FlinkCDC-DataStream-Checkpoint\target\cdc-02-FlinkCDC-DataStream-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar" vagrant@192.168.56.11:/home/vagrant/
```

**提交任务**

```bash
cd $FLINK_HOME
bin/flink run -c com.action.flinkcdc.FlinkCDC_DataStream_Initial_Checkpoint /home/vagrant/cdc-02-FlinkCDC-DataStream-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## 8.Web 控制台与故障排查

### 8.1 Web 控制台

- 本地：`http://localhost:8081`
- 远程：`http://192.168.56.11:8081`

### 8.2 Checkpoint 报错

若报 **`Failed to create checkpoint storage`** 或 **`Connection refused: server01:8020`**：

- **host** 应为 NameNode 所在节点（server01）
- **端口** 须与 `core-site.xml` 中 `fs.defaultFS` 一致：若为 `hdfs://server01:9000`，则 Checkpoint 用 **9000**，不能用
  8020。本工程已用 `hdfs://server01:9000/flinkCDC/ck`
- 若 HDFS 不可用，可临时改为 `file:///tmp/flinkCDC/ck` 测试

---

## 9.从 Checkpoint / Savepoint 恢复（断点续传）

### 9.1 方式一：Savepoint（推荐）

**查看作业 ID**

```bash
cd $FLINK_HOME
bin/flink list -r
```

或 Web UI → Running Jobs 查看 Job ID。

**创建 Savepoint（取消前执行）**

```bash
bin/flink savepoint <JobID> hdfs://server01:9000/flinkCDC/save
```

成功后记下输出的 Savepoint 路径。

**取消作业**

Web UI 中 Cancel Job，或：`bin/flink cancel <JobID>`

**从 Savepoint 恢复**

```bash
bin/flink run -s hdfs://server01:9000/flinkCDC/save/savepoint-xxx -c com.action.flinkcdc.FlinkCDC_DataStream_Initial_Checkpoint /home/vagrant/cdc-02-FlinkCDC-DataStream-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 9.2 方式二：从 Checkpoint 恢复

本工程已开启 `RETAIN_ON_CANCELLATION`，取消后最后一次 Checkpoint 会保留在 HDFS。

**取消作业**

`bin/flink cancel <JobID>`

**查找 Checkpoint 路径**

```bash
hdfs dfs -ls hdfs://server01:9000/flinkCDC/ck
hdfs dfs -ls hdfs://server01:9000/flinkCDC/ck/<JobID>
```

记下 `chk-<序号>` 的完整路径，例如：`hdfs://server01:9000/flinkCDC/ck/<JobID>/chk-42`。

**从 Checkpoint 恢复**

```bash
bin/flink run -s hdfs://server01:9000/flinkCDC/ck/<JobID>/chk-42 -c com.action.flinkcdc.FlinkCDC_DataStream_Initial_Checkpoint /home/vagrant/cdc-02-FlinkCDC-DataStream-Checkpoint-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### 9.3 Savepoint 与 Checkpoint 对比

| 对比项      | Checkpoint               | Savepoint                |
|----------|--------------------------|--------------------------|
| 触发方式     | 自动按间隔触发                  | 手动 `flink savepoint`     |
| 主要用途     | 故障恢复                     | 计划内停止与恢复                 |
| 生命周期     | 取消后默认删除；RETAIN 可保留最后一次   | 创建后持久保存                  |
| 恢复 -s 参数 | `.../ck/<JobID>/chk-xxx` | `.../save/savepoint-xxx` |
