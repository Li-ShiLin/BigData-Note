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