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
