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
