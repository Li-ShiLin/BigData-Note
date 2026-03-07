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

/*测试流程及效果如下*/
/*
1.在MySQL中创建测试表并插入数据
-- 建库：IF NOT EXISTS 避免重复创建，utf8mb4 支持完整 Unicode（含 emoji）
CREATE DATABASE IF NOT EXISTS test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE test;
DROP TABLE IF EXISTS t1;  -- 避免重复创建表
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

mysql> select * from t1;
+------+----------------+
| id   | name           |
+------+----------------+
| 1001 | zhangsan       |
| 1002 | lisi           |
| 1003 | wangwu-updated |
+------+----------------+
*/


/*
2.程序启动后，因指定了 scan.startup.mode = 'initial' 参数，Flink CDC SQL 使用 initial 模式
   initial 模式：先读取全量历史数据快照，然后继续监听 binlog 增量数据
   
   程序启动时会先对 MySQL 的 test 库 t1 表执行全量数据快照读取，将表中已存在的业务数据以表格形式输出：
   
   输出格式（表格形式，全量快照数据）：
+----+--------------------------------+--------------------------------+
| op |                             id |                           name |
+----+--------------------------------+--------------------------------+
| +I |                           1001 |                       zhangsan |
| +I |                           1003 |                 wangwu-updated |
| +I |                           1002 |                           lisi |
   
   注意：全量快照会读取当前表中的所有数据（3条：1001/1002/1003），已删除的数据（1004）不会出现
   全量快照完成后，程序会继续监听 binlog 日志，捕获后续的增量变更
   
   其他启动模式说明：
   'scan.startup.mode' = 'earliest-offset'  -- 从最早的 binlog 位置开始读取
   'scan.startup.mode' = 'latest-offset'  -- 从最新的 binlog 位置开始读取（默认值，不读取历史快照）
*/


/*
3.全量快照读取完成后，Flink CDC 继续监听MySQL 的 binlog 日志，当我们对 test 库 t1 表执行 INSERT/UPDATE/DELETE 操作时，
   Flink CDC 能够实时捕获这些变更，并以表格形式输出（与 DataStream 方式的 JSON 格式不同）

3.1 新增数据:   INSERT INTO t1 (id, name) VALUES ('1005', 'zhaoliu');
   输出格式（表格形式，增量数据）：

| +I |                           1005 |                        zhaoliu |



3.2 新增数据:   INSERT INTO t1 (id, name) VALUES ('1006', 'sunqi');
   输出格式（表格形式，增量数据）：

| +I |                           1006 |                          sunqi |



3.3 更新数据:   UPDATE t1 SET name='lisi-updated' WHERE id='1002';
   输出格式（表格形式，显示更新后的最新值）：

| -U |                           1002 |                           lisi |
| +U |                           1002 |                   lisi-updated |



3.4 更新数据:   UPDATE t1 SET name='wangwu' WHERE id='1003';
   输出格式（表格形式，增量更新）：

| -U |                           1003 |                 wangwu-updated |
| +U |                           1003 |                         wangwu |



3.5 删除数据:   DELETE FROM t1 WHERE id='1001';
输出格式：
| -D |                           1001 |                       zhangsan |
*/


/*4.SQL 方式与 DataStream 方式的对比（本示例使用 initial 模式）
+------------------+----------------------+----------------------+
| 对比项           | SQL 方式              | DataStream 方式       |
+------------------+----------------------+----------------------+
| 启动模式         | initial（已指定）     | initial（需显式指定） |
| 输出格式         | 表格形式              | Debezium JSON 格式    |
| 全量快照         | 输出当前表数据         | 输出 before=null,after=数据 |
| 删除操作         | 不输出（查询为空）     | 输出 before 数据      |
| 代码简洁度       | 更简洁（SQL 声明式）  | 相对复杂（API 调用）  |
| 灵活性           | 适合简单查询          | 适合复杂数据处理      |
+------------------+----------------------+----------------------+

5.启动模式说明：
   'scan.startup.mode' = 'initial'  -- 先读取全量快照，再读取增量数据（本示例使用）
   'scan.startup.mode' = 'earliest-offset'  -- 从最早的 binlog 位置开始读取
   'scan.startup.mode' = 'latest-offset'  -- 从最新的 binlog 位置开始读取（默认值，不读取历史快照）

6.总结
本示例使用 Flink CDC SQL 方式的 initial 启动模式，程序启动时会先读取 MySQL 表的全量历史数据快照，
然后继续监听 binlog 日志捕获增量变更。SQL 方式通过声明式的 SQL 语句，简化了 CDC 数据源的配置和使用。
与 DataStream 方式相比，SQL 方式更适合简单的数据查询和展示场景，代码更加简洁。
*/
