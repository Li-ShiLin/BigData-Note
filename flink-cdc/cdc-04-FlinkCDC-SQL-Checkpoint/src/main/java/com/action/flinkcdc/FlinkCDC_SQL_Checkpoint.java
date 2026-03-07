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
