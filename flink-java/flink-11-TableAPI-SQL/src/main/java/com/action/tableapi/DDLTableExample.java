package com.action.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * DDL 创建表示例
 * 演示如何使用 DDL 语句创建表，连接外部系统
 * <p>
 * 功能说明：
 * 1. 使用 EnvironmentSettings 创建表环境
 * 2. 使用 CREATE TABLE DDL 语句创建输入表，连接到文件系统
 * 3. 使用 CREATE TABLE DDL 语句创建输出表，连接到控制台打印
 * 4. 执行 SQL 查询并将结果写入输出表
 */
public class DDLTableExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 定义环境配置来创建表环境 ====================
        // 基于 blink 版本 planner 进行流处理
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()     // 使用流处理模式
                .useBlinkPlanner()     // 使用 Blink planner
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // ==================== 2. 创建输入表，连接外部系统读取数据 ====================
        // 使用 CREATE TABLE DDL 语句创建表
        // WITH 子句指定连接器（connector）和相关配置
        // 这里连接到文件系统，读取 CSV 格式的数据
        String createInputDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' = 'csv', " +
                " 'csv.field-delimiter' = ',', " +
                " 'csv.ignore-parse-errors' = 'true' " +
                ")";

        tableEnv.executeSql(createInputDDL);

        // ==================== 3. 执行 SQL 对表进行查询转换 ====================
        // 查询用户 Bob 的点击事件，并提取 user_name 和 url 字段
        Table resultTable = tableEnv.sqlQuery(
                "SELECT user_name, url " +
                        "FROM clickTable " +
                        "WHERE user_name = 'Bob'"
        );

        // ==================== 4. 创建输出表，连接外部系统用于输出 ====================
        // 创建一张用于控制台打印输出的表
        // connector 为 'print' 表示输出到控制台
        String createOutputDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " url STRING " +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";

        tableEnv.executeSql(createOutputDDL);

        // ==================== 5. 将查询结果写入输出表 ====================
        // 使用 executeInsert() 方法将结果表写入已注册的输出表中
        TableResult tableResult = resultTable.executeInsert("printOutTable");

        // ==================== 6. 执行程序 ====================
        // 注意：使用 TableEnvironment 时，不需要调用 env.execute()
        // 直接调用 tableResult.getJobClient() 或等待作业完成即可
        // 这里为了演示，我们等待作业完成
        tableResult.getJobClient().ifPresent(jobClient -> {
            try {
                jobClient.getJobExecutionResult().get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

