package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 和 SQL 快速上手示例
 * 演示如何使用 Table API 和 SQL 进行数据处理
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 将数据流转换成表
 * 3. 使用 SQL 查询提取 url 和 user 字段
 * 4. 将结果表转换成数据流并打印输出
 */
public class TableExample {
    public static void main(String[] args) throws Exception {
        // ==================== 1. 获取流执行环境 ====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 2. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 3. 获取表环境 ====================
        // 创建流式表环境，用于在流处理场景中使用 Table API 和 SQL
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表 ====================
        // 将 DataStream 转换为 Table，表名会自动从 Event 类的字段中推断
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // ==================== 5. 使用 SQL 查询数据 ====================
        // 执行 SQL 查询，从表中提取 url 和 user 字段
        // 注意：这里可以直接在 SQL 中使用 Table 对象名，Flink 会自动注册为虚拟表
        Table visitTable = tableEnv.sqlQuery("SELECT url, user FROM " + eventTable);

        // ==================== 6. 将表转换成数据流并打印输出 ====================
        // 将查询结果表转换回 DataStream，并打印输出
        tableEnv.toDataStream(visitTable).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

