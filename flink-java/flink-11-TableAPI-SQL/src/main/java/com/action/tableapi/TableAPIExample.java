package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 方式查询示例
 * 演示如何使用 Table API 进行数据处理，与 SQL 方式等效
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 将数据流转换成表
 * 3. 使用 Table API 方式提取 url 和 user 字段
 * 4. 将结果表转换成数据流并打印输出
 */
public class TableAPIExample {
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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 4. 将数据流转换成表 ====================
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // ==================== 5. 使用 Table API 方式提取数据 ====================
        // 使用 Table API 的 select() 方法选择字段
        // $() 方法用于指定表中的字段，类似于 SQL 中的列名
        Table clickTable = eventTable.select($("url"), $("user"));

        // ==================== 6. 将表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(clickTable).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

