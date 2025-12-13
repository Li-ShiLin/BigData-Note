package com.action.tableapi;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 分组聚合示例
 * 演示如何使用 GROUP BY 进行分组聚合统计
 *
 * 功能说明：
 * 1. 从数据流中读取用户点击事件
 * 2. 按照用户进行分组
 * 3. 统计每个用户的点击次数
 * 4. 分组聚合是更新查询，结果表会有更新操作
 */
public class GroupAggregationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("EventTable", eventStream);

        // ==================== 4. 执行分组聚合查询 ====================
        // 按照 user 字段进行分组，统计每个用户的点击次数
        // COUNT(url) 统计每个分组中 url 的个数
        Table urlCountTable = tableEnv.sqlQuery(
                "SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user"
        );

        // ==================== 5. 将表转换成更新日志流并打印输出 ====================
        // 分组聚合是更新查询，结果表会有更新操作
        // 必须使用 toChangelogStream() 转换，记录更新日志
        tableEnv.toChangelogStream(urlCountTable).print("用户点击统计");

        // ==================== 6. 执行程序 ====================
        env.execute();
    }
}

