package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 开窗聚合示例
 * 演示如何使用 OVER 窗口进行开窗聚合
 * <p>
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用 OVER 窗口对每行数据计算聚合值
 * 3. 开窗聚合是"多对多"的关系，每行数据都会得到一个聚合结果
 * 4. 支持基于时间范围和行数的开窗范围
 */
public class OverWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 读取数据源，并分配时间戳、生成水位线 ====================
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // ==================== 2. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 3. 将数据流转换成表，并指定时间属性 ====================
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 执行开窗聚合查询 ====================
        // 使用 OVER 窗口对每行数据计算聚合值
        // PARTITION BY user：按用户分组
        // ORDER BY ts：按时间属性排序（必须）
        // RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW：开窗范围为当前行之前 1 小时
        Table result = tableEnv.sqlQuery(
                "SELECT user, ts, " +
                        "COUNT(url) OVER (" +
                        "PARTITION BY user " +
                        "ORDER BY ts " +
                        "RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW" +
                        ") AS cnt " +
                        "FROM EventTable"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        // 开窗聚合是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

