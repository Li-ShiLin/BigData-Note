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
 * 行间隔开窗聚合示例
 * 演示如何使用行间隔（ROW intervals）进行开窗聚合
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用 OVER 窗口，基于行数进行开窗聚合
 * 3. ROWS BETWEEN 5 PRECEDING AND CURRENT ROW 表示选择当前行之前的 5 行数据（包括当前行共 6 条）
 * 4. 行间隔开窗聚合适用于需要基于数据条数进行统计的场景
 */
public class RowIntervalOverWindowExample {
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

        // ==================== 5. 执行行间隔开窗聚合查询 ====================
        // 使用 WINDOW 子句在 SELECT 外部单独定义一个 OVER 窗口，并重命名为 w
        // 这样可以方便重复引用定义好的 OVER 窗口
        // ROWS BETWEEN 2 PRECEDING AND CURRENT ROW：选择当前行之前的 2 行数据（包括当前行共 3 条）
        Table result = tableEnv.sqlQuery(
                "SELECT user, ts, " +
                        "COUNT(url) OVER w AS cnt, " +
                        "MAX(CHAR_LENGTH(url)) OVER w AS max_url " +
                        "FROM EventTable " +
                        "WINDOW w AS (" +
                        "PARTITION BY user " +
                        "ORDER BY ts " +
                        "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
        );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

