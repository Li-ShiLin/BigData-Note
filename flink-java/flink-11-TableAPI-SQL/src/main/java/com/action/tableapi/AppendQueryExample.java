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
 * 追加查询示例：窗口聚合
 * 演示如何使用窗口表值函数（Windowing TVF）进行窗口聚合
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用滚动窗口（TUMBLE）进行窗口聚合
 * 3. 统计每个用户在每个窗口内的访问次数
 * 4. 窗口聚合是追加查询，结果表只有 INSERT 操作
 */
public class AppendQueryExample {
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
        // 使用 .rowtime() 将 timestamp 字段指定为事件时间属性，并重命名为 ts
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")  // 将timestamp指定为事件时间，并命名为 ts
        );

        // ==================== 4. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("EventTable", eventTable);

        // ==================== 5. 设置1小时滚动窗口，执行 SQL统计查询 ====================
        // 使用窗口表值函数 TUMBLE() 定义滚动窗口
        // DESCRIPTOR(ts) 指定时间属性字段
        // INTERVAL '1' HOUR 指定窗口大小为 1 小时
        // GROUP BY 中需要包含 window_start 和 window_end，这是窗口 TVF 自动添加的字段
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +     // 窗口结束时间
                                "COUNT(url) AS cnt " +    // 统计url访问次数
                                "FROM TABLE( " +
                                "TUMBLE( TABLE EventTable, " +     // 1小时滚动窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY user, window_start, window_end "
                );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        // 窗口聚合是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

