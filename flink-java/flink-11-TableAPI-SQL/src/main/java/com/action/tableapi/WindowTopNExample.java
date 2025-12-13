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
 * 窗口 Top N 示例
 * 演示如何结合窗口聚合和 OVER 窗口实现窗口 Top N 查询
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用滚动窗口进行窗口聚合，统计每个用户在每个窗口内的访问次数
 * 3. 使用 OVER 窗口和 ROW_NUMBER() 函数对窗口聚合结果进行 Top N 排序
 * 4. 窗口 Top N 是追加查询，结果表只有 INSERT 操作
 */
public class WindowTopNExample {
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

        // ==================== 5. 定义子查询，进行窗口聚合 ====================
        // 基于 ts 时间字段定义 1 小时滚动窗口，统计每个用户的访问次数
        // 提取窗口信息 window_start 和 window_end，方便后续排序
        String subQuery =
                "SELECT window_start, window_end, user, COUNT(url) as cnt " +
                        "FROM TABLE ( " +
                        "TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
                        "GROUP BY window_start, window_end, user ";

        // ==================== 6. 定义 Top N 的外层查询 ====================
        // 对窗口聚合的结果表中每一行数据进行 OVER 聚合统计行号
        // 以窗口信息进行分组，按访问次数 cnt 进行排序
        // 筛选行号小于等于 2 的数据，得到每个窗口内访问次数最多的前两个用户
        String topNQuery =
                "SELECT * " +
                        "FROM (" +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER ( " +
                        "PARTITION BY window_start, window_end " +
                        "ORDER BY cnt desc " +
                        ") AS row_num " +
                        "FROM (" + subQuery + ")) " +
                        "WHERE row_num <= 2";

        // ==================== 7. 执行 SQL 得到结果表 ====================
        Table result = tableEnv.sqlQuery(topNQuery);

        // ==================== 8. 将结果表转换成数据流并打印输出 ====================
        // 窗口 Top N 是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 9. 执行程序 ====================
        env.execute();
    }
}

