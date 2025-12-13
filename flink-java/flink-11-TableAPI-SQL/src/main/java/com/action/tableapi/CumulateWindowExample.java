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
 * 累积窗口示例
 * 演示如何使用累积窗口（CUMULATE）进行周期性统计
 *
 * 功能说明：
 * 1. 定义事件时间属性
 * 2. 使用累积窗口（CUMULATE）进行统计
 * 3. 累积窗口会在统计周期内进行累积计算，每隔一段时间输出一次当前累积值
 * 4. 例如：按天统计 PV，但每隔 1 小时输出一次当天到目前为止的 PV 值
 */
public class CumulateWindowExample {
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

        // ==================== 5. 设置累积窗口，执行 SQL统计查询 ====================
        // CUMULATE() 函数定义累积窗口
        // 第三个参数为累积步长（step）：INTERVAL '30' MINUTE，表示每 30 分钟输出一次
        // 第四个参数为最大窗口长度（max window size）：INTERVAL '1' HOUR，表示统计周期为 1 小时
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " +
                                "COUNT(url) AS cnt " +
                                "FROM TABLE( " +
                                "CUMULATE( TABLE EventTable, " +     // 定义累积窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '30' MINUTE, " +  // 累积步长：30 分钟
                                "INTERVAL '1' HOUR)) " +    // 最大窗口长度：1 小时
                                "GROUP BY user, window_start, window_end "
                );

        // ==================== 6. 将结果表转换成数据流并打印输出 ====================
        tableEnv.toDataStream(result).print();

        // ==================== 7. 执行程序 ====================
        env.execute();
    }
}

