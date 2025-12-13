package com.action.tableapi;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 间隔联结查询示例
 * 演示如何使用间隔联结（Interval Join）进行基于时间的表连接
 * <p>
 * 功能说明：
 * 1. 创建两条数据流，分别代表订单流和点击流
 * 2. 使用间隔联结，将订单与它对应的点击行为连接起来
 * 3. 间隔联结只支持具有时间属性的"仅追加"表
 * 4. 时间间隔限制：订单时间在点击时间前后的一定时间范围内
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 创建第一条流：订单流 ====================
        // 订单流包含：用户、订单ID、订单时间
        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        // ==================== 3. 创建第二条流：点击流 ====================
        // 点击流包含：用户、URL、点击时间
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // ==================== 4. 将数据流转换成表，并指定时间属性 ====================
        // 对于 Tuple3 类型，字段名是 f0, f1, f2，需要使用字段名并添加别名
        Table orderTable = tableEnv.fromDataStream(orderStream,
                $("f0").as("user"), $("f1").as("orderId"), $("f2").rowtime().as("ot"));
        Table clickTable = tableEnv.fromDataStream(clickStream,
                $("user"), $("url"), $("timestamp").rowtime().as("ct"));

        // ==================== 5. 为方便在SQL中引用，在环境中注册表 ====================
        tableEnv.createTemporaryView("OrderTable", orderTable);
        tableEnv.createTemporaryView("ClickTable", clickTable);

        // ==================== 6. 执行间隔联结查询 ====================
        // 间隔联结不需要用 JOIN 关键字，直接在 FROM 后将要联结的两表列出来
        // 联结条件：o.`user` = c.`user`（等值条件）
        // 注意：user 是 Flink SQL 的保留关键字，需要使用反引号转义
        // 时间间隔限制：o.ot BETWEEN c.ct - INTERVAL '5' SECOND AND c.ct + INTERVAL '10' SECOND
        // 表示订单时间在点击时间前后 [-5s, +10s] 的范围内
        // 注意：将 ct 转换为 TIMESTAMP，避免结果表中有多个 rowtime 字段
        // 当转换为 DataStream 时，只能有一个 rowtime 字段作为事件时间戳
        String sql = "SELECT c.`user`, c.url, o.orderId, o.ot, CAST(c.ct AS TIMESTAMP) AS ct " +
                "FROM OrderTable o, ClickTable c " +
                "WHERE o.`user` = c.`user` " +
                "AND o.ot BETWEEN c.ct - INTERVAL '5' SECOND AND c.ct + INTERVAL '10' SECOND";
        Table result = tableEnv.sqlQuery(sql);

        // ==================== 7. 将结果表转换成数据流并打印输出 ====================
        // 间隔联结是追加查询，可以直接使用 toDataStream()
        tableEnv.toDataStream(result).print();

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}

