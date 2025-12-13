package com.action.multistream;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 间隔联结（Interval Join）示例
 * 
 * 间隔联结针对一条流的每个数据，开辟出其时间戳前后的一段时间间隔
 * 看这期间是否有来自另一条流的数据匹配
 * 
 * 匹配条件：a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound
 * 
 * 本例：一条是下订单的流，一条是浏览数据的流
 * 针对同一个用户，使用一个用户的下订单事件和这个用户最近十分钟的浏览数据做联结查询
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 订单流：(用户名, 订单ID, 时间戳)
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

        // 浏览流：用户浏览事件
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

        // 间隔联结：按照用户分组，订单时间前后 5 秒到 10 秒的浏览数据
        orderStream.keyBy(data -> data.f0)  // 按照用户名分组
                .intervalJoin(clickStream.keyBy(data -> data.user))  // 浏览流也按照用户名分组
                .between(Time.seconds(-5), Time.seconds(10))  // 下界 -5秒，上界 10秒
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        // 处理匹配的数据对：订单和对应的浏览记录
                        out.collect(right + " => " + left);
                    }
                })
                .print();

        env.execute();
    }
}

