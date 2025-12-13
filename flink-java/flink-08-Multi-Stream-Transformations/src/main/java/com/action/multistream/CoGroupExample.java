package com.action.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 窗口同组联结（Window CoGroup）示例
 * 
 * CoGroup 操作比窗口的 join 更加通用
 * 不仅可以实现类似 SQL 中的"内连接"（inner join）
 * 也可以实现左外连接（left outer join）、右外连接（right outer join）和全外连接（full outer join）
 * 
 * 与 window join 的区别：
 * - window join 计算笛卡尔积，每对匹配数据调用一次 join 方法
 * - coGroup 将窗口内所有数据一次性传入，可以自定义配对逻辑
 */
public class CoGroupExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }
                        )
        );

        // 创建第二条流：(key, timestamp)
        DataStream<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }
                        )
        );

        // 窗口同组联结：按照 key 分组，在 5 秒的滚动窗口内进行同组联结
        stream1.coGroup(stream2)
                .where(r -> r.f0)  // 第一条流的 key
                .equalTo(r -> r.f0)  // 第二条流的 key
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 5秒滚动窗口
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                        // 将窗口内两条流的所有数据一次性传入，可以自定义配对逻辑
                        collector.collect(iter1 + "=>" + iter2);
                    }
                })
                .print();

        env.execute();
    }
}

