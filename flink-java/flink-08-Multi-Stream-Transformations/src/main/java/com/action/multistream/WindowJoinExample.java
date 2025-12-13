package com.action.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口联结（Window Join）示例
 * 
 * 窗口联结可以定义时间窗口，并将两条流中共享一个公共键（key）的数据放在窗口中进行配对处理
 * 窗口联结是内连接（inner join），只有匹配成功的数据才会输出
 * 窗口内会计算两条流数据的笛卡尔积
 */
public class WindowJoinExample {
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
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
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
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        // 窗口联结：按照 key 分组，在 5 秒的滚动窗口内进行联结
        stream1.join(stream2)
                .where(r -> r.f0)  // 第一条流的 key
                .equalTo(r -> r.f0)  // 第二条流的 key
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 5秒滚动窗口
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> left, Tuple2<String, Long> right) throws Exception {
                        // 对匹配的数据对进行处理
                        return left + "=>" + right;
                    }
                })
                .print();

        env.execute();
    }
}

