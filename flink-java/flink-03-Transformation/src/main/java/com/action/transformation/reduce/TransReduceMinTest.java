package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 最小值归约示例
 * 使用 reduce() 方法找出每个用户访问的最小时间戳（最早访问时间）
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 找出每个用户访问的最小时间戳
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：比较两个元素的时间戳，返回时间戳较小的元素
 * - 使用场景：找出最早记录、最小值计算、首次访问时间等
 * <p>
 * 预期输出：
 * min> (Mary,1000)
 * min> (Bob,2000)
 * min> (Alice,3000)
 */
public class TransReduceMinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Mary", "./home", 4000L)
        );

        // 找出每个用户访问的最小时间戳
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 比较两个元素的时间戳，返回时间戳较小的元素
                        return value1.f1 < value2.f1 ? value1 : value2;
                    }
                })
                .print("min");

        env.execute();
    }
}

