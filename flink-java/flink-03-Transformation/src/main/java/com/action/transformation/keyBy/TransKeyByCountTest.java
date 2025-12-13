package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 统计用户访问次数示例
 * 使用 keyBy 分组后统计每个用户的访问次数（PV统计）
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 分组后统计每个用户的访问次数
 * - 实现方式：keyBy + map + sum 聚合
 * - 使用场景：用户行为统计、PV/UV统计、分组计数等
 * <p>
 * 预期输出：
 * count> (Mary,2)
 * count> (Bob,4)
 * count> (Alice,1)
 */
public class TransKeyByCountTest {
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

        // 统计每个用户的访问次数
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .sum(1)             // 对第二个字段（计数）求和
                .print("count");

        env.execute();
    }
}

