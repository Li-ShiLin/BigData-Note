package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 多字段组合键示例
 * 使用多个字段组合作为 key 进行分组
 * <p>
 * 特点说明：
 * - 功能：使用多个字段组合作为 key 进行分组，只有所有字段都相同的记录才会被分到同一组
 * - 实现方式：返回 Tuple 类型作为组合键
 * - 使用场景：需要按多个维度分组、复合键分组、统计用户对特定URL的访问次数等
 * <p>
 * 数据说明：
 * - 测试数据包含相同用户访问不同URL、不同用户访问相同URL、相同用户访问相同URL等多种情况
 * - 使用组合键（用户 + URL）后，只有用户和URL都相同的记录才会被分到同一组
 * - 例如：(Mary, ./home) 和 (Mary, ./home) 会被分到同一组
 * - 例如：(Mary, ./home) 和 (Mary, ./cart) 会被分到不同组
 * - 例如：(Mary, ./home) 和 (Bob, ./home) 会被分到不同组
 * <p>
 * 预期输出：
 * count> ((Mary,./home),2)
 * count> ((Mary,./cart),1)
 * count> ((Bob,./home),2)
 * count> ((Bob,./cart),1)
 * count> ((Alice,./prod?id=100),1)
 * count> ((Alice,./home),1)
 */
public class TransKeyByCompositeKeyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 测试数据：包含相同用户访问不同URL、不同用户访问相同URL、相同用户访问相同URL等情况
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),      // (Mary, ./home) - 组1
                new Event("Mary", "./cart", 2000L),      // (Mary, ./cart) - 组2
                new Event("Bob", "./home", 3000L),       // (Bob, ./home) - 组3
                new Event("Bob", "./cart", 4000L),       // (Bob, ./cart) - 组4
                new Event("Alice", "./prod?id=100", 5000L), // (Alice, ./prod?id=100) - 组5
                new Event("Mary", "./home", 6000L),      // (Mary, ./home) - 组1（与第一条相同）
                new Event("Bob", "./home", 7000L),       // (Bob, ./home) - 组3（与第三条相同）
                new Event("Alice", "./home", 8000L)      // (Alice, ./home) - 组6
        );

        // 使用多个字段组合作为 key（用户 + URL）
        // 只有用户和URL都相同的记录才会被分到同一组
        KeyedStream<Event, Tuple2<String, String>> keyedStream = stream.keyBy(new KeySelector<Event, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Event value) throws Exception {
                return Tuple2.of(value.user, value.url);
            }
        });

        // 统计每个（用户，URL）组合的访问次数，以展示组合键的分组效果
        keyedStream.map(new MapFunction<Event, Tuple2<Tuple2<String, String>, Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Long> map(Event value) throws Exception {
                        Tuple2<String, String> key = Tuple2.of(value.user, value.url);
                        return Tuple2.of(key, 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> value) throws Exception {
                        return value.f0;  // 使用组合键作为 key
                    }
                })
                .sum(1)             // 统计访问次数（对第二个字段求和）
                .print("count");

        env.execute();
    }
}

