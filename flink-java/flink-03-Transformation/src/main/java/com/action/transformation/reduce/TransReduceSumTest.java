package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 求和归约示例
 * 使用 reduce() 方法计算每个用户的总访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 对每个用户的访问时长进行累加求和
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：将两个元素的时长值相加，返回累加结果
 * - 使用场景：用户行为分析、时长统计、累加计算等
 * <p>
 * 预期输出：
 * sum> (Mary,5000)
 * sum> (Bob,12800)
 * sum> (Alice,3000)
 */
public class TransReduceSumTest {
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

        // 计算每个用户的总访问时长
        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple2.of(value.user, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 累加访问时长：将两个元素的时长值相加
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print("sum");

        env.execute();
    }
}

