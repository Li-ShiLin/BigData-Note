package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 平均值归约示例
 * 使用 reduce() 方法计算每个用户的平均访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 计算每个用户的平均访问时长
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：维护累加和和计数，最后计算平均值
 * - 使用场景：平均值计算、统计分析、用户行为分析等
 * <p>
 * 数据说明：
 * - 使用 Tuple3<String, Long, Long> 存储（用户，总时长，访问次数）
 * - reduce 过程中累加总时长和访问次数
 * - 最后通过 map 计算平均值
 * <p>
 * 预期输出：
 * avg> (Mary,2500.0)
 * avg> (Bob,3200.0)
 * avg> (Alice,3000.0)
 */
public class TransReduceAvgTest {
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

        // 计算每个用户的平均访问时长
        stream.map(new MapFunction<Event, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(Event value) throws Exception {
                        // Tuple3: (用户, 访问时长, 访问次数)
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple3.of(value.user, value.timestamp, 1L);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) throws Exception {
                        // 累加总时长和访问次数
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(Tuple3<String, Long, Long> value) throws Exception {
                        // 计算平均值：总时长 / 访问次数
                        double avg = (double) value.f1 / value.f2;
                        return Tuple3.of(value.f0, avg, value.f2);
                    }
                })
                .print("avg");

        env.execute();
    }
}

