package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 分组后计算总访问时长示例
 * 使用 keyBy 分组后计算每个用户的总访问时长
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 分组后计算每个用户的总访问时长
 * - 实现方式：keyBy + map + reduce
 * - 使用场景：用户行为分析、时长统计、分组汇总等
 * <p>
 * 预期输出：
 * totalDuration> (Mary,./home,1000)
 * totalDuration> (Bob,./cart,2000)
 * totalDuration> (Alice,./prod?id=100,3000)
 * totalDuration> (Bob,./cart,5300)
 * totalDuration> (Bob,./cart,8800)
 * totalDuration> (Bob,./cart,12600)
 * totalDuration> (Mary,./home,5000)
 */
public class TransKeyByAvgTest {
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
        stream.map(new MapFunction<Event, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Event value) throws Exception {
                        // 假设访问时长 = timestamp（简化示例）
                        return Tuple3.of(value.user, value.url, value.timestamp);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                        // 累加访问时长
                        return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                })
                .print("totalDuration");

        env.execute();
    }
}

