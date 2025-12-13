package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 字符串拼接归约示例
 * 使用 reduce() 方法将每个用户访问的所有 URL 拼接成一个字符串
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 将每个用户访问的所有 URL 拼接成一个字符串
 * - 实现方式：keyBy + map + reduce
 * - 归约逻辑：将两个元素的 URL 字符串用分隔符连接
 * - 使用场景：URL 列表收集、字符串拼接、访问路径记录等
 * <p>
 * 预期输出：
 * concat> (Mary,./home;./home)
 * concat> (Bob,./cart;./prod?id=1;./home;./prod?id=2)
 * concat> (Alice,./prod?id=100)
 */
public class TransReduceStringConcatTest {
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

        // 将每个用户访问的所有 URL 拼接成一个字符串
        stream.map(new MapFunction<Event, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Event value) throws Exception {
                        return Tuple2.of(value.user, value.url);
                    }
                })
                .keyBy(r -> r.f0)  // 按用户分组
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
                        // 将两个元素的 URL 字符串用分号连接
                        String concatenated = value1.f1 + ";" + value2.f1;
                        return Tuple2.of(value1.f0, concatenated);
                    }
                })
                .print("concat");

        env.execute();
    }
}

