package com.action.transformation.flatmap;


import com.action.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 转换算子示例
 * 使用 flatMap() 方法对数据流中的每个元素进行一对多转换
 * <p>
 * 特点说明：
 * - 功能：对数据流中的每个元素进行一对多转换，可以输出零个、一个或多个元素
 * - 输入输出：一个输入元素可以对应零个、一个或多个输出元素
 * - 实现方式：实现 FlatMapFunction<T, O> 接口，通过 Collector 收集输出元素
 * - 使用场景：数据拆分、字符串分词、一对多转换等
 * <p>
 * 预期输出：
 * Mary
 * Bob
 * ./cart
 */
public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}

