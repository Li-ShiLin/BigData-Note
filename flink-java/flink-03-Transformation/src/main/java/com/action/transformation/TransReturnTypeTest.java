package com.action.transformation;


import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 返回类型处理示例
 * 演示 Lambda 表达式返回类型的处理方式
 * 
 * 特点说明：
 * - 功能：演示 Lambda 表达式返回复杂类型时的处理方式
 * - 问题：Java 的 Lambda 表达式在返回泛型类型（如 Tuple2）时，类型擦除会导致 Flink 无法推断类型
 * - 解决方案：
 *   1. 使用 .returns() 显式指定返回类型
 *   2. 使用类替代 Lambda 表达式
 *   3. 使用匿名类替代 Lambda 表达式
 * - 使用场景：Lambda 表达式返回复杂类型时
 * 
 * 预期输出：
 * (Mary,1)
 * (Bob,1)
 * (Mary,1)
 * (Bob,1)
 * (Mary,1)
 * (Bob,1)
 */
public class TransReturnTypeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 想要转换成二元组类型，需要进行以下处理
        // 1) 使用显式的 ".returns(...)"
        DataStream<Tuple2<String, Long>> stream3 = clicks
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        stream3.print();


        // 2) 使用类来替代Lambda表达式
        clicks.map(new MyTuple2Mapper())
                .print();

        // 3) 使用匿名类来代替Lambda表达式
        clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).print();

        env.execute();
    }

    // 自定义MapFunction的实现类
    public static class MyTuple2Mapper implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event value) throws Exception {
            return Tuple2.of(value.user, 1L);
        }
    }
}

