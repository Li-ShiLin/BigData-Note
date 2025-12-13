package com.action.transformation.map;


import com.action.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Map 转换算子示例
 * 使用 map() 方法对数据流中的每个元素进行一对一转换
 * <p>
 * 特点说明：
 * - 功能：对数据流中的每个元素进行一对一转换
 * - 输入输出：一个输入元素对应一个输出元素
 * - 实现方式：实现 MapFunction<T, O> 接口，其中 T 是输入类型，O 是输出类型
 * - 使用场景：数据格式转换、字段提取、类型转换等
 * <p>
 * 预期输出：
 * Mary
 * Bob
 */
public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 进行转换计算，提取user字段
        // 1，使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new UserExtractor());

//        result1.print();

        //2．使用匿名类实现MapFunction接口
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });

        // 传入MapFunction的实现类
        stream.map(new UserExtractor()).print();

        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}

