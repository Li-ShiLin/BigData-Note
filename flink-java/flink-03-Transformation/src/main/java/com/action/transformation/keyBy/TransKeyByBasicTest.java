package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 基础用法示例
 * 演示 keyBy 的多种实现方式
 * <p>
 * 特点说明：
 * - 功能：演示 keyBy 的多种实现方式（Lambda 表达式、KeySelector 匿名类、自定义 KeySelector 类）
 * - 实现方式：
 *   1. Lambda 表达式：stream.keyBy(e -> e.user)
 *   2. KeySelector 匿名类：stream.keyBy(new KeySelector<Event, String>() {...})
 *   3. 自定义 KeySelector 类：stream.keyBy(new UserKeySelector())
 * - 返回值：keyBy 返回 KeyedStream，不再是 DataStream
 * - 使用场景：数据分组、分流、为后续聚合操作做准备
 * <p>
 * 预期输出：
 * keyBy1> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy1> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * keyBy2> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy2> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * keyBy3> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * keyBy3> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class TransKeyByBasicTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 方式1：使用 Lambda 表达式（推荐，代码简洁）
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(e -> e.user);
        keyedStream1.print("keyBy1");

        // 方式2：使用 KeySelector 匿名类
        KeyedStream<Event, String> keyedStream2 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event e) throws Exception {
                return e.user;
            }
        });
        keyedStream2.print("keyBy2");

        // 方式3：使用自定义 KeySelector 类（适合复杂逻辑）
        KeyedStream<Event, String> keyedStream3 = stream.keyBy(new UserKeySelector());
        keyedStream3.print("keyBy3");

        env.execute();
    }

    /**
     * 自定义 KeySelector 实现类
     */
    public static class UserKeySelector implements KeySelector<Event, String> {
        @Override
        public String getKey(Event value) throws Exception {
            return value.user;
        }
    }
}

