package com.action.transformation.keyBy;

import com.action.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy 简单聚合示例
 * 按键分组之后进行聚合，提取当前用户最近一次访问数据
 * <p>
 * 特点说明：
 * - 功能：使用 keyBy 按键分组，然后使用 max/maxBy 提取每个用户最近一次访问数据
 * - 实现方式：
 * 1. 使用 KeySelector 匿名类和 max() 方法
 * 2. 使用 Lambda 表达式和 maxBy() 方法
 * - 区别说明：
 * - max()：只更新指定字段，其他字段保持第一个元素的值
 * - maxBy()：返回完整记录，所有字段都更新为最大值对应的记录
 * - 使用场景：用户行为分析、最近访问记录提取、分组聚合等
 * <p>
 * 预期输出：
 * max> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * maxBy> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * max> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 * maxBy> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * maxBy> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 * max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.3}
 * maxBy> Event{user='Bob', url='./prod?id=1', timestamp=1970-01-01 08:00:03.3}
 * max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.5}
 * maxBy> Event{user='Bob', url='./home', timestamp=1970-01-01 08:00:03.5}
 * max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:03.8}
 * maxBy> Event{user='Bob', url='./prod?id=2', timestamp=1970-01-01 08:00:03.8}
 * max> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:04.2}
 * maxBy> Event{user='Bob', url='./prod?id=3', timestamp=1970-01-01 08:00:04.2}
 */
public class TransKeyBySimpleAggTest {
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
                new Event("Bob", "./prod?id=3", 4200L)
        );

        // 按键分组之后进行聚合，提取当前用户最近一次访问数据

        // 方式1：使用 KeySelector 匿名类和 max() 方法
        KeyedStream<Event, String> keyedStream = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
        keyedStream.max("timestamp").print("max");

        // 方式2：使用 Lambda 表达式和 maxBy() 方法
        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy");

        env.execute();
    }
}

