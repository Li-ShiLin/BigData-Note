package com.action.transformation.reduce;

import com.action.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce 自定义对象归约示例
 * 使用 reduce() 方法对 Event 对象进行归约，找出每个用户最近访问的记录
 * <p>
 * 特点说明：
 * - 功能：使用 reduce 对 Event 对象进行归约，找出每个用户最近访问的记录
 * - 实现方式：keyBy + reduce
 * - 归约逻辑：比较两个 Event 对象的时间戳，返回时间戳较大的 Event
 * - 使用场景：对象归约、最新记录提取、自定义聚合逻辑等
 * <p>
 * 预期输出：
 * latest> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:04.0}
 * latest> Event{user='Bob', url='./prod?id=2', timestamp=1970-01-01 08:00:03.8}
 * latest> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class TransReduceCustomObjectTest {
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

        // 找出每个用户最近访问的记录
        stream.keyBy(e -> e.user)  // 按用户分组
                .reduce(new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event value1, Event value2) throws Exception {
                        // 比较两个 Event 对象的时间戳，返回时间戳较大的 Event
                        return value1.timestamp > value2.timestamp ? value1 : value2;
                    }
                })
                .print("latest");

        env.execute();
    }
}

