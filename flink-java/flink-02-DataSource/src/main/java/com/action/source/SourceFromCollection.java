package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 从集合读取数据源示例
 * 使用 fromCollection() 方法从 Java 集合中读取数据
 * 
 * 特点说明：
 * - 适用场景：从内存中的 Java 集合创建数据流
 * - 支持类型：支持任意类型的集合（List、Set 等）
 * - 数据特点：有界数据流
 * - 使用场景：测试、小批量数据处理
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromCollection"
 * 
 * 预期输出：
 * nums> 2
 * nums> 5
 * events> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * events> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 */
public class SourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从整数集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        // 2. 从 Event 对象集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream = env.fromCollection(events);

        numStream.print("nums");
        stream.print("events");

        env.execute();
    }
}
