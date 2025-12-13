package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从元素读取数据源示例
 * 使用 fromElements() 方法直接从元素创建数据流
 * 
 * 特点说明：
 * - 适用场景：快速创建包含少量元素的数据流
 * - 使用方式：直接传入多个元素作为参数
 * - 数据特点：有界数据流
 * - 优势：代码简洁，适合测试和演示
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceFromElements"
 * 
 * 预期输出：
 * elements> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
 * elements> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
 * elements> Event{user='Alice', url='./prod?id=100', timestamp=1970-01-01 08:00:03.0}
 */
public class SourceFromElements {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        // 可以直接传入多个元素，Flink 会自动创建数据流
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        stream.print("elements");

        env.execute();
    }
}

