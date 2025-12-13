package com.action.source;

import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 综合数据源示例
 * 演示 Flink 支持的多种数据源读取方式
 * 
 * 特点说明：
 * - 综合演示：展示多种数据源创建方式
 * - 灵活切换：通过注释/取消注释切换不同的数据源
 * - 学习参考：适合初学者理解各种数据源的使用方法
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceTest"
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("flink-02-DataSource/input/clicks.csv");

        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4. 从 Socket 文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("server01", 7777);

        // 打印输出（根据需要取消注释）
        // stream1.print("1");
        // numStream.print("nums");
        // stream2.print("2");
        // stream3.print("3");
        stream4.print("4");

        env.execute();
    }
}

