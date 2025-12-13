package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * FlatMap 嵌套数据展开示例
 * 使用 flatMap() 方法将包含列表的数据展开成多个元素
 * <p>
 * 特点说明：
 * - 功能：将包含列表或数组的数据结构展开成多个独立的元素
 * - 输入输出：一个包含列表的对象对应多个列表中的元素
 * - 实现方式：遍历列表，将每个元素收集到输出流
 * - 使用场景：嵌套数据展开、数组扁平化、一对多数据转换等
 * <p>
 * 预期输出：
 * apple
 * banana
 * orange
 * dog
 * cat
 * bird
 * red
 * green
 * blue
 */
public class TransFlatmapNestedDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建包含列表的数据
        DataStreamSource<List<String>> stream = env.fromElements(
                Arrays.asList("apple", "banana", "orange"),
                Arrays.asList("dog", "cat", "bird"),
                Arrays.asList("red", "green", "blue")
        );

        // 将每个列表展开成多个元素
        stream.flatMap(new ListFlattener()).print();

        env.execute();
    }

    public static class ListFlattener implements FlatMapFunction<List<String>, String> {
        @Override
        public void flatMap(List<String> list, Collector<String> out) throws Exception {
            // 遍历列表中的每个元素，收集到输出流
            for (String item : list) {
                out.collect(item);
            }
        }
    }
}

