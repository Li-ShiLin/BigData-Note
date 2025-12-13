package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 字符拆分示例
 * 使用 flatMap() 方法将字符串拆分成单个字符
 * <p>
 * 特点说明：
 * - 功能：将输入的字符串拆分成单个字符
 * - 输入输出：一个字符串对应多个字符
 * - 实现方式：遍历字符串的每个字符，收集到输出流
 * - 使用场景：字符分析、文本处理、数据验证等
 * <p>
 * 预期输出：
 * F
 * l
 * i
 * n
 * k
 * H
 * e
 * l
 * l
 * o
 * W
 * o
 * r
 * l
 * d
 */
public class TransFlatmapCharSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements(
                "Flink",
                "Hello",
                "World"
        );

        // 将每个字符串拆分成单个字符
        stream.flatMap(new CharSplitter()).print();

        env.execute();
    }

    public static class CharSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String word, Collector<String> out) throws Exception {
            // 将字符串转换为字符数组
            char[] chars = word.toCharArray();
            // 遍历每个字符，收集到输出流
            for (char c : chars) {
                out.collect(String.valueOf(c));
            }
        }
    }
}

