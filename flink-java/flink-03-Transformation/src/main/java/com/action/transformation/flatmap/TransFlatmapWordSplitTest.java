package com.action.transformation.flatmap;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 字符串分词示例
 * 使用 flatMap() 方法将句子拆分成单词
 * <p>
 * 特点说明：
 * - 功能：将输入的句子字符串拆分成多个单词
 * - 输入输出：一个句子字符串对应多个单词字符串
 * - 实现方式：使用 split() 方法分割字符串，然后通过 Collector 收集每个单词
 * - 使用场景：文本处理、日志分析、关键词提取等
 * <p>
 * 预期输出：
 * hello
 * world
 * hello
 * flink
 * hello
 * java
 * apache
 * flink
 * is
 * great
 */
public class TransFlatmapWordSplitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements(
                "hello world",
                "hello flink",
                "hello java",
                "apache flink is great"
        );

        // 将每个句子拆分成单词
        stream.flatMap(new WordSplitter()).print();

        env.execute();
    }

    public static class WordSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> out) throws Exception {
            // 按空格分割句子，得到单词数组
            String[] words = sentence.split("\\s+");
            // 遍历每个单词，收集到输出流
            for (String word : words) {
                if (!word.isEmpty()) {
                    out.collect(word);
                }
            }
        }
    }
}

