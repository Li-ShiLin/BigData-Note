package com.action.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink有界流处理WordCount程序
 * 使用DataStream API处理有界数据流（文件）
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("flink-01-quickstart/input/words.txt");

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    // 将一行文本按空格切分
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 按照word进行分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 5. 分组内聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // 6. 打印结果
        result.print();

        // 7. 执行任务
        env.execute();
        /*
                输出：
                9> (hello,1)
                6> (java,1)
                31> (is,1)
                25> (flink,1)
                17> (world,1)
                5> (awesome,1)
                6> (java,2)
                22> (great,1)
                6> (java,3)
                9> (hello,2)
                9> (hello,3)
                31> (is,2)
                9> (hello,4)
                25> (flink,2)
                25> (flink,3)
                */
    }
}