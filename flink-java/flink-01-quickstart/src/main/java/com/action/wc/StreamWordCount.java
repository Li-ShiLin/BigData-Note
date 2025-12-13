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
 * Flink无界流处理WordCount程序
 * 使用DataStream API处理无界数据流（socket文本流）
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* centos linux 使用nx命令创建数据流
        sudo yum -y install nc
        nc -lk 7777
        abc
        bbc
        abc
        aaa
        abc*/
        // 2. 读取文本流
        DataStreamSource<String> lineDSS = env.socketTextStream("server01", 7777);

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    // 将一行文本按空格切分
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

        // 4. 按照word进行分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 5. 分组内聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // 6. 打印结果
        result.print();

        // 7. 执行任务
        env.execute();

        /*
        # 控制台打印
        29> (abc,1)
        20> (bbc,1)
        29> (abc,2)
        20> (aaa,1)
        29> (abc,3)
         */
    }
}