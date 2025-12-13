package com.action.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink批处理WordCount程序
 * 使用DataSet API实现单词频次统计
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据
        DataSource<String> lineDS = env.readTextFile("flink-01-quickstart/input/words.txt");

        // 3. 转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    // 将一行文本按空格切分，转换成(word, 1)的二元组
                    Arrays.stream(line.split(" "))
                            .forEach(word -> out.collect(Tuple2.of(word, 1L)));
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOne.groupBy(0);

        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 6. 打印结果
        sum.print();
        /*        输出
                (flink,3)
                (world,1)
                (hello,4)
                (awesome,1)
                (great,1)
                (java,3)
                (is,2)
                */
    }
}
