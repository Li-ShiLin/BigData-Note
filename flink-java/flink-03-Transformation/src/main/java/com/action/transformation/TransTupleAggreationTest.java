package com.action.transformation;



import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Tuple 聚合算子示例
 * 演示对 Tuple 类型数据进行聚合操作
 * 
 * 特点说明：
 * - 功能：对 Tuple 类型数据进行聚合操作
 * - 聚合方法：
 *   - sum()：求和
 *   - max()：最大值（只更新指定字段）
 *   - min()：最小值（只更新指定字段）
 *   - maxBy()：最大值（返回完整记录）
 *   - minBy()：最小值（返回完整记录）
 * - 字段指定：可以通过位置索引（如 1）或字段名称（如 "f1"）指定聚合字段
 * - 使用场景：数值统计、分组聚合等
 * 
 * 预期输出：
 * (a,1)
 * (b,3)
 */
public class TransTupleAggreationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

//        stream.keyBy(r -> r.f0).sum(1).print();
//        stream.keyBy(r -> r.f0).sum("f1").print();
//        stream.keyBy(r -> r.f0).max(1).print();
//        stream.keyBy(r -> r.f0).max("f1").print();
//        stream.keyBy(r -> r.f0).min(1).print();
//        stream.keyBy(r -> r.f0).min("f1").print();
//        stream.keyBy(r -> r.f0).maxBy(1).print();
//        stream.keyBy(r -> r.f0).maxBy("f1").print();
//        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).minBy("f1").print();

        env.execute();
    }
}

