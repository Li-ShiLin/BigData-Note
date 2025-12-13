package com.action.transformation.physicalPartition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 自定义分区示例
 * 使用 partitionCustom() 方法根据自定义逻辑进行分区
 * <p>
 * 特点说明：
 * - 功能：当 Flink 提供的所有分区策略都不能满足用户的需求时，可以通过自定义分区策略
 * - 分区策略：需要传入自定义分区器（Partitioner）对象和应用分区器的字段
 * - 字段指定：可以通过字段名称指定、通过字段位置索引指定，还可以实现一个 KeySelector
 * - 返回值：经过自定义分区之后，得到的依然是一个 DataStream
 * - 使用场景：特殊分区需求、业务规则分区、自定义负载均衡等
 * <p>
 * 数据说明：
 * - 将自然数按照奇偶分区
 * - 奇数发送到分区 1，偶数发送到分区 0
 */
public class TransPhysicalPartitionCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // 奇数返回 1，偶数返回 0
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);

        env.execute();

        /*
         * 预期输出：
         * 2> 1
         * 1> 2
         * 2> 3
         * 1> 4
         * 2> 5
         * 1> 6
         * 2> 7
         * 1> 8
         */
    }
}

