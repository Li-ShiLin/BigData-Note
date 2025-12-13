package com.action.transformation.physicalPartition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 物理分区 - 重缩放分区示例
 * 使用 rescale() 方法在上下游并行度之间进行局部重平衡
 * <p>
 * 特点说明：
 * - 功能：重缩放分区和轮询分区非常相似，但只会将数据轮询发送到下游并行任务的一部分中
 * - 分区策略：底层使用 Round-Robin 算法进行轮询，但只针对部分下游任务
 * - 区别说明：
 * - rebalance：每个发牌人都面向所有人发牌（所有上游任务和所有下游任务之间建立通信通道）
 * - rescale：分成小团体，发牌人只给自己团体内的所有人轮流发牌（每个任务和下游对应的部分任务之间建立通信通道）
 * - 效率优势：当下游任务数量是上游任务数量的整数倍时，rescale 的效率明显会更高
 * - 网络优化：rescale 可以让数据只在当前 TaskManager 的多个 slot 之间重新分配，从而避免了网络传输带来的损耗
 * - 使用场景：上下游并行度调整、局部重平衡、减少网络传输等
 * <p>
 * 数据说明：
 * - 使用并行数据源的富函数版本，可以调用 getRuntimeContext 方法获取运行时上下文信息
 * - 将奇数发送到索引为 1 的并行子任务
 * - 将偶数发送到索引为 0 的并行子任务
 */
public class TransPhysicalPartitionRescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务
                            // 将偶数发送到索引为 0 的并行子任务
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * 3> 1
         * 2> 4
         * 1> 2
         * 4> 3
         * 1> 6
         * 2> 8
         * 3> 5
         * 4> 7
         */
    }
}

