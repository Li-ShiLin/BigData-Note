package com.action.transformation.physicalPartition;


import com.action.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 物理分区示例
 * 演示各种物理分区策略
 * <p>
 * 特点说明：
 * - 功能：控制数据在并行任务之间的物理分布
 * - 分区策略：
 * 1. shuffle()：随机分区，数据随机分布到下游并行任务
 * 2. rebalance()：轮询分区，数据轮询分布到下游并行任务
 * 3. rescale()：重缩放分区，在上下游并行度之间进行局部重平衡
 * 4. broadcast()：广播分区，数据复制到所有下游并行任务
 * 5. global()：全局分区，所有数据发送到第一个并行任务
 * 6. partitionCustom()：自定义分区，根据自定义逻辑分区
 * - 使用场景：负载均衡、数据分布优化、广播数据等
 * <p>
 * 预期输出：
 * shuffle> Event{user='...', url='...', timestamp=...}
 * rebalance> Event{user='...', url='...', timestamp=...}
 * ...
 */
public class TransPhysicalPatitioningTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        // 1. 随机分区
        stream.shuffle().print("shuffle").setParallelism(4);

        // 2. 轮询分区
        stream.rebalance().print("rebalance").setParallelism(4);

        // 3. rescale重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {  // 这里使用了并行数据源的富函数版本
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            // 将奇数发送到索引为1的并行子任务
                            // 将偶数发送到索引为0的并行子任务
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

        // 4. 广播
        stream.broadcast().print("broadcast").setParallelism(4);

        // 5. 全局分区
        stream.global().print("global").setParallelism(4);

        // 6. 自定义重分区
        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
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
    }
}

