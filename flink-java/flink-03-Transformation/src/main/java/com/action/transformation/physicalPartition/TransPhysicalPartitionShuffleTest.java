package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 随机分区示例
 * 使用 shuffle() 方法将数据随机分配到下游并行任务
 * <p>
 * 特点说明：
 * - 功能：最简单的重分区方式，通过"洗牌"将数据随机分配到下游算子的并行任务
 * - 分区策略：随机分区服从均匀分布（uniform distribution），可以把流中的数据随机打乱
 * - 数据分布：均匀地传递到下游任务分区
 * - 结果特点：对于同样的输入数据，每次执行得到的结果不会相同（因为是完全随机的）
 * - 返回值：经过随机分区之后，得到的依然是一个 DataStream
 * - 使用场景：负载均衡、数据随机分布、打破数据倾斜等
 */
public class TransPhysicalPartitionShuffleTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经洗牌后打印输出，并行度为 4
        stream.shuffle().print("shuffle").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * shuffle:4> Event{user='Bob', url='./home', timestamp=2025-11-15 23:33:15.91}
         * shuffle:4> Event{user='Bob', url='./home', timestamp=2025-11-15 23:33:16.939}
         * shuffle:3> Event{user='Bob', url='./cart', timestamp=2025-11-15 23:33:17.949}
         * shuffle:4> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:33:18.963}
         * shuffle:4> Event{user='Bob', url='./fav', timestamp=2025-11-15 23:33:19.972}
         * shuffle:4> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:33:20.981}
         * shuffle:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:33:21.983}
         * shuffle:3> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:33:22.993}
         * shuffle:1> Event{user='Mary', url='./home', timestamp=2025-11-15 23:33:24.002}
         * shuffle:2> Event{user='Bob', url='./fav', timestamp=2025-11-15 23:33:25.013}
         * shuffle:2> Event{user='Alice', url='./cart', timestamp=2025-11-15 23:33:26.02}
         */
    }
}

