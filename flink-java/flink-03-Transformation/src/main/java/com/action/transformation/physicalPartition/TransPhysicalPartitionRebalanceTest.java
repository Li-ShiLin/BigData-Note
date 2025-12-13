package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 轮询分区示例
 * 使用 rebalance() 方法将数据轮询分配到下游并行任务
 * <p>
 * 特点说明：
 * - 功能：按照先后顺序将数据做依次分发，使用 Round-Robin 负载均衡算法
 * - 分区策略：轮询分区，将输入流数据平均分配到下游的并行任务中去
 * - 算法说明：Round-Robin 算法用在了很多地方，例如 Kafka 和 Nginx
 * - 数据分布：数据被平均分配到所有并行任务中
 * - 返回值：经过轮询分区之后，得到的依然是一个 DataStream
 * - 使用场景：负载均衡、数据平均分布、打破数据倾斜等
 */
public class TransPhysicalPartitionRebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经轮询重分区后打印输出，并行度为 4
        stream.rebalance().print("rebalance").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * rebalance:4> Event{user='Cary', url='./prod?id=1', timestamp=2025-11-15 23:28:32.155}
         * rebalance:1> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:28:33.179}
         * rebalance:2> Event{user='Cary', url='./prod?id=2', timestamp=2025-11-15 23:28:34.189}
         * rebalance:3> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:35.201}
         * rebalance:4> Event{user='Alice', url='./home', timestamp=2025-11-15 23:28:36.213}
         * rebalance:1> Event{user='Mary', url='./prod?id=2', timestamp=2025-11-15 23:28:37.226}
         * rebalance:2> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:38.233}
         * rebalance:3> Event{user='Cary', url='./prod?id=1', timestamp=2025-11-15 23:28:39.244}
         * rebalance:4> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:40.245}
         * rebalance:1> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:28:41.253}
         * rebalance:2> Event{user='Cary', url='./prod?id=2', timestamp=2025-11-15 23:28:42.265}
         * rebalance:3> Event{user='Mary', url='./prod?id=2', timestamp=2025-11-15 23:28:43.276}
         * rebalance:4> Event{user='Alice', url='./prod?id=1', timestamp=2025-11-15 23:28:44.288}
         */
    }
}

