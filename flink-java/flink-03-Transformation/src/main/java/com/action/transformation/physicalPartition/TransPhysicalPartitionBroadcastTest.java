package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 广播分区示例
 * 使用 broadcast() 方法将数据复制到所有下游并行任务
 * <p>
 * 特点说明：
 * - 功能：经过广播之后，数据会在不同的分区都保留一份
 * - 分区策略：数据被复制然后广播到下游的所有并行任务中
 * - 数据特点：每个下游并行任务都会收到完整的数据副本
 * - 返回值：经过广播分区之后，得到的依然是一个 DataStream
 * - 使用场景：动态更新配置、动态发送规则、小表关联、广播变量等
 */
public class TransPhysicalPartitionBroadcastTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * broadcast:1> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:3> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:2> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:4> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:23:20.223}
         * broadcast:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:4> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:3> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         * broadcast:2> Event{user='Cary', url='./home', timestamp=2025-11-15 23:23:21.256}
         */
    }
}

