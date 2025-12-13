package com.action.transformation.physicalPartition;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 物理分区 - 全局分区示例
 * 使用 global() 方法将所有数据发送到下游算子的第一个并行子任务
 * <p>
 * 特点说明：
 * - 功能：将所有输入流数据都发送到下游算子的第一个并行子任务中去
 * - 分区策略：非常极端的分区方式，相当于强行让下游任务并行度变成了 1
 * - 数据分布：所有数据都发送到第一个并行任务（索引为 0）
 * - 注意事项：使用这个操作要非常谨慎，可能对程序造成很大的压力
 * - 返回值：经过全局分区之后，得到的依然是一个 DataStream
 * - 使用场景：全局聚合、单点处理、调试等
 */
public class TransPhysicalPartitionGlobalTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经全局分区后打印输出，并行度为 4（但所有数据都会发送到第一个任务）
        stream.global().print("global").setParallelism(4);

        env.execute();

        /*
         * 预期输出：
         * global:1> Event{user='Cary', url='./home', timestamp=2025-11-15 23:26:56.925}
         * global:1> Event{user='Mary', url='./cart', timestamp=2025-11-15 23:26:57.957}
         * global:1> Event{user='Bob', url='./home', timestamp=2025-11-15 23:26:58.97}
         * global:1> Event{user='Bob', url='./cart', timestamp=2025-11-15 23:26:59.984}
         * global:1> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:27:00.996}
         * global:1> Event{user='Alice', url='./fav', timestamp=2025-11-15 23:27:02.007}
         * global:1> Event{user='Mary', url='./fav', timestamp=2025-11-15 23:27:03.02}
         * global:1> Event{user='Mary', url='./prod?id=1', timestamp=2025-11-15 23:27:04.021}
         * global:1> Event{user='Bob', url='./prod?id=2', timestamp=2025-11-15 23:27:05.033}
         */
    }
}

