package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 保存点（Savepoint）使用示例
 * 
 * 保存点与检查点最大的区别，就是触发的时机：
 * - 检查点是由 Flink 自动管理的，定期创建，发生故障之后自动读取进行恢复，这是一个"自动存盘"的功能
 * - 保存点不会自动创建，必须由用户明确地手动触发保存操作，所以就是"手动存盘"
 * 
 * 保存点的用途：
 * - 版本管理和归档存储
 * - 更新 Flink 版本
 * - 更新应用程序
 * - 调整并行度
 * - 暂停应用程序
 * 
 * 注意：为了方便后续的维护，强烈建议在程序中为每一个算子手动指定 ID
 */
public class SavepointExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 启用检查点（保存点基于检查点机制） ====================
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 设置状态后端 ====================
        env.setStateBackend(new HashMapStateBackend());

        // ==================== 设置默认保存点目录 ====================
        // 保存点的默认路径可以通过配置文件 flink-conf.yaml 中的 state.savepoints.dir 项来设定
        // 也可以在程序代码中通过执行环境来设置
        env.setDefaultSavepointDirectory("file:///tmp/flink-savepoints");

        // ==================== 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .uid("source-id")  // 为算子指定 ID，方便从保存点恢复
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .uid("watermark-assigner-id");  // 为算子指定 ID

        // 统计每个用户的访问次数，使用状态保存计数
        stream.keyBy(data -> data.user)
                .process(new UserVisitCountFunction())
                .uid("visit-count-processor-id")  // 为算子指定 ID
                .print("用户访问统计");

        env.execute("Savepoint Example");
    }

    /**
     * 自定义处理函数：统计每个用户的访问次数
     * 使用状态保存计数，这样可以从保存点恢复状态
     */
    public static class UserVisitCountFunction extends KeyedProcessFunction<String, Event, String> {
        // 定义状态，保存每个用户的访问次数
        private ValueState<Long> visitCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            visitCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("visit-count", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 更新访问次数
            Long count = visitCountState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            visitCountState.update(count);

            // 输出统计结果
            out.collect(value.user + " 访问次数: " + count);
        }
    }
}

/**
 * 保存点的使用说明：
 * 
 * 1. 创建保存点（在命令行中执行）：
 *    bin/flink savepoint <jobId> [targetDirectory]
 *    例如：bin/flink savepoint abc123def456 /tmp/flink-savepoints
 * 
 * 2. 停止作业并创建保存点：
 *    bin/flink stop --savepointPath [targetDirectory] <jobId>
 *    例如：bin/flink stop --savepointPath /tmp/flink-savepoints abc123def456
 * 
 * 3. 从保存点重启应用：
 *    bin/flink run -s <savepointPath> [runArgs]
 *    例如：bin/flink run -s /tmp/flink-savepoints/savepoint-abc123 -c com.action.faulttolerance.SavepointExample
 * 
 * 注意事项：
 * - 保存点能够在程序更改的时候依然兼容，前提是状态的拓扑结构和数据类型不变
 * - 保存点中状态都是以算子 ID-状态名称这样的 key-value 组织起来的
 * - 对于没有设置 ID 的算子，Flink 默认会自动进行设置，所以在重新启动应用后可能会导致 ID 不同而无法兼容以前的状态
 * - 强烈建议在程序中为每一个算子手动指定 ID
 */

