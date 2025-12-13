package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 状态一致性保证示例
 * 
 * 状态一致性有三种级别：
 * 1. 最多一次（AT-MOST-ONCE）：当任务发生故障时，直接重启，不恢复丢失的状态，也不重放丢失的数据
 * 2. 至少一次（AT-LEAST-ONCE）：所有数据都不会丢，肯定被处理了；不过不能保证只处理一次，有些数据会被重复处理
 * 3. 精确一次（EXACTLY-ONCE）：所有数据不仅不会丢失，而且只被处理一次，不会重复处理
 * 
 * 本示例演示如何通过检查点配置来保证不同的状态一致性级别
 */
public class StateConsistencyExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 配置检查点 ====================
        // 启用检查点，间隔时间 1 秒
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // ==================== 精确一次（EXACTLY-ONCE）配置 ====================
        // 设置检查点模式为精确一次，这是最严格的一致性保证
        // 可以真正意义上保证结果的绝对正确，在发生故障恢复后，就好像从未发生过故障一样
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000);

        // 设置最小间隔时间，确保检查点之间有足够的间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // 设置最大并发检查点数量为 1
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 开启外部持久化存储
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ==================== 至少一次（AT-LEAST-ONCE）配置示例 ====================
        // 如果希望使用至少一次语义，可以这样配置：
        // checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 至少一次语义对于大多数低延迟的流处理程序来说已经足够，而且处理效率会更高

        // ==================== 设置状态后端 ====================
        env.setStateBackend(new HashMapStateBackend());
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 设置重启策略 ====================
        // 固定延迟重启策略：失败后最多重启 3 次，每次重启间隔 10 秒
        // 这样可以保证在故障恢复时能够重放数据，达到至少一次或精确一次的语义
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // ==================== 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 使用状态统计每个用户的访问次数
        // 通过检查点机制，可以保证状态的一致性
        stream.keyBy(data -> data.user)
                .process(new ConsistentStateProcessFunction())
                .print("一致性状态统计");

        env.execute("State Consistency Example");
    }

    /**
     * 自定义处理函数：使用状态统计访问次数
     * 通过检查点机制，可以保证状态在故障恢复后的一致性
     */
    public static class ConsistentStateProcessFunction extends KeyedProcessFunction<String, Event, String> {
        // 定义状态，保存每个用户的访问次数
        private ValueState<Long> visitCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            // 状态会被保存到检查点中，故障恢复后可以从检查点恢复
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
            // 通过检查点机制，可以保证：
            // - 精确一次：每个数据只被处理一次，状态和输出结果都只包含一次处理
            // - 至少一次：数据不丢失，但可能重复处理
            out.collect(value.user + " 访问次数: " + count + " (时间戳: " + value.timestamp + ")");
        }
    }
}

/**
 * 状态一致性保证说明：
 * 
 * 1. 精确一次（EXACTLY-ONCE）：
 *    - 需要设置检查点模式为 EXACTLY_ONCE
 *    - 需要外部数据源支持数据重放（如 Kafka）
 *    - 需要外部存储系统支持幂等写入或事务写入
 *    - 可以真正意义上保证结果的绝对正确
 * 
 * 2. 至少一次（AT-LEAST-ONCE）：
 *    - 需要设置检查点模式为 AT_LEAST_ONCE
 *    - 需要外部数据源支持数据重放
 *    - 数据不会丢失，但可能重复处理
 *    - 适用于对重复处理不敏感的场景（如去重统计）
 * 
 * 3. 最多一次（AT-MOST-ONCE）：
 *    - 不启用检查点或检查点失败时不重启
 *    - 数据可能丢失，但不会重复处理
 *    - 适用于对数据丢失不敏感的场景
 */

