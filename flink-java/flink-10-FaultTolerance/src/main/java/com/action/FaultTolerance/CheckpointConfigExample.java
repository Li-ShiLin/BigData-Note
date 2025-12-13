package com.action.faulttolerance;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * 检查点配置示例
 * 
 * 演示如何配置 Flink 的检查点机制，包括：
 * - 启用检查点
 * - 设置检查点模式（精确一次/至少一次）
 * - 配置检查点超时时间
 * - 配置最小间隔时间
 * - 配置最大并发检查点数量
 * - 配置外部持久化存储
 * - 配置不对齐检查点
 */
public class CheckpointConfigExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 启用检查点 ====================
        // 每隔 1 秒启动一次检查点保存
        // 如果不传参数直接启用检查点，默认的间隔周期为 500 毫秒，这种方式已经被弃用
        env.enableCheckpointing(1000);

        // ==================== 2. 获取检查点配置对象 ====================
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // ==================== 3. 设置检查点模式 ====================
        // 设置检查点一致性的保证级别
        // EXACTLY_ONCE：精确一次（默认），保证数据只处理一次
        // AT_LEAST_ONCE：至少一次，保证数据不丢失，但可能重复处理
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // ==================== 4. 设置检查点超时时间 ====================
        // 用于指定检查点保存的超时时间，超时没完成就会被丢弃掉
        // 这里设置为 1 分钟
        checkpointConfig.setCheckpointTimeout(60000);

        // ==================== 5. 设置最小间隔时间 ====================
        // 用于指定在上一个检查点完成之后，检查点协调器最快等多久可以触发保存下一个检查点
        // 这就意味着即使已经达到了周期触发的时间点，只要距离上一个检查点完成的间隔不够，就依然不能开启下一次检查点的保存
        // 当指定这个参数时，maxConcurrentCheckpoints 的值强制为 1
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        // ==================== 6. 设置最大并发检查点数量 ====================
        // 用于指定运行中的检查点最多可以有多少个
        // 由于每个任务的处理进度不同，完全可能出现后面的任务还没完成前一个检查点的保存、前面任务已经开始保存下一个检查点了
        // 如果前面设置了 minPauseBetweenCheckpoints，则这个参数就不起作用了
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // ==================== 7. 开启外部持久化存储 ====================
        // 用于开启检查点的外部持久化，而且默认在作业失败的时候不会自动清理
        // RETAIN_ON_CANCELLATION：作业取消的时候也会保留外部检查点
        // DELETE_ON_CANCELLATION：在作业取消的时候会自动删除外部检查点，但是如果是作业失败退出，则会保留检查点
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ==================== 8. 启用不对齐检查点 ====================
        // 不再执行检查点的分界线对齐操作，启用之后可以大大减少产生背压时的检查点保存时间
        // 这个设置要求检查点模式必须为 EXACTLY_ONCE，并且并发的检查点个数为 1
        checkpointConfig.enableUnalignedCheckpoints();

        // ==================== 9. 设置检查点异常时是否让整个任务失败 ====================
        // 用于指定在检查点发生异常的时候，是否应该让任务直接失败退出
        // 默认为 true，如果设置为 false，则任务会丢弃掉检查点然后继续运行
        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        // ==================== 10. 设置状态后端 ====================
        // 使用 HashMapStateBackend，将状态存储在内存中
        env.setStateBackend(new HashMapStateBackend());

        // ==================== 11. 设置检查点存储 ====================
        // 配置存储检查点到文件系统（生产环境推荐使用 HDFS 等分布式文件系统）
        // 这里使用本地文件系统作为示例
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== 12. 设置重启策略 ====================
        // 固定延迟重启策略：失败后最多重启 3 次，每次重启间隔 10 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // ==================== 13. 创建数据流并处理 ====================
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 简单的处理逻辑：统计每个用户的访问次数
        stream.keyBy(data -> data.user)
                .map(data -> {
                    // 模拟处理逻辑
                    return data.user + " 访问了 " + data.url;
                })
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        // 模拟输出到外部系统
                        System.out.println("输出: " + value);
                    }
                });

        env.execute("Checkpoint Config Example");
    }
}

