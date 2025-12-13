package com.action.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 自定义并行数据源示例
 * 使用 ParallelSourceFunction 接口实现可并行的数据源
 * 
 * 与 SourceFunction 的区别：
 * - SourceFunction 只能单并行度运行
 * - ParallelSourceFunction 可以设置多个并行度
 * 
 * 特点说明：
 * - 适用场景：需要高吞吐量的数据生成场景
 * - 实现方式：实现 ParallelSourceFunction<T> 接口
 * - 并行度：可以设置多个并行度，提高数据生成速度
 * - 与 SourceFunction 的区别：
 *   - SourceFunction：只能单并行度运行
 *   - ParallelSourceFunction：可以设置多个并行度
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceCustomParallelTest"
 * 
 * 预期输出：
 * 1> -1234567890
 * 2> 987654321
 * 1> -456789012
 * 2> 123456789
 * ...
 */
public class SourceCustomParallelTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为 2，会启动 2 个并行任务同时生成数据
        env.addSource(new CustomSource()).setParallelism(2).print();

        env.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

/**
 * 注意事项：
 * 
 * 1. 并行度设置
 * 默认并行度：
 * // Flink 默认并行度等于 CPU 核心数
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * // 设置并行度
 * env.setParallelism(4);
 * 
 * 并行度影响：
 * - 数据会被分发到多个 subtask
 * - 影响性能和数据分布
 * - 相同 key 的数据始终发送到同一个 subtask
 * 
 * 2. 自定义数据源实现要点
 * - 必须实现 run() 和 cancel() 方法
 * - 使用 running 标志位控制循环
 * - 通过 ctx.collect() 发送数据
 * - 注意线程安全（如果使用共享变量）
 * 
 * 3. 常见错误
 * - 忘记实现 cancel() 方法
 * - 没有设置 running 标志位
 * - 在 run() 方法中使用阻塞操作导致无法取消
 * 
 * 4. 无界流处理
 * - 无界流程序不会自动结束
 * - 需要手动停止（IDE 停止按钮或 Ctrl+C）
 * 
 * 停止方式：
 * - 在 IDE 中点击停止按钮
 * - 在终端按 Ctrl+C
 * - 调用 cancel() 方法（自定义数据源）
 * 
 * 常见问题：
 * 
 * Q1: 并行度设置无效
 * 原因：某些数据源不支持并行
 * 解决方案：
 * - SourceFunction 只能单并行度
 * - 使用 ParallelSourceFunction 支持并行
 * - 检查数据源类型
 * 
 * Q2: 自定义数据源无法停止
 * 原因：未正确实现 cancel() 方法
 * 解决方案：
 * @Override
 * public void cancel() {
 *     running = false;  // 必须设置标志位
 * }
 */

