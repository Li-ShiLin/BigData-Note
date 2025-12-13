package com.action.source;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源示例
 * 使用自定义的 SourceFunction 实现类创建数据源
 * 
 * ClickSource 实现了 SourceFunction<Event> 接口
 * 会持续生成随机的点击事件数据
 * 
 * 特点说明：
 * - 适用场景：需要自定义数据生成逻辑的场景
 * - 实现方式：实现 SourceFunction<T> 接口
 * - 数据特点：可以是无界流（持续生成）或有界流（生成完成后结束）
 * - 优势：灵活控制数据生成逻辑
 * 
 * ClickSource 实现要点：
 * 1. 实现接口：SourceFunction<Event>
 * 2. 核心方法：
 *    - run()：数据生成逻辑，通过 ctx.collect() 发送数据
 *    - cancel()：取消数据生成，设置 running = false
 * 3. 控制机制：使用 running 标志位控制数据生成循环
 * 
 * 运行方式：
 * cd flink-02-DataSource
 * mvn compile exec:java -Dexec.mainClass="com.action.source.SourceCustomTest"
 * 
 * 预期输出：
 * SourceCustom> Event{user='Bob', url='./cart', timestamp=2024-11-12 00:16:23.456}
 * SourceCustom> Event{user='Mary', url='./home', timestamp=2024-11-12 00:16:24.456}
 * SourceCustom> Event{user='Alice', url='./prod?id=1', timestamp=2024-11-12 00:16:25.456}
 * ...
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 有了自定义的 source function，调用 addSource 方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();
    }
}

/**
 * 注意事项：
 * 
 * 1. 自定义数据源实现要点
 * - 必须实现 run() 和 cancel() 方法
 * - 使用 running 标志位控制循环
 * - 通过 ctx.collect() 发送数据
 * - 注意线程安全（如果使用共享变量）
 * 
 * 2. 常见错误
 * - 忘记实现 cancel() 方法
 * - 没有设置 running 标志位
 * - 在 run() 方法中使用阻塞操作导致无法取消
 * 
 * 3. 无界流处理
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
 * Q4: 自定义数据源无法停止
 * 原因：未正确实现 cancel() 方法
 * 解决方案：
 * @Override
 * public void cancel() {
 *     running = false;  // 必须设置标志位
 * }
 */

