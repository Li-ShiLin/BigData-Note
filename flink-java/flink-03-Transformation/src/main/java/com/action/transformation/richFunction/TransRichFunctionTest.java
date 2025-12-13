package com.action.transformation.richFunction;


import com.action.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Rich Function 富函数示例
 * 演示 Rich Function 的使用，可以访问运行时上下文
 * <p>
 * 特点说明：
 * - 功能：Rich Function 提供了生命周期方法和运行时上下文访问
 * - 生命周期方法：
 * - open()：任务初始化时调用，可以用于初始化资源
 * - close()：任务结束时调用，可以用于清理资源
 * - 运行时上下文：通过 getRuntimeContext() 可以访问：
 * - 并行子任务索引
 * - 并行度
 * - 任务名称
 * - 分布式缓存
 * - 累加器
 * - 使用场景：需要访问运行时信息、初始化资源、使用分布式缓存等
 * <p>
 * 预期输出：
 * 索引为 0 的任务开始
 * 索引为 1 的任务开始
 * 1000
 * 2000
 * 5000
 * 60000
 * 索引为 0 的任务结束
 * 索引为 1 的任务结束
 */
public class TransRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        // 将点击事件转换成长整型的时间戳输出
        clicks.map(new RichMapFunction<Event, Long>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
                    }

                    @Override
                    public Long map(Event value) throws Exception {
                        return value.timestamp;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
                    }
                })
                .print();

        env.execute();
    }
}

