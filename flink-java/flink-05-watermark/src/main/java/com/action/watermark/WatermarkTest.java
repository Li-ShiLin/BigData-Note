package com.action.watermark;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 水位线生成示例
 * 演示 Flink 内置水位线生成器的使用
 * 
 * 包含两种场景：
 * 1. 有序流：使用 forMonotonousTimestamps()
 * 2. 乱序流：使用 forBoundedOutOfOrderness()
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 示例 1：有序流的水位线生成 ====================
        // 对于有序流，主要特点就是时间戳单调增长，所以永远不会出现迟到数据的问题
        // 这是周期性生成水位线的最简单的场景，直接拿当前最大的时间戳作为水位线就可以了
        
        env
                .addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 有序流水位线生成器
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .print("有序流");

        // ==================== 示例 2：乱序流的水位线生成 ====================
        // 由于乱序流中需要等待迟到数据到齐，所以必须设置一个固定量的延迟时间
        // 这时生成水位线的时间戳，就是当前数据流中最大的时间戳减去延迟的结果
        
        // 注意：如果要测试乱序流，需要取消注释下面的代码，并注释掉上面的有序流代码
        /*
        env
                .addSource(new ClickSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为 5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .print("乱序流");
        */

        env.execute();
    }
}

