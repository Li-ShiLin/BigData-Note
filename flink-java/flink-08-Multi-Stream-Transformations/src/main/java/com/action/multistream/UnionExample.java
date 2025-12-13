package com.action.multistream;

import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Union 示例：联合多条流
 * 
 * Union 操作要求流中的数据类型必须相同，合并之后的新流会包括所有流中的元素
 * 在事件时间语义下，合流之后的水位线以最小的那个为准（木桶原理）
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流（从 socket 读取，这里用 fromElements 模拟）
        SingleOutputStreamOperator<Event> stream1 = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );
        stream1.print("stream1");

        // 创建第二条流
        SingleOutputStreamOperator<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 3000L),
                new Event("Cary", "./prod?id=1", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );
        stream2.print("stream2");

        // 合并两条流：union 可以合并多条流，但数据类型必须相同
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        // 输出当前水位线，观察合流后水位线的变化
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print("union");

        env.execute();
    }
}

