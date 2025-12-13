package com.action.watermark;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义断点式水位线生成器示例
 * 演示如何实现基于事件触发的断点式水位线生成
 * 
 * 断点式生成器会不停地检测 onEvent() 中的事件，
 * 当发现带有水位线信息的特殊事件时，就立即发出水位线。
 * 一般来说，断点式生成器不会通过 onPeriodicEmit() 发出水位线。
 */
public class CustomPunctuatedWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomPunctuatedWatermarkStrategy())
                .print();

        env.execute();
    }

    /**
     * 自定义断点式水位线策略
     */
    public static class CustomPunctuatedWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPunctuatedGenerator();
        }
    }

    /**
     * 自定义断点式水位线生成器
     */
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event r, long eventTimestamp, WatermarkOutput output) {
            // 只有在遇到特定的 user 时，才发出水位线
            if (r.user.equals("Mary")) {
                output.emitWatermark(new Watermark(r.timestamp - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事情，因为我们在 onEvent 方法中发射了水位线
        }
    }
}

