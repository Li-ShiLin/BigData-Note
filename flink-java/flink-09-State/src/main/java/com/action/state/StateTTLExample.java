package com.action.state;

import com.action.ClickSource;
import com.action.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 状态生存时间（TTL）示例
 * 
 * 在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存储空间的耗尽
 * 可以配置状态的"生存时间"（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除
 * 
 * 注意：目前的 TTL 设置只支持处理时间
 */
public class StateTTLExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                .process(new StateTTLProcessFunction())
                .print();

        env.execute();
    }

    public static class StateTTLProcessFunction extends KeyedProcessFunction<String, Event, String> {
        private ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建状态 TTL 配置
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))  // 状态生存时间为 10 秒
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 只有创建状态和更改状态时更新失效时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 从不返回过期值
                    .build();

            // 创建状态描述器并启用 TTL
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("count", Long.class);
            stateDescriptor.enableTimeToLive(ttlConfig);

            countState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 访问状态，如果状态已过期会自动清除
            Long count = countState.value();
            if (count == null) {
                count = 1L;
            } else {
                count++;
            }
            countState.update(count);
            out.collect(ctx.getCurrentKey() + " 访问次数: " + count);
        }
    }
}

