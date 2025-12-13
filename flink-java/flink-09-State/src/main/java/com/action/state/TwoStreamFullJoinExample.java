package com.action.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 列表状态（ListState）示例
 * 
 * 在 Flink SQL 中，支持两条流的全量 Join，语法如下：
 * SELECT * FROM A INNER JOIN B WHERE A.id = B.id
 * 
 * 这里使用列表状态变量来实现这个 SQL 语句的功能
 * 将两条流的所有数据都保存下来，然后进行 Join
 */
public class TwoStreamFullJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建第一条流：(key, stream-name, timestamp)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
        );

        // 创建第二条流：(key, stream-name, timestamp)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> t, long l) {
                                return t.f2;
                            }
                        })
        );

        // 定义列表状态，保存流1和流2中的所有数据
        stream1.connect(stream2)
                .keyBy(data -> data.f0, data -> data.f0)  // 按照 key 分组
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    // 保存流1的所有数据
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    // 保存流2的所有数据
                    private ListState<Tuple3<String, String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化列表状态
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {
                        // 获取流1的数据，添加到列表状态中
                        stream1ListState.add(left);
                        // 遍历流2的所有数据，进行 join
                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            out.collect(left + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        // 获取流2的数据，添加到列表状态中
                        stream2ListState.add(right);
                        // 遍历流1的所有数据，进行 join
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            out.collect(left + " => " + right);
                        }
                    }
                })
                .print();

        env.execute();
    }
}

