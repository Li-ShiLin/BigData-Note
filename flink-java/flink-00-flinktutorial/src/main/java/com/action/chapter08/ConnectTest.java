package com.action.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Connect 演示：将两个不同类型的数据流共享状态管道，通过 CoMapFunction 在一个算子中分别处理。
 * 与 union 区别：Connect 保留各自的类型信息，可针对 map1/map2 写差异化逻辑。
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1,2,3);
        DataStream<Long> stream2 = env.fromElements(1L,2L,3L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                // map1 只处理第一个流
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) {
                // map2 只处理第二个流
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }
}

