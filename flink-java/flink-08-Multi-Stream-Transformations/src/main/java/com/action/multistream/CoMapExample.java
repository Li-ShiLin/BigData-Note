package com.action.multistream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Connect 示例：连接两条流
 * 
 * Connect 操作允许流的数据类型不同，得到的是 ConnectedStreams
 * 需要使用 CoMapFunction、CoFlatMapFunction 或 CoProcessFunction 来处理
 * 注意：Connect 只能连接两条流，而 Union 可以连接多条流
 */
public class CoMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建两条数据类型不同的流
        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        // 连接两条流，得到 ConnectedStreams
        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        
        // 使用 CoMapFunction 处理连接流
        // map1 处理第一条流的数据，map2 处理第二条流的数据
        SingleOutputStreamOperator<String> result = connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                return "Integer: " + value;
            }

            @Override
            public String map2(Long value) {
                return "Long: " + value;
            }
        });

        result.print();

        env.execute();
    }
}

