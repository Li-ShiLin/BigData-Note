package com.action.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 输出到 Kafka 示例
 * 使用 FlinkKafkaProducer 将数据写入 Kafka
 * <p>
 * 特点说明：
 * - 功能：将数据流写入 Kafka 主题（topic）
 * - 精确一次语义：FlinkKafkaProducer 支持端到端的精确一次（exactly once）语义保证
 * - 两阶段提交：使用 TwoPhaseCommitSinkFunction 实现事务性保证
 * - 使用场景：数据管道、实时数据流、事件流处理等
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "server01:9092");

        // 注意：这里需要先创建 input/clicks.csv 文件，或者使用其他数据源
        // DataStreamSource<String> stream = env.readTextFile("input/clicks.csv");

        // 使用 fromElements 作为示例数据源
        DataStreamSource<String> stream = env.fromElements(
                "Mary,./home,1000",
                "Bob,./cart,2000",
                "Alice,./prod?id=100,3000"
        );

        stream
                .addSink(new FlinkKafkaProducer<String>(
                        "clicks",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}

