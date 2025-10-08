package com.action.kafka01helloworld.simpleKafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置Kafka服务器地址
        props.put("bootstrap.servers", "192.168.56.10:9092");
        // 设置消费分组名
        props.put("group.id", "test-group");
        // 设置数据key的反序列化处理类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置数据value的反序列化处理类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置自动提交offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        // 创建消费者实例
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        consumer.subscribe(Collections.singletonList("test-topic"));
        // 循环拉取消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                records.forEach(record -> {
                    System.out.printf("收到消息: offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                });
            }
        } finally {
            consumer.close();
        }
    }
}