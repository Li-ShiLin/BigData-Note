package com.action.kafka01helloworld.simpleKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置Kafka服务器地址
        props.put("bootstrap.servers", "192.168.56.10:9092");
        // 设置数据key的序列化处理类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置数据value的序列化处理类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 发送一条消息到名为 "test-topic" 的主题
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key", "Hello, Kafka!");
        producer.send(record);
        // 关闭生产者
        producer.close();
    }
}