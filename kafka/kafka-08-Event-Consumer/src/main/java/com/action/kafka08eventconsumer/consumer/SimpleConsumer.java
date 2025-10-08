package com.action.kafka08eventconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 简单消费者 - 演示基本的消息消费功能
 * 
 * 功能说明：
 * 1) 自动提交模式：消费消息后自动提交偏移量
 * 2) 单条消费：一次处理一条消息
 * 3) 日志记录：记录消费的消息详情
 * 4) 异常处理：捕获并记录消费异常
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解标记消费方法
 * - 支持 ConsumerRecord 和 @Payload/@Header 两种参数方式
 * - 自动提交模式下无需手动确认消息
 * - 异常会中断消费，需要重启应用才能继续
 * 
 * 关键参数说明：
 * - ConsumerRecord: 包含完整的消息信息（topic、partition、offset、key、value等）
 * - @Payload: 直接获取消息值，简化参数处理
 * - @Header: 获取消息头信息，如分区号、偏移量等
 * - Acknowledgment: 手动确认接口（自动提交模式下为null）
 */
@Component
public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    /**
     * 消费者组ID配置
     */
    @Value("${spring.kafka.consumer.group-id:demo-consumer-group}")
    private String groupId;

    /**
     * Topic名称配置
     */
    @Value("${demo.topic.name:demo-consumer-topic}")
    private String topicName;

    /**
     * 简单消息消费方法（使用 ConsumerRecord）
     * 
     * 执行时机：当有消息到达指定 topic 时
     * 作用：处理单条消息，记录消费详情
     * 
     * 参数说明：
     * - topics: 监听的 topic 名称，支持多个 topic
     * - groupId: 消费者组ID，用于负载均衡和偏移量管理
     * - containerFactory: 使用的监听器容器工厂
     * 
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送测试消息示例
     * echo "Hello Kafka Consumer" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "key1:test message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * @param record 完整的消息记录
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(ConsumerRecord<String, String> record) {
        try {
            // 消费信息日志
            log.info("📦 Simple Consumer (ConsumerRecord) 👥 Group: {} 📋 Topic: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} ⏰ Timestamp: {} 📄 Headers: {}", 
                    groupId, record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), record.headers());
            
            // 模拟消息处理逻辑
            processMessage(record.value());
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志
            log.error("Error processing message from topic={}, partition={}, offset={}", 
                     record.topic(), record.partition(), record.offset(), ex);
        }
    }

    /**
     * 简单消息消费方法（使用 @Payload 和 @Header）
     * 
     * 执行时机：当有消息到达指定 topic 时
     * 作用：使用注解方式简化参数处理
     * 
     * 参数说明：
     * - @Payload: 直接获取消息值，类型自动转换
     * - @Header: 获取指定的消息头信息
     * 
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送测试消息示例
     * echo "Payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "payload-key:Payload test with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * @param message 消息值
     * @param partition 分区号
     * @param offset 偏移量
     * @param key 消息键
     */
    @KafkaListener(
            topics = "${demo.topic.name:demo-consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-payload",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessageWithPayload(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        
        try {
            // 消费信息日志
            log.info("📦 Simple Consumer (@Payload/@Header) 👥 Group: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} 🔄 Acknowledgment: Auto", 
                    groupId + "-payload", partition, offset, key != null ? key : "null", message);
            
            // 模拟消息处理逻辑
            processMessage(message);
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志
            log.error("Error processing message from partition={}, offset={}", 
                     partition, offset, ex);
        }
    }

    /**
     * 消息处理逻辑
     * 
     * 作用：模拟实际的消息处理业务逻辑
     * 实现：可以根据消息内容进行不同的处理
     * 
     * @param message 消息内容
     */
    private void processMessage(String message) {
        try {
            // 模拟业务处理时间
            Thread.sleep(100);
            
            // 根据消息内容进行不同处理
            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
            } else {
                log.info("Processing normal message: {}", message);
            }
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
        }
    }
}
