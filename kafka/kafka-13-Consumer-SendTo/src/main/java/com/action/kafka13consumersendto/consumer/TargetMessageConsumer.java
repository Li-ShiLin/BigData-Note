package com.action.kafka13consumersendto.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 目标消息消费者
 * 
 * 功能说明：
 * 1) 监听转发后的目标消息
 * 2) 接收来自消息转发的消息
 * 3) 演示消息转发的完整流程
 * 4) 提供消息接收确认
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听目标主题
 * - 使用 Acknowledgment 进行手动消息确认
 * - 记录转发消息的详细信息
 * - 演示消息转发的最终接收
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class TargetMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(TargetMessageConsumer.class);

    /**
     * 接收转发消息演示
     * 
     * 执行流程：
     * 1) 监听 target-topic 主题
     * 2) 使用 targetGroup 消费者组
     * 3) 接收来自消息转发的消息
     * 4) 打印转发消息的详细信息
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = "${kafka.topic.target:target-topic}", groupId = "targetGroup")
    public void receiveForwardedMessage(ConsumerRecord<String, String> record,
                                      @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                      @Header(value = KafkaHeaders.OFFSET) String offset,
                                      Acknowledgment ack) {
        try {
            // 打印接收到的转发消息
            log.info("[目标主题] === 接收转发消息演示 ===");
            log.info("[目标主题] 转发消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            log.info("[目标主题] 消息时间戳: {}", record.timestamp());
            log.info("[目标主题] 消息头: {}", record.headers());
            log.info("[目标主题] 完整消息记录: {}", record.toString());
            log.info("[目标主题] ==========================================");
            
            // 模拟消息处理逻辑
            processForwardedMessage(record);
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("[目标主题] 转发消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
        } catch (Exception e) {
            log.error("[目标主题] 处理转发消息时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
        }
    }

    /**
     * 模拟转发消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟对转发消息的业务处理
     * 2) 演示消息转发的最终处理流程
     * 3) 用于验证消息转发的完整性
     * 
     * @param record 转发消息记录
     * @throws Exception 处理异常
     */
    private void processForwardedMessage(ConsumerRecord<String, String> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();
        String value = record.value();
        
        log.info("[目标主题] 开始处理转发消息: topic={}, partition={}, offset={}, key={}, value={}", 
                topic, partition, offset, key, value);
        
        // 模拟处理时间
        Thread.sleep(100);
        
        // 根据消息内容进行不同的处理
        if (value.contains("[FORWARDED]")) {
            log.info("[目标主题] 处理基础转发消息: {}", value);
        } else if (value.contains("[ENHANCED]")) {
            log.info("[目标主题] 处理增强转发消息: {}", value);
        } else if (value.contains("[CONDITIONAL-FORWARD]")) {
            log.info("[目标主题] 处理条件转发消息: {}", value);
        } else {
            log.info("[目标主题] 处理其他类型转发消息: {}", value);
        }
        
        log.info("[目标主题] 转发消息处理完成: topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
