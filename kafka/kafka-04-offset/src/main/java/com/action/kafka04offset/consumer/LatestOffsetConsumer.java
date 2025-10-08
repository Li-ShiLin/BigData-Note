package com.action.kafka04offset.consumer;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Latest 偏移量策略消费者
 * 自动将偏移量重置为最新偏移量
 * 
 * @author action
 * @since 2024
 */
@Slf4j
@Component
public class LatestOffsetConsumer {

    /**
     * 监听 latest 主题的消息
     * 使用 latest 偏移量策略，只会消费新产生的消息
     */
    @KafkaListener(
        topics = KafkaConstants.LATEST_TOPIC,
        groupId = KafkaConstants.LATEST_GROUP,
        containerFactory = "latestKafkaListenerContainerFactory"
    )
    public void handleLatestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== LATEST 偏移量策略消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.LATEST_GROUP);
        log.info("==============================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50); // 模拟处理时间
            log.info("消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听通用测试主题的消息 - 使用 latest 策略
     */
    @KafkaListener(
        topics = KafkaConstants.TEST_TOPIC,
        groupId = KafkaConstants.LATEST_GROUP + "-test",
        containerFactory = "latestKafkaListenerContainerFactory"
    )
    public void handleTestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== LATEST 策略 - 测试主题消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.LATEST_GROUP + "-test");
        log.info("====================================");
        
        // 模拟消息处理
        try {
            Thread.sleep(50);
            log.info("测试消息处理完成: {}", key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("测试消息处理被中断: {}", key);
        }
        
        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
