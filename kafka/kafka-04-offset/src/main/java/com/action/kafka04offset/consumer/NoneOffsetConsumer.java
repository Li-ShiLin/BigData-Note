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
 * None 偏移量策略消费者
 * 如果没有为消费者组找到以前的偏移量，则向消费者抛出异常
 * 
 * @author action
 * @since 2024
 */
@Slf4j
@Component
public class NoneOffsetConsumer {

    /**
     * 监听 none 主题的消息
     * 使用 none 偏移量策略，如果没有找到偏移量会抛出异常
     * 注意：此消费者会在应用启动时延迟启动，避免立即遇到无偏移量异常
     */
    @KafkaListener(
        topics = KafkaConstants.NONE_TOPIC,
        groupId = KafkaConstants.NONE_GROUP,
        containerFactory = "noneKafkaListenerContainerFactory",
        errorHandler = "noneErrorHandler",
        autoStartup = "false"
    )
    public void handleNoneMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== NONE 偏移量策略消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.NONE_GROUP);
        log.info("============================");
        
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
     * 监听通用测试主题的消息 - 使用 none 策略
     * 注意：这个消费者可能会因为找不到偏移量而抛出异常
     */
    @KafkaListener(
        topics = KafkaConstants.TEST_TOPIC,
        groupId = KafkaConstants.NONE_GROUP + "-test",
        containerFactory = "noneKafkaListenerContainerFactory",
        errorHandler = "noneErrorHandler",
        autoStartup = "false"
    )
    public void handleTestMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        log.info("=== NONE 策略 - 测试主题消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", key);
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.NONE_GROUP + "-test");
        log.info("==================================");
        
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
