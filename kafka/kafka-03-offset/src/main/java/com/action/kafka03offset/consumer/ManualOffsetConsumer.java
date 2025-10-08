package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 手动偏移量控制消费者
 * 使用manualOffsetFactory，手动控制偏移量提交
 * 只有在消息处理成功后才提交偏移量，确保消息不丢失
 */
@Component
@Slf4j
public class ManualOffsetConsumer {

    /**
     * 消费手动偏移量控制的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用manualOffsetFactory配置
     * Acknowledgment参数用于手动提交偏移量
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_MANUAL,
            containerFactory = "manualOffsetFactory"
    )
    public void consumeManual(String message, Acknowledgment ack) {
        try {
            log.info("🟠 [MANUAL组] 消费消息: {}", message);
            // 处理消息
            processMessage(message);
            // 消息处理成功后手动提交偏移量
            ack.acknowledge();
            log.info("✅ 偏移量已提交");
        } catch (Exception e) {
            log.error("❌ 消息处理失败: {}", e.getMessage());
            // 处理失败时不提交偏移量，消息会被重新消费
        }
    }

    /**
     * 模拟消息处理逻辑
     * 如果消息包含"error"关键字，则抛出异常模拟处理失败
     */
    private void processMessage(String message) {
        if (message.contains("error")) {
            throw new RuntimeException("模拟业务处理异常");
        }
    }
}


