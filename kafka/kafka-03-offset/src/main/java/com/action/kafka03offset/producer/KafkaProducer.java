package com.action.kafka03offset.producer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka消息生产者
 * 通过KafkaTemplate异步发送消息到指定Topic
 * 支持单条消息发送和批量消息发送
 */
@Service
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送单条演示消息
     * 使用异步方式发送，通过回调处理发送结果
     * 
     * @param message 要发送的消息内容
     */
    public void sendDemoMessage(String message) {
        log.info("发送演示消息: {}", message);

        // 异步发送消息，使用whenComplete处理结果
        kafkaTemplate.send(KafkaConstants.TOPIC_DEMO, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        // 发送成功，记录分区和偏移量信息
                        log.info("✅ 消息发送成功: partition={}, offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        // 发送失败，记录错误信息
                        log.error("❌ 消息发送失败: {}", ex.getMessage());
                    }
                });
    }

    /**
     * 批量发送测试消息
     * 用于演示不同偏移量策略的消费行为
     * 
     * @param count 要发送的消息数量
     */
    public void sendBatchMessages(int count) {
        log.info("开始批量发送 {} 条测试消息", count);
        for (int i = 1; i <= count; i++) {
            // 生成带时间戳的测试消息
            String message = "测试消息-" + i + "-" + System.currentTimeMillis();
            kafkaTemplate.send(KafkaConstants.TOPIC_DEMO, message);
        }
        log.info("✅ 批量消息发送完成");
    }
}


