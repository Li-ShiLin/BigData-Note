package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 最早偏移量策略消费者
 * 使用earliestOffsetFactory，从最早位置开始消费
 * 新消费者组会收到所有历史消息，包括启动前已存在的消息
 */
@Component
@Slf4j
public class EarliestOffsetConsumer {

    /**
     * 消费最早偏移量策略的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用earliestOffsetFactory配置
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_EARLIEST,
            containerFactory = "earliestOffsetFactory"
    )
    public void consumeEarliest(String message) {
        log.info("🟢 [EARLIEST组] 消费消息: {}", message);
    }
}


