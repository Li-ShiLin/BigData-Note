package com.action.kafka03offset.consumer;

import com.action.kafka03offset.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 最新偏移量策略消费者
 * 使用latestOffsetFactory，从最新位置开始消费
 * 新消费者组不会收到历史消息，只消费启动后的新消息
 */
@Component
@Slf4j
public class LatestOffsetConsumer {

    /**
     * 消费最新偏移量策略的消息
     * 使用@KafkaListener注解监听指定Topic和消费者组
     * containerFactory指定使用latestOffsetFactory配置
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEMO,
            groupId = KafkaConstants.CONSUMER_GROUP_LATEST,
            containerFactory = "latestOffsetFactory"
    )
    public void consumeLatest(String message) {
        log.info("🔵 [LATEST组] 消费消息: {}", message);
    }
}


