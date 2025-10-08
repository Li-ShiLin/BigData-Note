package com.action.kafka06producerpartitionstrategy.consumer;

import com.action.kafka06producerpartitionstrategy.constants.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 分区策略消费者
 */
@Slf4j
@Component
public class PartitionStrategyConsumer {

    /**
     * 消费默认分区策略主题的消息
     */
    @KafkaListener(topics = KafkaConstants.TOPIC_DEFAULT_PARTITION, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeDefaultPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        log.info("默认分区策略 - 消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                topic, partition, offset, key, message);
        
        // 手动确认消息
        acknowledgment.acknowledge();
    }

    /**
     * 消费自定义分区策略主题的消息
     */
    @KafkaListener(topics = KafkaConstants.TOPIC_CUSTOM_PARTITION, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeCustomPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        log.info("自定义分区策略 - 消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                topic, partition, offset, key, message);
        
        // 手动确认消息
        acknowledgment.acknowledge();
    }

    /**
     * 消费基于 Key 分区策略主题的消息
     */
    @KafkaListener(topics = KafkaConstants.TOPIC_KEY_BASED_PARTITION, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeKeyBasedPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        log.info("基于Key分区策略 - 消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                topic, partition, offset, key, message);
        
        // 手动确认消息
        acknowledgment.acknowledge();
    }

    /**
     * 消费轮询分区策略主题的消息
     */
    @KafkaListener(topics = KafkaConstants.TOPIC_ROUND_ROBIN_PARTITION, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeRoundRobinPartition(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {
        
        log.info("轮询分区策略 - 消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                topic, partition, offset, key, message);
        
        // 手动确认消息
        acknowledgment.acknowledge();
    }

    /**
     * 通用消费者 - 监听所有主题
     */
    @KafkaListener(topics = {
            KafkaConstants.TOPIC_DEFAULT_PARTITION,
            KafkaConstants.TOPIC_CUSTOM_PARTITION,
            KafkaConstants.TOPIC_KEY_BASED_PARTITION,
            KafkaConstants.TOPIC_ROUND_ROBIN_PARTITION
    }, groupId = KafkaConstants.CONSUMER_GROUP + "-all")
    public void consumeAllTopics(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("通用消费者 - 消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        
        // 手动确认消息
        acknowledgment.acknowledge();
    }
}
