package com.action.kafka02springboot.consumer;

import com.action.kafka02springboot.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = KafkaConstants.TOPIC_USERS, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consume(String message) {
        log.info("#### → 消费消息: {}", message);
    }

    @KafkaListener(topics = KafkaConstants.TOPIC_USERS, groupId = KafkaConstants.CONSUMER_GROUP)
    public void consumeWithMetadata(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("消费消息: topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());

            processMessage(record.value());
            ack.acknowledge();
            log.info("偏移量已提交");
        } catch (Exception e) {
            log.error("消息处理失败: {}", e.getMessage());
        }
    }

    private void processMessage(String message) {
        log.info("处理消息: {}", message);
    }
}