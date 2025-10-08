package com.action.kafka02springboot.producer;

import com.action.kafka02springboot.config.KafkaConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;


@Service
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        log.info("#### → 发送消息: {}", message);

        // 异步发送
        kafkaTemplate.send(KafkaConstants.TOPIC_USERS, message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        // 发送成功
                        log.info("消息发送成功: topic={}, partition={}, offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        // 发送失败
                        log.error("消息发送失败: {}", ex.getMessage());
                    }
                });
    }

    // 同步发送示例
    public void sendMessageSync(String message) throws Exception {
        SendResult<String, String> result = kafkaTemplate.send(KafkaConstants.TOPIC_USERS, message)
                .get(3, TimeUnit.SECONDS);
        log.info("同步发送成功: {}", result.getRecordMetadata().offset());
    }
}