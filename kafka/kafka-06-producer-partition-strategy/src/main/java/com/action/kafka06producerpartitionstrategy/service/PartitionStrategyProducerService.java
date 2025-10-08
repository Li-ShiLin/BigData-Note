package com.action.kafka06producerpartitionstrategy.service;

import com.action.kafka06producerpartitionstrategy.constants.KafkaConstants;
import com.action.kafka06producerpartitionstrategy.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 分区策略生产者服务
 */
@Slf4j
@Service
public class PartitionStrategyProducerService {

    private final KafkaTemplate<String, String> defaultKafkaTemplate;
    private final KafkaTemplate<String, String> customPartitionKafkaTemplate;
    private final KafkaTemplate<String, String> keyBasedPartitionKafkaTemplate;
    private final KafkaTemplate<String, String> roundRobinPartitionKafkaTemplate;

    public PartitionStrategyProducerService(
            @Qualifier("defaultKafkaTemplate") KafkaTemplate<String, String> defaultKafkaTemplate,
            @Qualifier("customPartitionKafkaTemplate") KafkaTemplate<String, String> customPartitionKafkaTemplate,
            @Qualifier("keyBasedPartitionKafkaTemplate") KafkaTemplate<String, String> keyBasedPartitionKafkaTemplate,
            @Qualifier("roundRobinPartitionKafkaTemplate") KafkaTemplate<String, String> roundRobinPartitionKafkaTemplate) {
        this.defaultKafkaTemplate = defaultKafkaTemplate;
        this.customPartitionKafkaTemplate = customPartitionKafkaTemplate;
        this.keyBasedPartitionKafkaTemplate = keyBasedPartitionKafkaTemplate;
        this.roundRobinPartitionKafkaTemplate = roundRobinPartitionKafkaTemplate;
    }

    /**
     * 使用默认分区策略发送消息
     */
    public void sendWithDefaultPartition(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = defaultKafkaTemplate
                .send(KafkaConstants.TOPIC_DEFAULT_PARTITION, key, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("默认分区策略 - 消息发送成功: topic={}, partition={}, offset={}, key={}, value={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key, message);
            } else {
                log.error("默认分区策略 - 消息发送失败: key={}, value={}, error={}", key, message, ex.getMessage());
            }
        });
    }

    /**
     * 使用自定义分区策略发送消息
     */
    public void sendWithCustomPartition(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = customPartitionKafkaTemplate
                .send(KafkaConstants.TOPIC_CUSTOM_PARTITION, key, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("自定义分区策略 - 消息发送成功: topic={}, partition={}, offset={}, key={}, value={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key, message);
            } else {
                log.error("自定义分区策略 - 消息发送失败: key={}, value={}, error={}", key, message, ex.getMessage());
            }
        });
    }

    /**
     * 使用基于 Key 的分区策略发送消息
     */
    public void sendWithKeyBasedPartition(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = keyBasedPartitionKafkaTemplate
                .send(KafkaConstants.TOPIC_KEY_BASED_PARTITION, key, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("基于Key分区策略 - 消息发送成功: topic={}, partition={}, offset={}, key={}, value={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key, message);
            } else {
                log.error("基于Key分区策略 - 消息发送失败: key={}, value={}, error={}", key, message, ex.getMessage());
            }
        });
    }

    /**
     * 使用轮询分区策略发送消息
     */
    public void sendWithRoundRobinPartition(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = roundRobinPartitionKafkaTemplate
                .send(KafkaConstants.TOPIC_ROUND_ROBIN_PARTITION, key, message);
        
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("轮询分区策略 - 消息发送成功: topic={}, partition={}, offset={}, key={}, value={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key, message);
            } else {
                log.error("轮询分区策略 - 消息发送失败: key={}, value={}, error={}", key, message, ex.getMessage());
            }
        });
    }

    /**
     * 批量发送消息演示不同分区策略
     */
    public void sendBatchMessages() {
        log.info("开始批量发送消息演示不同分区策略...");
        
        // 默认分区策略演示
        for (int i = 1; i <= 10; i++) {
            sendWithDefaultPartition("key-" + i, "默认分区策略消息-" + i);
        }
        
        // 自定义分区策略演示
        sendWithCustomPartition("ORDER-001", "订单消息-001");
        sendWithCustomPartition("USER-001", "用户消息-001");
        sendWithCustomPartition("SYSTEM-001", "系统消息-001");
        sendWithCustomPartition("OTHER-001", "其他消息-001");
        
        // 基于 Key 分区策略演示
        for (int i = 1; i <= 10; i++) {
            sendWithKeyBasedPartition("user-" + i, "基于Key分区消息-" + i);
        }
        
        // 轮询分区策略演示
        for (int i = 1; i <= 10; i++) {
            sendWithRoundRobinPartition(null, "轮询分区消息-" + i);
        }
        
        log.info("批量发送消息完成");
    }
}
