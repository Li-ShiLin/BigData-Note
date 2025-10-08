package com.action.kafka04offset.service;

import com.action.kafka04offset.constants.KafkaConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka 生产者服务
 * 用于发送测试消息到不同主题
 *
 * @author action
 * @since 2024
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送消息到指定主题
     *
     * @param topic   主题名称
     * @param key     消息键
     * @param message 消息内容
     * @return 发送结果
     */
    public CompletableFuture<SendResult<String, String>> sendMessage(String topic, String key, String message) {
        log.info("发送消息到主题: {}, 键: {}, 内容: {}", topic, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("消息发送失败 - 主题: {}, 错误: {}", topic, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("消息发送成功 - 主题: {}, 分区: {}, 偏移量: {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 发送测试消息到 earliest 主题
     *
     * @param messageCount 消息数量
     */
    public void sendEarliestTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 earliest 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "earliest-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Earliest Test " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.EARLIEST_TOPIC, key, value);

            // 添加小延迟，确保消息有序
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 earliest 主题", messageCount);
    }

    /**
     * 发送测试消息到 latest 主题
     *
     * @param messageCount 消息数量
     */
    public void sendLatestTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 latest 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "latest-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Latest Test " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.LATEST_TOPIC, key, value);

            // 添加小延迟，确保消息有序
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 latest 主题", messageCount);
    }

    /**
     * 发送测试消息到 none 主题
     *
     * @param messageCount 消息数量
     */
    public void sendNoneTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到 none 主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "none-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "None Test " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.NONE_TOPIC, key, value);

            // 添加小延迟，确保消息有序
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到 none 主题", messageCount);
    }

    /**
     * 发送测试消息到通用测试主题
     *
     * @param messageCount 消息数量
     */
    public void sendTestMessages(int messageCount) {
        log.info("开始发送 {} 条消息到测试主题", messageCount);

        for (int i = 1; i <= messageCount; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "test-" + i;
            String value = KafkaConstants.MESSAGE_VALUE_PREFIX + "Test Message " + i +
                    " - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            sendMessage(KafkaConstants.TEST_TOPIC, key, value);

            // 添加小延迟，确保消息有序
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("发送消息被中断");
                break;
            }
        }

        log.info("完成发送 {} 条消息到测试主题", messageCount);
    }
}
