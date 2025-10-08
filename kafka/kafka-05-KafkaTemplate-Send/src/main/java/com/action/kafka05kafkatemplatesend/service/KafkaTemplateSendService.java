package com.action.kafka05kafkatemplatesend.service;

import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
import com.action.kafka05kafkatemplatesend.model.OrderMessage;
import com.action.kafka05kafkatemplatesend.model.SystemEventMessage;
import com.action.kafka05kafkatemplatesend.model.UserMessage;
import com.action.kafka0common.common.CommonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * KafkaTemplate 发送方法测试服务
 * 测试KafkaTemplate的各种send重载方法
 *
 * @author action
 * @since 2024
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaTemplateSendService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 获取KafkaTemplate实例，用于访问默认主题等信息
     */
    public KafkaTemplate<String, String> getKafkaTemplate() {
        return kafkaTemplate;
    }

    /**
     * 测试方法1: send(String topic, V data)
     * 只发送消息值，不指定键
     */
    public CompletableFuture<SendResult<String, String>> sendSimpleMessage(String topic, String message) {
        log.info("=== 测试方法1: send(topic, data) ===");
        log.info("主题: {}, 消息: {}", topic, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("简单消息发送失败 - 主题: {}, 错误: {}", topic, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("简单消息发送成功 - 主题: {}, 分区: {}, 偏移量: {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法2: send(String topic, K key, V data)
     * 发送消息键和消息值
     */
    public CompletableFuture<SendResult<String, String>> sendKeyValueMessage(String topic, String key, String message) {
        log.info("=== 测试方法2: send(topic, key, data) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("键值消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("键值消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法3: send(String topic, Integer partition, K key, V data)
     * 指定分区发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendPartitionMessage(String topic, Integer partition, String key, String message) {
        log.info("=== 测试方法3: send(topic, partition, key, data) ===");
        log.info("主题: {}, 分区: {}, 键: {}, 消息: {}", topic, partition, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, partition, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("分区消息发送失败 - 主题: {}, 分区: {}, 键: {}, 错误: {}", topic, partition, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("分区消息发送成功 - 主题: {}, 分区: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        topic, partition, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法4: send(String topic, Integer partition, Long timestamp, K key, V data)
     * 指定分区和时间戳发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendTimestampMessage(String topic, Integer partition, Long timestamp, String key, String message) {
        log.info("=== 测试方法4: send(topic, partition, timestamp, key, data) ===");
        log.info("主题: {}, 分区: {}, 时间戳: {}, 键: {}, 消息: {}", topic, partition, timestamp, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, partition, timestamp, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("时间戳消息发送失败 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 错误: {}",
                        topic, partition, timestamp, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("时间戳消息发送成功 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        topic, partition, timestamp, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法5: send(ProducerRecord<K, V> record)
     * 使用ProducerRecord发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendProducerRecordMessage(String topic, String key, String message) {
        log.info("=== 测试方法5: send(ProducerRecord) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("ProducerRecord消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("ProducerRecord消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法6: send(Message<?> message)
     * 使用Spring Message发送消息
     */
    public CompletableFuture<SendResult<String, String>> sendSpringMessage(String topic, String key, String message) {
        log.info("=== 测试方法6: send(Message) ===");
        log.info("主题: {}, 键: {}, 消息: {}", topic, key, message);

        Message<String> springMessage = MessageBuilder
                .withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.CORRELATION_ID, "correlation-" + System.currentTimeMillis())
                .setHeader("custom-header", "custom-value")
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(springMessage);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Spring Message消息发送失败 - 主题: {}, 键: {}, 错误: {}", topic, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("Spring Message消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法7: sendDefault(V data)
     * 使用默认主题发送简单消息
     */
    public CompletableFuture<SendResult<String, String>> sendDefaultSimpleMessage(String message) {
        log.info("=== 测试方法7: sendDefault(data) ===");
        log.info("默认主题: {}, 消息: {}", kafkaTemplate.getDefaultTopic(), message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.sendDefault(message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("默认主题简单消息发送失败 - 主题: {}, 错误: {}", kafkaTemplate.getDefaultTopic(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("默认主题简单消息发送成功 - 主题: {}, 分区: {}, 偏移量: {}",
                        kafkaTemplate.getDefaultTopic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法8: sendDefault(K key, V data)
     * 使用默认主题发送键值消息
     */
    public CompletableFuture<SendResult<String, String>> sendDefaultKeyValueMessage(String key, String message) {
        log.info("=== 测试方法8: sendDefault(key, data) ===");
        log.info("默认主题: {}, 键: {}, 消息: {}", kafkaTemplate.getDefaultTopic(), key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.sendDefault(key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("默认主题键值消息发送失败 - 主题: {}, 键: {}, 错误: {}", kafkaTemplate.getDefaultTopic(), key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("默认主题键值消息发送成功 - 主题: {}, 键: {}, 分区: {}, 偏移量: {}",
                        kafkaTemplate.getDefaultTopic(), key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法9: sendDefault(Integer partition, K key, V data)
     * 使用默认主题发送分区消息
     */
    public CompletableFuture<SendResult<String, String>> sendDefaultPartitionMessage(Integer partition, String key, String message) {
        log.info("=== 测试方法9: sendDefault(partition, key, data) ===");
        log.info("默认主题: {}, 分区: {}, 键: {}, 消息: {}", kafkaTemplate.getDefaultTopic(), partition, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.sendDefault(partition, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("默认主题分区消息发送失败 - 主题: {}, 分区: {}, 键: {}, 错误: {}",
                        kafkaTemplate.getDefaultTopic(), partition, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("默认主题分区消息发送成功 - 主题: {}, 分区: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        kafkaTemplate.getDefaultTopic(), partition, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 测试方法10: sendDefault(Integer partition, Long timestamp, K key, V data)
     * 使用默认主题发送时间戳消息
     */
    public CompletableFuture<SendResult<String, String>> sendDefaultTimestampMessage(Integer partition, Long timestamp, String key, String message) {
        log.info("=== 测试方法10: sendDefault(partition, timestamp, key, data) ===");
        log.info("默认主题: {}, 分区: {}, 时间戳: {}, 键: {}, 消息: {}", kafkaTemplate.getDefaultTopic(), partition, timestamp, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.sendDefault(partition, timestamp, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("默认主题时间戳消息发送失败 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 错误: {}",
                        kafkaTemplate.getDefaultTopic(), partition, timestamp, key, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("默认主题时间戳消息发送成功 - 主题: {}, 分区: {}, 时间戳: {}, 键: {}, 实际分区: {}, 偏移量: {}",
                        kafkaTemplate.getDefaultTopic(), partition, timestamp, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 批量发送测试消息
     */
    public void sendBatchTestMessages() {
        log.info("开始批量发送测试消息...");

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // 测试方法1: 简单消息
        for (int i = 1; i <= 3; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Simple Message " + i + " - " + timestamp;
            sendSimpleMessage(KafkaConstants.TOPIC_SIMPLE, message);
        }

        // 测试方法2: 键值消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "key-value-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Key-Value Message " + i + " - " + timestamp;
            sendKeyValueMessage(KafkaConstants.TOPIC_KEY_VALUE, key, message);
        }

        // 测试方法3: 分区消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "partition-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Partition Message " + i + " - " + timestamp;
            sendPartitionMessage(KafkaConstants.TOPIC_PARTITION, 0, key, message);
        }

        // 测试方法4: 时间戳消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "timestamp-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Timestamp Message " + i + " - " + timestamp;
            long timestampValue = System.currentTimeMillis() + i * 1000; // 递增时间戳
            sendTimestampMessage(KafkaConstants.TOPIC_TIMESTAMP, 0, timestampValue, key, message);
        }

        // 测试方法5: ProducerRecord消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "producer-record-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "ProducerRecord Message " + i + " - " + timestamp;
            sendProducerRecordMessage(KafkaConstants.TOPIC_PRODUCER_RECORD, key, message);
        }

        // 测试方法6: Spring Message消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "spring-message-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Spring Message " + i + " - " + timestamp;
            sendSpringMessage(KafkaConstants.TOPIC_MESSAGE, key, message);
        }

        // 测试方法7: 默认主题简单消息
        for (int i = 1; i <= 3; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Simple Message " + i + " - " + timestamp;
            sendDefaultSimpleMessage(message);
        }

        // 测试方法8: 默认主题键值消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-key-value-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Key-Value Message " + i + " - " + timestamp;
            sendDefaultKeyValueMessage(key, message);
        }

        // 测试方法9: 默认主题分区消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-partition-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Partition Message " + i + " - " + timestamp;
            sendDefaultPartitionMessage(0, key, message);
        }

        // 测试方法10: 默认主题时间戳消息
        for (int i = 1; i <= 3; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-timestamp-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Timestamp Message " + i + " - " + timestamp;
            long timestampValue = System.currentTimeMillis() + i * 1000; // 递增时间戳
            sendDefaultTimestampMessage(0, timestampValue, key, message);
        }

        log.info("批量发送测试消息完成");
    }

    // ==================== 对象消息发送演示方法 ====================

    // （移除重复的对象消息发送方法；保留下方“对象消息发送方法”中的实现）

    /**
     * 批量发送对象消息演示
     * 演示如何批量发送不同类型的对象消息
     */
    public void sendBatchObjectMessages() {
        log.info("开始批量发送对象消息演示...");

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        // 演示案例1: 发送用户消息对象
        for (int i = 1; i <= 3; i++) {
            UserMessage userMessage = new UserMessage(
                    (long) i,
                    "user" + i,
                    "用户消息内容 " + i + " - " + timestamp,
                    "USER_MESSAGE",
                    LocalDateTime.now(),
                    "ACTIVE"
            );
            sendUserMessageObject("user-message-topic", userMessage);
        }

        // 演示案例2: 发送订单消息对象
        for (int i = 1; i <= 3; i++) {
            OrderMessage orderMessage = new OrderMessage(
                    "ORDER-" + String.format("%06d", i),
                    (long) i,
                    "PENDING",
                    new BigDecimal("99.99").multiply(new BigDecimal(i)),
                    Arrays.asList(
                            new OrderMessage.OrderItem("PROD-001", "商品A", new BigDecimal("29.99"), 2, new BigDecimal("59.98")),
                            new OrderMessage.OrderItem("PROD-002", "商品B", new BigDecimal("39.99"), 1, new BigDecimal("39.99"))
                    ),
                    LocalDateTime.now(),
                    LocalDateTime.now(),
                    "订单备注 " + i
            );
            sendOrderMessageObject("order-message-topic", orderMessage);
        }

        // 演示案例3: 发送系统事件消息对象
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("source", "kafka-demo");
            attributes.put("version", "1.0.0");
            attributes.put("environment", "test");

            SystemEventMessage eventMessage = new SystemEventMessage(
                    "EVENT-" + String.format("%06d", i),
                    "SYSTEM_EVENT",
                    "INFO",
                    "系统事件描述 " + i + " - " + timestamp,
                    LocalDateTime.now(),
                    "kafka-demo-service",
                    "192.168.1.100",
                    (long) i,
                    attributes,
                    "SUCCESS"
            );
            sendSystemEventMessageObject("system-event-topic", eventMessage);
        }

        log.info("批量发送对象消息演示完成");
    }

    // ==================== 对象消息发送方法 ====================

    /**
     * 发送用户消息对象
     * 演示使用CommonUtils进行对象转换
     */
    public CompletableFuture<SendResult<String, String>> sendUserMessageObject(String topic, UserMessage userMessage) {
        log.info("=== 发送用户消息对象 ===");
        log.info("用户消息: {}", userMessage);

        // 使用CommonUtils.convert()将对象转换为JSON字符串
        String userMessageJson = CommonUtils.convert(userMessage, String.class);
        String key = "user-" + userMessage.getUserId();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, userMessageJson);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("用户消息发送失败 - 用户ID: {}, 错误: {}", userMessage.getUserId(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("用户消息发送成功 - 用户ID: {}, 分区: {}, 偏移量: {}",
                        userMessage.getUserId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 发送订单消息对象
     * 演示使用CommonUtils进行复杂对象转换
     */
    public CompletableFuture<SendResult<String, String>> sendOrderMessageObject(String topic, OrderMessage orderMessage) {
        log.info("=== 发送订单消息对象 ===");
        log.info("订单消息: {}", orderMessage);

        // 使用CommonUtils.convert()将复杂对象转换为JSON字符串
        String orderMessageJson = CommonUtils.convert(orderMessage, String.class);
        String key = "order-" + orderMessage.getOrderId();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, orderMessageJson);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("订单消息发送失败 - 订单ID: {}, 错误: {}", orderMessage.getOrderId(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("订单消息发送成功 - 订单ID: {}, 分区: {}, 偏移量: {}",
                        orderMessage.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 发送系统事件消息对象
     * 演示使用CommonUtils进行事件对象转换
     */
    public CompletableFuture<SendResult<String, String>> sendSystemEventMessageObject(String topic, SystemEventMessage eventMessage) {
        log.info("=== 发送系统事件消息对象 ===");
        log.info("系统事件消息: {}", eventMessage);

        // 使用CommonUtils.convert()将事件对象转换为JSON字符串
        String eventMessageJson = CommonUtils.convert(eventMessage, String.class);
        String key = "event-" + eventMessage.getEventId();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, eventMessageJson);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("系统事件消息发送失败 - 事件ID: {}, 错误: {}", eventMessage.getEventId(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("系统事件消息发送成功 - 事件ID: {}, 分区: {}, 偏移量: {}",
                        eventMessage.getEventId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 使用ProducerRecord发送用户消息对象
     * 演示ProducerRecord方式发送对象消息
     */
    public CompletableFuture<SendResult<String, String>> sendUserMessageWithProducerRecord(String topic, UserMessage userMessage) {
        log.info("=== 使用ProducerRecord发送用户消息对象 ===");
        log.info("用户消息: {}", userMessage);

        String userMessageJson = CommonUtils.convert(userMessage, String.class);
        String key = "producer-record-user-" + userMessage.getUserId();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, userMessageJson);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("ProducerRecord用户消息发送失败 - 用户ID: {}, 错误: {}",
                        userMessage.getUserId(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("ProducerRecord用户消息发送成功 - 用户ID: {}, 分区: {}, 偏移量: {}",
                        userMessage.getUserId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }

    /**
     * 使用Spring Message发送订单消息对象
     * 演示Spring Message方式发送对象消息
     */
    public CompletableFuture<SendResult<String, String>> sendOrderMessageWithSpringMessage(String topic, OrderMessage orderMessage) {
        log.info("=== 使用Spring Message发送订单消息对象 ===");
        log.info("订单消息: {}", orderMessage);

        String orderMessageJson = CommonUtils.convert(orderMessage, String.class);
        String key = "spring-message-order-" + orderMessage.getOrderId();

        Message<String> springMessage = MessageBuilder
                .withPayload(orderMessageJson)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(KafkaHeaders.CORRELATION_ID, "order-" + System.currentTimeMillis())
                .setHeader("message-type", "order-message")
                .setHeader("order-id", orderMessage.getOrderId())
                .setHeader("user-id", orderMessage.getUserId())
                .build();

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(springMessage);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Spring Message订单消息发送失败 - 订单ID: {}, 错误: {}",
                        orderMessage.getOrderId(), ex.getMessage(), ex);
            } else if (result != null) {
                log.info("Spring Message订单消息发送成功 - 订单ID: {}, 分区: {}, 偏移量: {}",
                        orderMessage.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
