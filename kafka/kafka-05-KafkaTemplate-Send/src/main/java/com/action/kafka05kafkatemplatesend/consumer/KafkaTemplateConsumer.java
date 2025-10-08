package com.action.kafka05kafkatemplatesend.consumer;

import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
import com.action.kafka05kafkatemplatesend.model.OrderMessage;
import com.action.kafka05kafkatemplatesend.model.SystemEventMessage;
import com.action.kafka05kafkatemplatesend.model.UserMessage;
import com.action.kafka0common.common.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * KafkaTemplate 发送方法测试消费者
 * 监听所有测试主题的消息
 *
 * @author action
 * @since 2024
 */
@Slf4j
@Component
public class KafkaTemplateConsumer {

    /**
     * 监听简单消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_SIMPLE,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSimpleMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 简单消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("========================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听键值消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_KEY_VALUE,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleKeyValueMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 键值消息消费者被触发 ===");
        log.info("=== 键值消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("======================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听分区消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_PARTITION,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePartitionMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 分区消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("======================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听时间戳消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_TIMESTAMP,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleTimestampMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 时间戳消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("========================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听ProducerRecord消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_PRODUCER_RECORD,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleProducerRecordMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== ProducerRecord消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("==============================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听Spring Message消息主题
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_MESSAGE,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSpringMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== Spring Message消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("==============================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听默认主题消息
     */
    @KafkaListener(
            topics = KafkaConstants.TOPIC_DEFAULT,
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleDefaultMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 默认主题消息消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);
        log.info("========================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    // ==================== 对象消息消费者 ====================

    /**
     * 监听用户消息对象主题
     */
    @KafkaListener(
            topics = "user-message-topic",
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleUserMessageObject(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 用户消息对象消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("原始消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);

        try {
            // 使用CommonUtils将JSON字符串转换为UserMessage对象
            UserMessage userMessage = CommonUtils.convertString(message, UserMessage.class);
            log.info("解析后的用户消息对象: {}", userMessage);
            log.info("用户ID: {}", userMessage.getUserId());
            log.info("用户名: {}", userMessage.getUsername());
            log.info("消息内容: {}", userMessage.getContent());
            log.info("消息类型: {}", userMessage.getMessageType());
            log.info("状态: {}", userMessage.getStatus());
            log.info("创建时间: {}", userMessage.getCreateTime());
            // 移除扩展信息字段日志
        } catch (Exception e) {
            log.error("解析用户消息对象失败: {}", e.getMessage(), e);
        }

        log.info("=====================================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听订单消息对象主题
     */
    @KafkaListener(
            topics = "order-message-topic",
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderMessageObject(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 订单消息对象消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("原始消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);

        try {
            // 使用CommonUtils将JSON字符串转换为OrderMessage对象
            OrderMessage orderMessage = CommonUtils.convertString(message, OrderMessage.class);
            log.info("解析后的订单消息对象: {}", orderMessage);
            log.info("订单ID: {}", orderMessage.getOrderId());
            log.info("用户ID: {}", orderMessage.getUserId());
            log.info("订单状态: {}", orderMessage.getStatus());
            log.info("订单总金额: {}", orderMessage.getTotalAmount());
            log.info("商品项数量: {}", orderMessage.getItems() != null ? orderMessage.getItems().size() : 0);

            if (orderMessage.getItems() != null && !orderMessage.getItems().isEmpty()) {
                log.info("商品项详情:");
                for (int i = 0; i < orderMessage.getItems().size(); i++) {
                    OrderMessage.OrderItem item = orderMessage.getItems().get(i);
                    log.info("  商品{}: ID={}, 名称={}, 单价={}, 数量={}, 小计={}",
                            i + 1, item.getProductId(), item.getProductName(),
                            item.getPrice(), item.getQuantity(), item.getSubtotal());
                }
            }

            log.info("创建时间: {}", orderMessage.getCreateTime());
            log.info("更新时间: {}", orderMessage.getUpdateTime());
            log.info("备注: {}", orderMessage.getRemark());
        } catch (Exception e) {
            log.error("解析订单消息对象失败: {}", e.getMessage(), e);
        }

        log.info("=====================================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 监听系统事件消息对象主题
     */
    @KafkaListener(
            topics = "system-event-topic",
            groupId = KafkaConstants.CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSystemEventMessageObject(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) @Nullable String key,
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        log.info("=== 系统事件消息对象消费者 ===");
        log.info("主题: {}", topic);
        log.info("分区: {}", partition);
        log.info("偏移量: {}", offset);
        log.info("消息键: {}", StringUtils.hasText(key) ? key : "无");
        log.info("原始消息内容: {}", message);
        log.info("时间戳: {}", record.timestamp());
        log.info("消费者组: {}", KafkaConstants.CONSUMER_GROUP);

        try {
            // 使用CommonUtils将JSON字符串转换为SystemEventMessage对象
            SystemEventMessage eventMessage = CommonUtils.convertString(message, SystemEventMessage.class);
            log.info("解析后的系统事件消息对象: {}", eventMessage);
            log.info("事件ID: {}", eventMessage.getEventId());
            log.info("事件类型: {}", eventMessage.getEventType());
            log.info("事件级别: {}", eventMessage.getLevel());
            log.info("事件描述: {}", eventMessage.getDescription());
            log.info("事件时间: {}", eventMessage.getEventTime());
            log.info("服务名称: {}", eventMessage.getServiceName());
            log.info("主机IP: {}", eventMessage.getHostIp());
            log.info("用户ID: {}", eventMessage.getUserId());
            log.info("状态: {}", eventMessage.getStatus());

            if (eventMessage.getAttributes() != null && !eventMessage.getAttributes().isEmpty()) {
                log.info("扩展属性:");
                eventMessage.getAttributes().forEach((k, v) -> log.info("  {}: {}", k, v));
            }
        } catch (Exception e) {
            log.error("解析系统事件消息对象失败: {}", e.getMessage(), e);
        }

        log.info("=====================================");

        // 手动确认消息
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
