package com.action.kafka05kafkatemplatesend.controller;


import com.action.kafka05kafkatemplatesend.constants.KafkaConstants;
import com.action.kafka05kafkatemplatesend.model.OrderMessage;
import com.action.kafka05kafkatemplatesend.model.SystemEventMessage;
import com.action.kafka05kafkatemplatesend.model.UserMessage;
import com.action.kafka05kafkatemplatesend.service.KafkaTemplateSendService;
import com.action.kafka0common.common.R;
import com.action.kafka0common.exception.BusinessException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * KafkaTemplate 发送方法测试控制器
 * 提供各种KafkaTemplate send方法的测试接口
 *
 * @author action
 * @since 2024
 */
@Slf4j
@RestController
@RequestMapping("/api/kafka/template")
@RequiredArgsConstructor
public class KafkaTemplateController {

    private final KafkaTemplateSendService kafkaTemplateSendService;

    /**
     * 测试方法1: send(String topic, V data) - 简单消息
     */
    @GetMapping("/send/simple")
    public R<Map<String, Object>> sendSimpleMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条简单消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Simple Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendSimpleMessage(KafkaConstants.TOPIC_SIMPLE, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条简单消息");
        data.put("topic", KafkaConstants.TOPIC_SIMPLE);
        data.put("count", count);
        data.put("method", "send(topic, data)");
        data.put("description", "只发送消息值，不指定键");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法2: send(String topic, K key, V data) - 键值消息
     */
    @GetMapping("/send/key-value")
    public R<Map<String, Object>> sendKeyValueMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条键值消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "key-value-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Key-Value Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendKeyValueMessage(KafkaConstants.TOPIC_KEY_VALUE, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条键值消息");
        data.put("topic", KafkaConstants.TOPIC_KEY_VALUE);
        data.put("count", count);
        data.put("method", "send(topic, key, data)");
        data.put("description", "发送消息键和消息值");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法3: send(String topic, Integer partition, K key, V data) - 分区消息
     */
    @GetMapping("/send/partition")
    public R<Map<String, Object>> sendPartitionMessage(
            @RequestParam(defaultValue = "3") int count,
            @RequestParam(defaultValue = "0") int partition) {

        log.info("收到请求：发送 {} 条分区消息到分区 {}", count, partition);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        if (partition < 0) {
            throw new BusinessException("9997", "分区号不能为负数");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "partition-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Partition Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendPartitionMessage(KafkaConstants.TOPIC_PARTITION, partition, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条分区消息到分区 " + partition);
        data.put("topic", KafkaConstants.TOPIC_PARTITION);
        data.put("count", count);
        data.put("partition", partition);
        data.put("method", "send(topic, partition, key, data)");
        data.put("description", "指定分区发送消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法4: send(String topic, Integer partition, Long timestamp, K key, V data) - 时间戳消息
     */
    @GetMapping("/send/timestamp")
    public R<Map<String, Object>> sendTimestampMessage(
            @RequestParam(defaultValue = "3") int count,
            @RequestParam(defaultValue = "0") int partition) {

        log.info("收到请求：发送 {} 条时间戳消息到分区 {}", count, partition);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        if (partition < 0) {
            throw new BusinessException("9997", "分区号不能为负数");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "timestamp-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Timestamp Message " + i + " - " + timestamp;
            long timestampValue = System.currentTimeMillis() + i * 1000; // 递增时间戳
            kafkaTemplateSendService.sendTimestampMessage(KafkaConstants.TOPIC_TIMESTAMP, partition, timestampValue, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条时间戳消息到分区 " + partition);
        data.put("topic", KafkaConstants.TOPIC_TIMESTAMP);
        data.put("count", count);
        data.put("partition", partition);
        data.put("method", "send(topic, partition, timestamp, key, data)");
        data.put("description", "指定分区和时间戳发送消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法5: send(ProducerRecord<K, V> record) - ProducerRecord消息
     */
    @GetMapping("/send/producer-record")
    public R<Map<String, Object>> sendProducerRecordMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条ProducerRecord消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "producer-record-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "ProducerRecord Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendProducerRecordMessage(KafkaConstants.TOPIC_PRODUCER_RECORD, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条ProducerRecord消息");
        data.put("topic", KafkaConstants.TOPIC_PRODUCER_RECORD);
        data.put("count", count);
        data.put("method", "send(ProducerRecord)");
        data.put("description", "使用ProducerRecord发送消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法6: send(Message<?> message) - Spring Message消息
     */
    @GetMapping("/send/spring-message")
    public R<Map<String, Object>> sendSpringMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条Spring Message消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "spring-message-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Spring Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendSpringMessage(KafkaConstants.TOPIC_MESSAGE, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条Spring Message消息");
        data.put("topic", KafkaConstants.TOPIC_MESSAGE);
        data.put("count", count);
        data.put("method", "send(Message)");
        data.put("description", "使用Spring Message发送消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法7: sendDefault(V data) - 默认主题简单消息
     */
    @GetMapping("/send/default-simple")
    public R<Map<String, Object>> sendDefaultSimpleMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条默认主题简单消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Simple Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendDefaultSimpleMessage(message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条默认主题简单消息");
        data.put("topic", kafkaTemplateSendService.getKafkaTemplate().getDefaultTopic());
        data.put("count", count);
        data.put("method", "sendDefault(data)");
        data.put("description", "使用默认主题发送简单消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法8: sendDefault(K key, V data) - 默认主题键值消息
     */
    @GetMapping("/send/default-key-value")
    public R<Map<String, Object>> sendDefaultKeyValueMessage(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条默认主题键值消息", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-key-value-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Key-Value Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendDefaultKeyValueMessage(key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条默认主题键值消息");
        data.put("topic", kafkaTemplateSendService.getKafkaTemplate().getDefaultTopic());
        data.put("count", count);
        data.put("method", "sendDefault(key, data)");
        data.put("description", "使用默认主题发送键值消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法9: sendDefault(Integer partition, K key, V data) - 默认主题分区消息
     */
    @GetMapping("/send/default-partition")
    public R<Map<String, Object>> sendDefaultPartitionMessage(
            @RequestParam(defaultValue = "3") int count,
            @RequestParam(defaultValue = "0") int partition) {

        log.info("收到请求：发送 {} 条默认主题分区消息到分区 {}", count, partition);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        if (partition < 0) {
            throw new BusinessException("9997", "分区号不能为负数");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-partition-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Partition Message " + i + " - " + timestamp;
            kafkaTemplateSendService.sendDefaultPartitionMessage(partition, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条默认主题分区消息到分区 " + partition);
        data.put("topic", kafkaTemplateSendService.getKafkaTemplate().getDefaultTopic());
        data.put("count", count);
        data.put("partition", partition);
        data.put("method", "sendDefault(partition, key, data)");
        data.put("description", "使用默认主题发送分区消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 测试方法10: sendDefault(Integer partition, Long timestamp, K key, V data) - 默认主题时间戳消息
     */
    @GetMapping("/send/default-timestamp")
    public R<Map<String, Object>> sendDefaultTimestampMessage(
            @RequestParam(defaultValue = "3") int count,
            @RequestParam(defaultValue = "0") int partition) {

        log.info("收到请求：发送 {} 条默认主题时间戳消息到分区 {}", count, partition);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        if (partition < 0) {
            throw new BusinessException("9997", "分区号不能为负数");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            String key = KafkaConstants.MESSAGE_KEY_PREFIX + "default-timestamp-" + i;
            String message = KafkaConstants.MESSAGE_VALUE_PREFIX + "Default Timestamp Message " + i + " - " + timestamp;
            long timestampValue = System.currentTimeMillis() + i * 1000; // 递增时间戳
            kafkaTemplateSendService.sendDefaultTimestampMessage(partition, timestampValue, key, message);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条默认主题时间戳消息到分区 " + partition);
        data.put("topic", kafkaTemplateSendService.getKafkaTemplate().getDefaultTopic());
        data.put("count", count);
        data.put("partition", partition);
        data.put("method", "sendDefault(partition, timestamp, key, data)");
        data.put("description", "使用默认主题发送时间戳消息");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 批量发送所有类型的测试消息
     */
    @GetMapping("/send/batch")
    public R<Map<String, Object>> sendBatchMessages() {
        log.info("收到请求：批量发送所有类型的测试消息");

        kafkaTemplateSendService.sendBatchTestMessages();

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功批量发送所有类型的测试消息");
        data.put("topics", new String[]{
                KafkaConstants.TOPIC_SIMPLE,
                KafkaConstants.TOPIC_KEY_VALUE,
                KafkaConstants.TOPIC_PARTITION,
                KafkaConstants.TOPIC_TIMESTAMP,
                KafkaConstants.TOPIC_PRODUCER_RECORD,
                KafkaConstants.TOPIC_MESSAGE,
                KafkaConstants.TOPIC_DEFAULT
        });
        data.put("countPerTopic", 3);
        data.put("totalCount", 21);
        data.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        return R.success(data);
    }

    // ==================== 对象消息发送接口 ====================

    /**
     * 对象消息发送案例1: 发送用户消息对象
     */
    @PostMapping("/send/object/user-message")
    public R<Map<String, Object>> sendUserMessageObject(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条用户消息对象", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
            UserMessage userMessage = new UserMessage(
                    (long) i,
                    "user" + i,
                    "用户消息内容 " + i + " - " + timestamp,
                    "USER_MESSAGE",
                    LocalDateTime.now(),
                    "ACTIVE"
            );
            kafkaTemplateSendService.sendUserMessageObject("user-message-topic", userMessage);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条用户消息对象");
        data.put("topic", "user-message-topic");
        data.put("count", count);
        data.put("objectType", "UserMessage");
        data.put("description", "使用CommonUtils进行对象转换，发送用户消息对象");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 对象消息发送案例2: 发送订单消息对象
     */
    @PostMapping("/send/object/order-message")
    public R<Map<String, Object>> sendOrderMessageObject(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条订单消息对象", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
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
            kafkaTemplateSendService.sendOrderMessageObject("order-message-topic", orderMessage);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条订单消息对象");
        data.put("topic", "order-message-topic");
        data.put("count", count);
        data.put("objectType", "OrderMessage");
        data.put("description", "使用CommonUtils进行对象转换，发送复杂订单消息对象");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 对象消息发送案例3: 发送系统事件消息对象
     */
    @PostMapping("/send/object/system-event-message")
    public R<Map<String, Object>> sendSystemEventMessageObject(
            @RequestParam(defaultValue = "3") int count) {

        log.info("收到请求：发送 {} 条系统事件消息对象", count);

        if (count <= 0 || count > 100) {
            throw new BusinessException("9998", "消息数量必须在1-100之间");
        }

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        for (int i = 1; i <= count; i++) {
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
            kafkaTemplateSendService.sendSystemEventMessageObject("system-event-topic", eventMessage);
        }

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功发送 " + count + " 条系统事件消息对象");
        data.put("topic", "system-event-topic");
        data.put("count", count);
        data.put("objectType", "SystemEventMessage");
        data.put("description", "使用CommonUtils进行对象转换，发送系统事件消息对象");
        data.put("timestamp", timestamp);

        return R.success(data);
    }

    /**
     * 批量发送对象消息演示
     */
    @PostMapping("/send/object/batch")
    public R<Map<String, Object>> sendBatchObjectMessages() {
        log.info("收到请求：批量发送对象消息演示");

        kafkaTemplateSendService.sendBatchObjectMessages();

        Map<String, Object> data = new HashMap<>();
        data.put("message", "成功批量发送对象消息演示");
        data.put("topics", new String[]{
                "user-message-topic",
                "order-message-topic",
                "system-event-topic"
        });
        data.put("countPerTopic", 3);
        data.put("totalCount", 9);
        data.put("objectTypes", new String[]{
                "UserMessage",
                "OrderMessage",
                "SystemEventMessage"
        });
        data.put("description", "演示使用CommonUtils进行对象转换，发送不同类型的对象消息");
        data.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        return R.success(data);
    }
}
