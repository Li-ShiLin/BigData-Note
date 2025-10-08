package com.action.kafka13consumersendto.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * 消息转发消费者
 * 
 * 功能说明：
 * 1) 演示 Kafka 消息转发功能
 * 2) 使用 @SendTo 注解实现消息自动转发
 * 3) 支持消息处理和转发逻辑
 * 4) 提供多种消息转发方式
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听源主题
 * - 使用 @SendTo 注解指定转发目标主题
 * - 使用 Acknowledgment 进行手动消息确认
 * - 支持消息内容修改和增强
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - @SendTo: 指定消息转发目标主题
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class MessageForwardConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageForwardConsumer.class);

    /**
     * 基础消息转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 sourceGroup 消费者组
     * 3) 接收消息并添加转发标识
     * 4) 自动转发到 target-topic 主题
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 转发消息内容
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "sourceGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String forwardMessage(ConsumerRecord<String, String> record,
                                @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                @Header(value = KafkaHeaders.OFFSET) String offset,
                                Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[forward] === 消息转发演示 ===");
            log.info("[forward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            // 处理消息并添加转发标识
            String originalMessage = record.value();
            String forwardedMessage = "[FORWARDED] " + originalMessage + " -> processed at " + System.currentTimeMillis();
            
            // 打印转发消息
            log.info("[forward] 转发消息: {}", forwardedMessage);
            log.info("[forward] 消息转发完成: {} -> {}", topic, "target-topic");
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("[forward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
            // 返回转发消息内容
            return forwardedMessage;
            
        } catch (Exception e) {
            log.error("[forward] 消息转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 消息增强转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 enhanceGroup 消费者组
     * 3) 接收消息并进行业务处理
     * 4) 增强消息内容并转发
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 增强后的转发消息内容
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "enhanceGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String enhanceAndForwardMessage(ConsumerRecord<String, String> record,
                                         @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                         @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                         @Header(value = KafkaHeaders.OFFSET) String offset,
                                         Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[enhanceAndForward] === 消息增强转发演示 ===");
            log.info("[enhanceAndForward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            // 模拟业务处理逻辑
            String originalMessage = record.value();
            String processedMessage = processMessage(originalMessage);
            
            // 构建增强后的消息
            String enhancedMessage = String.format(
                "[ENHANCED] Original: %s | Processed: %s | Timestamp: %d | Source: %s", 
                originalMessage, processedMessage, System.currentTimeMillis(), topic
            );
            
            // 打印增强后的消息
            log.info("[enhanceAndForward] 增强消息: {}", enhancedMessage);
            log.info("[enhanceAndForward] 消息增强转发完成: {} -> {}", topic, "target-topic");
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("[enhanceAndForward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
            
            // 返回增强后的转发消息内容
            return enhancedMessage;
            
        } catch (Exception e) {
            log.error("[enhanceAndForward] 消息增强转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 条件消息转发演示
     * 
     * 执行流程：
     * 1) 监听 source-topic 主题
     * 2) 使用 conditionGroup 消费者组
     * 3) 根据消息内容决定是否转发
     * 4) 满足条件时转发，不满足时返回null
     * 5) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     * 
     * @return 满足条件时返回转发消息，否则返回null
     */
    @KafkaListener(topics = "${kafka.topic.source:source-topic}", groupId = "conditionGroup")
    @SendTo("${kafka.topic.target:target-topic}")
    public String conditionalForwardMessage(ConsumerRecord<String, String> record,
                                           @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                           @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                           @Header(value = KafkaHeaders.OFFSET) String offset,
                                           Acknowledgment ack) {
        try {
            // 打印接收到的原始消息
            log.info("[conditionalForward] === 条件消息转发演示 ===");
            log.info("[conditionalForward] 原始消息: topic={}, partition={}, offset={}, key={}, value={}", 
                    topic, partition, offset, record.key(), record.value());
            
            String originalMessage = record.value();
            
            // 条件判断：只转发包含特定关键词的消息
            if (shouldForwardMessage(originalMessage)) {
                String forwardedMessage = "[CONDITIONAL-FORWARD] " + originalMessage + " | Condition: PASSED";
                
                // 打印转发消息
                log.info("[conditionalForward] 条件转发消息: {}", forwardedMessage);
                log.info("[conditionalForward] 消息条件转发完成: {} -> {}", topic, "target-topic");
                
                // 手动确认消息处理完成
                ack.acknowledge();
                log.info("[conditionalForward] 消息处理完成并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
                
                // 返回转发消息内容
                return forwardedMessage;
            } else {
                // 不满足转发条件
                log.info("[conditionalForward] 消息不满足转发条件，跳过转发: {}", originalMessage);
                
                // 手动确认消息处理完成（即使不转发也要确认）
                ack.acknowledge();
                log.info("[conditionalForward] 消息处理完成（未转发）并已确认: topic={}, partition={}, offset={}", topic, partition, offset);
                
                // 返回null表示不转发
                return null;
            }
            
        } catch (Exception e) {
            log.error("[conditionalForward] 条件消息转发处理时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            throw e;
        }
    }

    /**
     * 模拟消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟实际业务处理逻辑
     * 2) 对消息内容进行转换和增强
     * 3) 用于演示消息处理流程
     * 
     * @param originalMessage 原始消息内容
     * @return 处理后的消息内容
     */
    private String processMessage(String originalMessage) {
        // 模拟处理时间
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 模拟业务处理：转换为大写并添加处理标识
        return originalMessage.toUpperCase() + " [PROCESSED]";
    }

    /**
     * 判断是否应该转发消息
     * 
     * 功能说明：
     * 1) 根据消息内容判断是否满足转发条件
     * 2) 演示条件转发的业务逻辑
     * 3) 用于演示选择性转发
     * 
     * @param message 消息内容
     * @return 是否应该转发
     */
    private boolean shouldForwardMessage(String message) {
        // 转发条件：消息长度大于5且包含特定关键词
        return message != null && 
               message.length() > 5 && 
               (message.contains("forward") || message.contains("test") || message.contains("demo"));
    }
}
