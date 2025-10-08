package com.action.kafka08eventconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 手动提交消费者 - 演示手动提交偏移量的消息消费功能
 * 
 * 功能说明：
 * 1) 手动提交模式：消费消息后需要手动确认偏移量
 * 2) 精确控制：可以控制何时提交偏移量，避免消息丢失
 * 3) 异常处理：支持消费失败时的重试和回滚机制
 * 4) 批量确认：支持批量处理后的批量确认
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解标记消费方法
 * - 使用 manualCommitKafkaListenerContainerFactory 容器工厂
 * - 必须调用 Acknowledgment.acknowledge() 确认消息
 * - 异常时可以选择是否确认消息
 * 
 * 关键参数说明：
 * - Acknowledgment: 手动确认接口，必须调用 acknowledge() 方法
 * - ConsumerRecord: 包含完整的消息信息
 * - @Payload: 直接获取消息值
 * - @Header: 获取消息头信息
 */
@Component
public class ManualCommitConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);

    /**
     * 消费者组ID配置
     */
    @Value("${spring.kafka.consumer.group-id:demo-consumer-group}")
    private String groupId;

    /**
     * Topic名称配置
     */
    @Value("${demo.topic.name:demo-consumer-topic}")
    private String topicName;

    // 手动提交统计信息
    private long totalMessages = 0;
    private long successMessages = 0;
    private long failedMessages = 0;

    /**
     * 手动提交消息消费方法（使用 ConsumerRecord）
     * 
     * 执行时机：当有消息到达指定 topic 时
     * 作用：处理单条消息，手动确认偏移量
     * 
     * 参数说明：
     * - topics: 监听的 topic 名称
     * - groupId: 消费者组ID
     * - containerFactory: 使用手动提交的监听器容器工厂
     * 
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送测试消息示例
     * echo "Manual commit test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "manual-key:Manual commit with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送错误消息测试重试机制
     * echo "error message for retry test" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * @param record 完整的消息记录
     * @param ack 手动确认接口
     */
    @KafkaListener(
        topics = "${demo.topic.name:demo-consumer-topic}",
        groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual",
        containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeMessageWithManualCommit(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            totalMessages++;
            
            // 手动提交消费信息日志
            log.info("📦 Manual Commit Consumer (ConsumerRecord) 👥 Group: {} 📋 Topic: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} ⏰ Timestamp: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual", record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), totalMessages, successMessages, failedMessages);
            
            // 处理消息
            boolean success = processMessage(record.value());
            
            if (success) {
                // 处理成功：确认消息
                ack.acknowledge();
                successMessages++;
                log.info("✅ Message acknowledged successfully");
            } else {
                // 处理失败：不确认消息，会重新消费
                failedMessages++;
                log.warn("❌ Message processing failed, will be retried");
            }
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志，不确认消息
            failedMessages++;
            log.error("Error processing message from topic={}, partition={}, offset={}", 
                     record.topic(), record.partition(), record.offset(), ex);
            
            // 注意：异常时不调用 ack.acknowledge()，消息会被重新消费
        }
    }

    /**
     * 手动提交消息消费方法（使用 @Payload 和 @Header）
     * 
     * 执行时机：当有消息到达指定 topic 时
     * 作用：使用注解方式简化参数处理，手动确认偏移量
     * 
     * 参数说明：
     * - @Payload: 直接获取消息值
     * - @Header: 获取消息头信息
     * - Acknowledgment: 手动确认接口
     * 
     * 测试命令：
     * # 发送单条消息（无键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * # 发送单条消息（带键）
     * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送测试消息示例
     * echo "Manual payload test message" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "manual-payload-key:Manual payload with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * 
     * # 发送紧急消息测试
     * echo "urgent message for priority test" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * @param message 消息值
     * @param partition 分区号
     * @param offset 偏移量
     * @param key 消息键
     * @param ack 手动确认接口
     */
    @KafkaListener(
        topics = "${demo.topic.name:demo-consumer-topic}",
        groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual-payload",
        containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeMessageWithManualCommitPayload(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            Acknowledgment ack) {
        
        try {
            totalMessages++;
            
            // 手动提交消费信息日志
            log.info("📦 Manual Commit Consumer (@Payload/@Header) 👥 Group: {} 🔢 Partition: {} 📍 Offset: {} 🔑 Key: {} 💬 Value: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual-payload", partition, offset, key != null ? key : "null", message, totalMessages, successMessages, failedMessages);
            
            // 处理消息
            boolean success = processMessage(message);
            
            if (success) {
                // 处理成功：确认消息
                ack.acknowledge();
                successMessages++;
                log.info("✅ Message acknowledged successfully");
            } else {
                // 处理失败：不确认消息，会重新消费
                failedMessages++;
                log.warn("❌ Message processing failed, will be retried");
            }
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志，不确认消息
            failedMessages++;
            log.error("Error processing message from partition={}, offset={}", 
                     partition, offset, ex);
            
            // 注意：异常时不调用 ack.acknowledge()，消息会被重新消费
        }
    }

    /**
     * 手动提交批量消息消费方法
     * 
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：批量处理消息，批量确认偏移量
     * 
     * 参数说明：
     * - @Payload: 批量消息值列表
     * - @Header: 批量消息的公共头信息
     * - Acknowledgment: 手动确认接口
     * 
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..6}; do echo "manual batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     * 
     * # 发送批量消息（带键）
     * for i in {1..4}; do echo "manual-batch-key-$i:manual batch with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     * 
     * # 快速发送多条消息测试批量手动提交
     * for i in {1..15}; do echo "rapid manual batch $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.05; done
     * 
     * # 发送包含错误消息的批次测试
     * echo "normal message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "error message for batch retry" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "normal message 3" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * @param messages 批量消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     * @param ack 手动确认接口
     */
    @KafkaListener(
        topics = "${demo.topic.name:demo-consumer-topic}",
        groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-manual-batch",
        containerFactory = "manualCommitKafkaListenerContainerFactory"
    )
    public void consumeBatchMessagesWithManualCommit(
            @Payload List<String> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) List<String> keys,
            Acknowledgment ack) {
        
        try {
            totalMessages += messages.size();
            
            // 批量手动提交消费信息日志
            log.info("📦 Manual Commit Batch Consumer 👥 Group: {} 📊 Batch size: {} 🔢 Partitions: {} 📍 Offsets: {} 🔑 Keys: {} 📊 Total: {} Success: {} Failed: {}", 
                    groupId + "-manual-batch", messages.size(), partitions, offsets, keys != null ? keys : "null", totalMessages, successMessages, failedMessages);
            
            // 处理批量消息
            boolean allSuccess = processBatchMessages(messages, partitions, offsets, keys);
            
            if (allSuccess) {
                // 全部处理成功：确认整个批次
                ack.acknowledge();
                successMessages += messages.size();
                log.info("✅ Batch acknowledged successfully");
            } else {
                // 部分处理失败：不确认批次，会重新消费
                failedMessages += messages.size();
                log.warn("❌ Batch processing failed, will be retried");
            }
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志，不确认批次
            failedMessages += messages.size();
            log.error("Error processing batch messages, batch size: {}", messages.size(), ex);
            
            // 注意：异常时不调用 ack.acknowledge()，整个批次会被重新消费
        }
    }

    /**
     * 处理批量消息
     * 
     * 作用：处理批量消息，返回是否全部成功
     * 实现：遍历消息列表，进行批量处理
     * 
     * @param messages 消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     * @return 是否全部处理成功
     */
    private boolean processBatchMessages(List<String> messages, 
                                       List<Integer> partitions, 
                                       List<Long> offsets, 
                                       List<String> keys) {
        try {
            boolean allSuccess = true;
            
            // 处理每条消息
            for (int i = 0; i < messages.size(); i++) {
                String message = messages.get(i);
                Integer partition = partitions.get(i);
                Long offset = offsets.get(i);
                String key = keys != null ? keys.get(i) : null;
                
                log.debug("Processing message from partition {}: key={}, value={}, offset={}", 
                         partition, key != null ? key : "null", message, offset);
                
                boolean success = processMessage(message);
                if (!success) {
                    allSuccess = false;
                    log.warn("Message processing failed: partition={}, offset={}", partition, offset);
                }
            }
            
            // 模拟批量处理时间
            Thread.sleep(300);
            
            return allSuccess;
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
            return false;
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
            return false;
        }
    }

    /**
     * 消息处理逻辑
     * 
     * 作用：模拟实际的消息处理业务逻辑
     * 实现：可以根据消息内容进行不同的处理，返回处理结果
     * 
     * @param message 消息内容
     * @return 处理是否成功
     */
    private boolean processMessage(String message) {
        try {
            // 模拟业务处理时间
            Thread.sleep(100);
            
            // 根据消息内容进行不同处理
            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
                // 模拟错误消息处理失败
                return Math.random() > 0.3; // 70% 成功率
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
                // 紧急消息总是成功
                return true;
            } else {
                log.debug("Processing normal message: {}", message);
                // 普通消息 95% 成功率
                return Math.random() > 0.05;
            }
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
            return false;
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
            return false;
        }
    }

    /**
     * 获取手动提交统计信息
     * 
     * @return 统计信息字符串
     */
    public String getStatistics() {
        return String.format("Manual Commit Consumer Statistics - Total: %d, Success: %d, Failed: %d, Success Rate: %.2f%%", 
                           totalMessages, successMessages, failedMessages, 
                           totalMessages > 0 ? (double) successMessages / totalMessages * 100 : 0);
    }
}
