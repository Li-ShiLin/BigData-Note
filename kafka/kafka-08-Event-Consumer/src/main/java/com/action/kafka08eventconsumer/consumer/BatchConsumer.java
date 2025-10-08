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
 * 批量消费者 - 演示批量消息消费功能
 * 
 * 功能说明：
 * 1) 批量消费：一次处理多条消息，提高消费效率
 * 2) 自动提交模式：消费完成后自动提交偏移量
 * 3) 批量处理：适合高吞吐量场景
 * 4) 统计信息：记录批量消费的统计信息
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解标记批量消费方法
 * - 支持 List<ConsumerRecord> 和 List<String> 两种参数方式
 * - 批量消费可以减少网络开销，提高处理效率
 * - 异常会中断整个批次的消费
 * 
 * 关键参数说明：
 * - List<ConsumerRecord>: 批量消息记录列表
 * - List<String>: 批量消息值列表
 * - @Header: 批量消息的公共头信息
 * - Acknowledgment: 手动确认接口（自动提交模式下为null）
 */
@Component
public class BatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);

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

    // 批量消费统计信息
    private long totalBatches = 0;
    private long totalMessages = 0;

    /**
     * 批量消息消费方法（使用 List<ConsumerRecord>）
     * 
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：批量处理消息，提高消费效率
     * 
     * 参数说明：
     * - topics: 监听的 topic 名称
     * - groupId: 消费者组ID
     * - containerFactory: 使用批量消费的监听器容器工厂
     * 
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..5}; do echo "batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     * 
     * # 发送批量消息（带键）
     * for i in {1..3}; do echo "batch-key-$i:batch message with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     * 
     * # 快速发送多条消息测试批量消费
     * for i in {1..10}; do echo "rapid message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; sleep 0.1; done
     * 
     * @param records 批量消息记录列表
     */
    @KafkaListener(
        topics = "${demo.topic.name:demo-consumer-topic}",
        groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-batch",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records) {
        try {
            // 更新统计信息
            totalBatches++;
            totalMessages += records.size();
            
            // 批量消费信息日志
            log.info("📦 Batch Consumer (List<ConsumerRecord>) 👥 Group: {} 📊 Batch size: {} 📈 Total batches: {} 📈 Total messages: {}", 
                    groupId + "-batch", records.size(), totalBatches, totalMessages);
            
            // 处理批量消息
            processBatchMessages(records);
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志
            log.error("Error processing batch messages, batch size: {}", records.size(), ex);
        }
    }

    /**
     * 批量消息消费方法（使用 List<String>）
     * 
     * 执行时机：当有消息批次到达指定 topic 时
     * 作用：使用简化的参数方式处理批量消息
     * 
     * 参数说明：
     * - @Payload: 批量消息值列表
     * - @Header: 批量消息的公共头信息
     * - Acknowledgment: 手动确认接口
     * 
     * 测试命令：
     * # 发送批量消息（无键）
     * for i in {1..8}; do echo "payload batch message $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic; done
     * 
     * # 发送批量消息（带键）
     * for i in {1..4}; do echo "payload-key-$i:payload batch with key $i" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"; done
     * 
     * # 混合发送测试（部分有键，部分无键）
     * echo "mixed message 1" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * echo "mixed-key:mixed message with key" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic --property "parse.key=true" --property "key.separator=:"
     * echo "mixed message 3" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-consumer-topic
     * 
     * @param messages 批量消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     * @param ack 手动确认接口
     */
    @KafkaListener(
        topics = "${demo.topic.name:demo-consumer-topic}",
        groupId = "${spring.kafka.consumer.group-id:demo-consumer-group}-batch-payload",
        containerFactory = "manualBatchKafkaListenerContainerFactory"
    )
    public void consumeBatchMessagesWithPayload(
            @Payload List<String> messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) List<String> keys,
            Acknowledgment ack) {
        
        try {
            // 更新统计信息
            totalBatches++;
            totalMessages += messages.size();
            
            // 批量消费信息日志
            log.info("📦 Batch Consumer (@Payload/@Header) 👥 Group: {} 📊 Batch size: {} 🔢 Partitions: {} 📍 Offsets: {} 🔑 Keys: {} 📈 Total batches: {} 📈 Total messages: {}", 
                    groupId + "-batch-payload", messages.size(), partitions, offsets, keys != null ? keys : "null", totalBatches, totalMessages);
            
            // 处理批量消息
            processBatchMessagesWithDetails(messages, partitions, offsets, keys);
            
            // 手动确认偏移量
            if (ack != null) {
                ack.acknowledge();
                log.info("✅ Batch acknowledged successfully");
            }
            
        } catch (Exception ex) {
            // 异常处理：记录错误日志
            log.error("Error processing batch messages, batch size: {}", messages.size(), ex);
        }
    }

    /**
     * 处理批量消息（使用 ConsumerRecord）
     * 
     * 作用：处理批量消息记录，提取详细信息
     * 实现：遍历消息列表，进行批量处理
     * 
     * @param records 批量消息记录列表
     */
    private void processBatchMessages(List<ConsumerRecord<String, String>> records) {
        try {
            // 按分区分组处理
            records.stream()
                .collect(java.util.stream.Collectors.groupingBy(ConsumerRecord::partition))
                .forEach((partition, partitionRecords) -> {
                    log.info("Processing {} messages from partition {}", 
                            partitionRecords.size(), partition);
                    
                    // 处理分区内的消息
                    partitionRecords.forEach(record -> {
                        log.debug("Message from partition {}: key={}, value={}, offset={}", 
                                record.partition(), record.key(), record.value(), record.offset());
                        processMessage(record.value());
                    });
                });
            
            // 模拟批量处理时间
            Thread.sleep(200);
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
        }
    }

    /**
     * 处理批量消息（使用详细信息）
     * 
     * 作用：处理批量消息的详细信息
     * 实现：遍历消息列表，进行批量处理
     * 
     * @param messages 消息值列表
     * @param partitions 分区号列表
     * @param offsets 偏移量列表
     * @param keys 消息键列表
     */
    private void processBatchMessagesWithDetails(List<String> messages, 
                                               List<Integer> partitions, 
                                               List<Long> offsets, 
                                               List<String> keys) {
        try {
            // 按分区分组处理
            for (int i = 0; i < messages.size(); i++) {
                String message = messages.get(i);
                Integer partition = partitions.get(i);
                Long offset = offsets.get(i);
                String key = keys != null ? keys.get(i) : null;
                
                log.debug("Message from partition {}: key={}, value={}, offset={}", 
                         partition, key != null ? key : "null", message, offset);
                
                processMessage(message);
            }
            
            // 模拟批量处理时间
            Thread.sleep(200);
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Batch processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in batch processing", ex);
        }
    }

    /**
     * 消息处理逻辑
     * 
     * 作用：模拟实际的消息处理业务逻辑
     * 实现：可以根据消息内容进行不同的处理
     * 
     * @param message 消息内容
     */
    private void processMessage(String message) {
        try {
            // 模拟业务处理时间
            Thread.sleep(50);
            
            // 根据消息内容进行不同处理
            if (message.contains("error")) {
                log.warn("Processing error message: {}", message);
            } else if (message.contains("urgent")) {
                log.warn("Processing urgent message: {}", message);
            } else {
                log.debug("Processing normal message: {}", message);
            }
            
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Message processing interrupted", ex);
        } catch (Exception ex) {
            log.error("Error in message processing", ex);
        }
    }

    /**
     * 获取批量消费统计信息
     * 
     * @return 统计信息字符串
     */
    public String getStatistics() {
        return String.format("Batch Consumer Statistics - Total Batches: %d, Total Messages: %d", 
                           totalBatches, totalMessages);
    }
}
