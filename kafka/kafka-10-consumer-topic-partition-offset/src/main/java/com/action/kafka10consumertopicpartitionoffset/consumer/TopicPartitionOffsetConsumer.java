package com.action.kafka10consumertopicpartitionoffset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 指定Topic、Partition、Offset消费演示消费者
 * 
 * 功能说明：
 * 1) 演示如何指定具体的Topic、Partition、Offset进行消费
 * 2) 监听指定分区的所有消息
 * 3) 监听指定分区的指定偏移量开始的消息
 * 4) 支持手动确认消息处理
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解配置监听
 * - 使用 topicPartitions 属性指定详细的监听配置
 * - 使用 @TopicPartition 指定主题和分区
 * - 使用 @PartitionOffset 指定分区的起始偏移量
 * - 使用 Acknowledgment 进行手动消息确认
 * 
 * 关键参数说明：
 * - topicPartitions: 可配置更加详细的监听信息，可指定topic、partition、offset监听
 * - @TopicPartition: 指定要监听的主题和分区
 * - @PartitionOffset: 指定分区的起始偏移量
 * - 注意：topics和topicPartitions不能同时使用
 */
@Component
public class TopicPartitionOffsetConsumer {

    private static final Logger log = LoggerFactory.getLogger(TopicPartitionOffsetConsumer.class);

    /**
     * 指定Topic、Partition、Offset消费演示
     * 
     * 监听配置说明：
     * 1) 监听 topic-partition-offset-demo 主题的 0、1、2 号分区（从最新偏移量开始）
     * 2) 监听 topic-partition-offset-demo 主题的 3 号分区（从偏移量 3 开始）
     * 3) 监听 topic-partition-offset-demo 主题的 4 号分区（从偏移量 3 开始）
     * 
     * 执行流程：
     * 1) 消费者启动后开始监听指定的分区
     * 2) 接收消息并打印详细信息
     * 3) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录，包含所有元数据
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(
            groupId = "${kafka.consumer.group}",
            topicPartitions = {
                    @TopicPartition(
                            topic = "${kafka.topic.name}",
                            partitions = {"0", "1", "2"},
                            partitionOffsets = {
                                    @PartitionOffset(partition = "3", initialOffset = "3"),
                                    @PartitionOffset(partition = "4", initialOffset = "3")
                            })
            })
    public void consumeMessage(ConsumerRecord<String, String> record,
                              @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                              @Header(value = KafkaHeaders.OFFSET) String offset,
                              Acknowledgment ack) {
        try {
            // 打印接收到的消息详细信息
            log.info("=== 指定Topic、Partition、Offset消费演示 ===");
            log.info("主题: {}", topic);
            log.info("分区: {}", partition);
            log.info("偏移量: {}", offset);
            log.info("消息键: {}", record.key());
            log.info("消息值: {}", record.value());
            log.info("消息时间戳: {}", record.timestamp());
            log.info("消息头: {}", record.headers());
            log.info("完整消息记录: {}", record.toString());
            log.info("==========================================");
            
            // 模拟消息处理逻辑
            processMessage(record);
            
            // 手动确认消息处理完成
            ack.acknowledge();
            log.info("消息处理完成并已确认: topic={}, partition={}, offset={}", 
                    topic, partition, offset);
            
        } catch (Exception e) {
            log.error("处理消息时发生错误: topic={}, partition={}, offset={}, error={}", 
                    topic, partition, offset, e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 模拟消息处理逻辑
     * 
     * 功能说明：
     * 1) 模拟实际业务处理逻辑
     * 2) 根据不同的分区和偏移量进行不同的处理
     * 3) 演示消息处理的完整流程
     * 
     * @param record 消息记录
     * @throws Exception 处理异常
     */
    private void processMessage(ConsumerRecord<String, String> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();
        String value = record.value();
        
        log.info("开始处理消息: topic={}, partition={}, offset={}, key={}, value={}", 
                topic, partition, offset, key, value);
        
        // 根据分区进行不同的处理逻辑
        switch (partition) {
            case 0:
                log.info("处理分区0的消息: {}", value);
                break;
            case 1:
                log.info("处理分区1的消息: {}", value);
                break;
            case 2:
                log.info("处理分区2的消息: {}", value);
                break;
            case 3:
                log.info("处理分区3的消息（从偏移量3开始）: {}", value);
                break;
            case 4:
                log.info("处理分区4的消息（从偏移量3开始）: {}", value);
                break;
            default:
                log.warn("未知分区: {}", partition);
        }
        
        // 模拟处理时间
        Thread.sleep(100);
        
        log.info("消息处理完成: topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
