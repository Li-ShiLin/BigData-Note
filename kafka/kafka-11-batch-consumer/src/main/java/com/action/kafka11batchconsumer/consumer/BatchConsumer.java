package com.action.kafka11batchconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 批量消息消费演示消费者
 * 
 * 功能说明：
 * 1) 演示如何批量消费Kafka消息
 * 2) 监听指定主题的所有消息
 * 3) 支持批量处理多条消息
 * 4) 支持手动确认消息处理
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解配置监听
 * - 使用 topics 属性指定要监听的主题
 * - 使用 List<ConsumerRecord> 接收批量消息
 * - 使用 Acknowledgment 进行手动消息确认
 * 
 * 关键参数说明：
 * - topics: 要监听的主题名称
 * - groupId: 消费者组ID，用于负载均衡
 * - 注意：批量消费需要配置 spring.kafka.listener.type=batch
 */
@Component
public class BatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchConsumer.class);

    /**
     * 批量消息消费演示
     * 
     * 监听配置说明：
     * 1) 监听 batch-consumer-demo 主题的所有消息
     * 2) 使用批量消费模式，每次最多消费20条消息
     * 3) 使用手动确认模式，确保消息处理完成后再确认
     * 
     * 执行流程：
     * 1) 消费者启动后开始监听指定主题
     * 2) 接收批量消息并打印详细信息
     * 3) 批量处理消息
     * 4) 手动确认消息处理完成
     * 
     * 参数说明：
     * - List<ConsumerRecord> records: 批量消息记录列表
     * - Acknowledgment ack: 手动确认对象
     * 
     * 注意：在批量消费模式下，不能使用 @Header 注解获取主题信息，
     * 需要从 ConsumerRecord 中获取主题信息
     */
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.group}")
    public void consumeBatchMessages(List<ConsumerRecord<String, String>> records,
                                   Acknowledgment ack) {
        try {
            // 从第一条消息记录中获取主题信息
            String topic = records.isEmpty() ? "unknown" : records.get(0).topic();
            
            // 打印接收到的批量消息信息
            log.info("=== 批量消息消费演示 ===");
            log.info("主题: {}", topic);
            log.info("批量消息数量: {}", records.size());
            log.info("==========================================");
            
            // 批量处理消息
            processBatchMessages(records);
            
            // 手动确认所有消息处理完成
            ack.acknowledge();
            log.info("批量消息处理完成并已确认: 主题={}, 数量={}", topic, records.size());
            
        } catch (Exception e) {
            String topic = records.isEmpty() ? "unknown" : records.get(0).topic();
            log.error("处理批量消息时发生错误: 主题={}, 数量={}, 错误={}", 
                    topic, records.size(), e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 批量处理消息逻辑
     * 
     * 功能说明：
     * 1) 遍历批量消息列表
     * 2) 打印每条消息的详细信息
     * 3) 模拟实际业务处理逻辑
     * 4) 演示批量处理的优势
     * 
     * @param records 批量消息记录列表
     * @throws Exception 处理异常
     */
    private void processBatchMessages(List<ConsumerRecord<String, String>> records) throws Exception {
        log.info("开始批量处理消息，数量: {}", records.size());
        
        // 遍历批量消息
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> record = records.get(i);
            
            // 根据分区确定处理描述
            String partitionDesc = switch (record.partition()) {
                case 0 -> "分区0";
                case 1 -> "分区1";
                case 2 -> "分区2";
                default -> "未知分区" + record.partition();
            };
            
            // 打印单条消息信息（合并成一行，包含分区处理信息）
            log.info("批量消息[{}]: topic={}, partition={}, offset={}, key={}, value={}, timestamp={}, 处理{}: {}", 
                    i + 1, record.topic(), record.partition(), record.offset(), 
                    record.key(), record.value(), record.timestamp(), partitionDesc, record.value());
        }
        
        log.info("批量消息处理完成，总数量: {}", records.size());
    }

}
