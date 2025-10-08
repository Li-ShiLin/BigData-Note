package com.action.kafka12consumerinterceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * 消费者拦截器演示消费者
 * 
 * 功能说明：
 * 1) 监听 Kafka 主题，接收经过拦截器处理的消息
 * 2) 演示拦截器在消息消费过程中的作用
 * 3) 支持手动确认消息处理
 * 4) 记录详细的消费日志信息
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解配置监听
 * - 使用 containerFactory 指定自定义的监听器容器工厂
 * - 使用 Acknowledgment 进行手动消息确认
 * - 通过 @Header 注解获取消息元数据信息
 * 
 * 关键参数说明：
 * - @KafkaListener: 配置监听的主题和消费者组
 * - ConsumerRecord: 完整的消息记录，包含所有元数据
 * - Acknowledgment: 手动确认对象，用于控制偏移量提交
 * - @Header: 获取消息头信息，如主题、分区、偏移量等
 */
@Component
public class ConsumerDemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoConsumer.class);

    /**
     * 消费消息演示
     * 
     * 监听配置说明：
     * 1) 监听 consumer-interceptor-demo 主题的所有消息
     * 2) 使用 consumer-interceptor-group 消费者组
     * 3) 使用自定义的监听器容器工厂（包含拦截器配置）
     * 
     * 执行流程：
     * 1) 消息经过拦截器链处理（过滤和统计）
     * 2) 消费者接收处理后的消息
     * 3) 打印消息详细信息
     * 4) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload ConsumerRecord: 完整的消息记录，包含所有元数据
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Header KafkaHeaders.OFFSET: 消息偏移量
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(
            topics = "${demo.topic.name:consumer-interceptor-demo}",
            groupId = "${demo.consumer.group:consumer-interceptor-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessage(ConsumerRecord<String, String> record,
                              @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                              @Header(value = KafkaHeaders.OFFSET) String offset,
                              Acknowledgment ack) {
        try {
            // 消费者进行消费 - 打印接收到的消息详细信息
            log.info("消费者进行消费: topic={}, partition={}, offset={}, key={}, value={}, timestamp={}, headers={}", 
                    topic, partition, offset, record.key(), record.value(), record.timestamp(), record.headers());
            
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
     * 2) 根据不同的消息内容进行不同的处理
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
        
        // 根据消息内容进行不同的处理逻辑
        if (value != null) {
            if (value.contains("important")) {
                log.info("处理重要消息: {}", value);
                // 模拟重要消息的特殊处理
                Thread.sleep(200);
            } else if (value.contains("urgent")) {
                log.info("处理紧急消息: {}", value);
                // 模拟紧急消息的快速处理
                Thread.sleep(50);
            } else {
                log.info("处理普通消息: {}", value);
                // 模拟普通消息的处理
                Thread.sleep(100);
            }
        }
        
        log.info("消息处理完成: topic={}, partition={}, offset={}", topic, partition, offset);
    }
}
