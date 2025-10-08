package com.action.kafka13consumersendto.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 消息转发演示生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供多种消息发送方式，便于测试消息转发
 * 3) 支持指定分区发送消息
 * 4) 支持异步消息发送
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 返回 CompletableFuture 支持异步处理和回调
 * - 提供多种发送方式，便于测试不同场景
 * 
 * 关键参数说明：
 * - KafkaTemplate: Spring 提供的 Kafka 操作模板，线程安全
 * - CompletableFuture: 异步编程模型，支持链式操作和异常处理
 */
@Service
public class MessageForwardProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 源消息 Topic 名称
     * 从配置文件读取，默认 source-topic
     */
    @Value("${kafka.topic.source:source-topic}")
    private String sourceTopicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public MessageForwardProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送基础转发测试消息
     * 
     * 执行流程：
     * 1) 发送消息到源主题
     * 2) 消息会被转发消费者接收并转发
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param message 要发送的消息内容
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBasicForwardMessage(String message) {
        // 使用 KafkaTemplate 发送消息到源主题
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(sourceTopicName, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送带键的转发测试消息
     * 
     * 执行流程：
     * 1) 使用指定的键发送消息到源主题
     * 2) 消息会被转发消费者接收并转发
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param key 消息键（用于分区路由）
     * @param message 要发送的消息内容
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendForwardMessageWithKey(String key, String message) {
        // 使用 KafkaTemplate 发送带键的消息到源主题
        return kafkaTemplate.send(sourceTopicName, key, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送增强转发测试消息
     * 
     * 执行流程：
     * 1) 发送消息到源主题
     * 2) 消息会被增强转发消费者接收并处理
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param message 要发送的消息内容
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendEnhanceForwardMessage(String message) {
        // 使用 KafkaTemplate 发送消息到源主题
        return kafkaTemplate.send(sourceTopicName, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送条件转发测试消息
     * 
     * 执行流程：
     * 1) 发送消息到源主题
     * 2) 消息会被条件转发消费者接收并判断是否转发
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param message 要发送的消息内容
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendConditionalForwardMessage(String message) {
        // 使用 KafkaTemplate 发送消息到源主题
        return kafkaTemplate.send(sourceTopicName, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 批量发送转发测试消息
     * 
     * 执行流程：
     * 1) 循环发送指定数量的消息到源主题
     * 2) 为每个消息生成唯一的内容
     * 3) 消息会被转发消费者接收并转发
     * 4) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchForwardMessages(int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成唯一的消息内容
            String message = "forward test message " + i + " at " + System.currentTimeMillis();
            
            // 发送消息到源主题并存储 Future
            futures[i] = kafkaTemplate.send(sourceTopicName, message)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送不同类型的转发测试消息
     * 
     * 执行流程：
     * 1) 发送多种类型的消息到源主题
     * 2) 演示不同类型的消息转发
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendVariousForwardMessages() {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[4];
        
        // 基础转发消息
        futures[0] = kafkaTemplate.send(sourceTopicName, "basic forward message")
                .thenApply(recordMetadata -> null);
        
        // 增强转发消息
        futures[1] = kafkaTemplate.send(sourceTopicName, "enhance forward message")
                .thenApply(recordMetadata -> null);
        
        // 条件转发消息（满足条件）
        futures[2] = kafkaTemplate.send(sourceTopicName, "conditional forward test message")
                .thenApply(recordMetadata -> null);
        
        // 条件转发消息（不满足条件）
        futures[3] = kafkaTemplate.send(sourceTopicName, "skip message")
                .thenApply(recordMetadata -> null);
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送指定分区的转发测试消息
     * 
     * 执行流程：
     * 1) 发送消息到指定分区
     * 2) 消息会被转发消费者接收并转发
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param message 要发送的消息内容
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendForwardMessageToPartition(int partition, String message) {
        // 使用 KafkaTemplate 发送消息到指定分区
        return kafkaTemplate.send(sourceTopicName, partition, null, message)
                .thenApply(recordMetadata -> null);
    }
}
