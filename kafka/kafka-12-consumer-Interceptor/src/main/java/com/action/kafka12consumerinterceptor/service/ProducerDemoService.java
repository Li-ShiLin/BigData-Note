package com.action.kafka12consumerinterceptor.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 生产者演示服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供多种消息发送方式，便于测试消费者拦截器
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
public class ProducerDemoService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 目标 Topic 名称
     * 从配置文件读取，默认 consumer-interceptor-demo
     */
    @Value("${demo.topic.name:consumer-interceptor-demo}")
    private String topicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public ProducerDemoService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送消息到指定分区
     * 
     * 执行流程：
     * 1) 使用指定的分区号发送消息
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param key 消息键（可选）
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessageToPartition(int partition, String key, String value) {
        // 使用 KafkaTemplate 发送消息到指定分区
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(topicName, partition, key, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到指定分区（无键）
     * 
     * 执行流程：
     * 1) 使用指定的分区号发送消息（无键）
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessageToPartition(int partition, String value) {
        // 使用 KafkaTemplate 发送消息到指定分区（无键）
        return kafkaTemplate.send(topicName, partition, null, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到所有分区（让Kafka自动分区）
     * 
     * 执行流程：
     * 1) 发送消息，让Kafka根据键的哈希值自动选择分区
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param key 消息键（用于分区路由）
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String key, String value) {
        // 使用 KafkaTemplate 发送消息，让Kafka自动选择分区
        return kafkaTemplate.send(topicName, key, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送消息到所有分区（无键，让Kafka自动分区）
     * 
     * 执行流程：
     * 1) 发送消息（无键），让Kafka自动选择分区
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param value 消息值
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String value) {
        // 使用 KafkaTemplate 发送消息（无键），让Kafka自动选择分区
        return kafkaTemplate.send(topicName, value)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 批量发送消息到指定分区
     * 
     * 执行流程：
     * 1) 循环发送指定数量的消息到指定分区
     * 2) 为每个消息生成唯一的键和值
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchMessagesToPartition(int partition, int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成唯一的键和值
            String key = "partition-" + partition + "-key-" + i;
            String value = "partition-" + partition + "-message-" + i;
            
            // 发送消息到指定分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, partition, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送消息到所有分区（用于测试自动分区）
     * 
     * 执行流程：
     * 1) 循环发送指定数量的消息到所有分区
     * 2) 为每个消息生成唯一的键和值
     * 3) 让Kafka根据键的哈希值自动选择分区
     * 4) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchMessages(int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成唯一的键和值
            String key = "auto-partition-key-" + i;
            String value = "auto-partition-message-" + i;
            
            // 发送消息，让Kafka自动选择分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送重要消息（用于测试拦截器过滤功能）
     * 
     * 执行流程：
     * 1) 发送包含 "important" 关键词的消息
     * 2) 这些消息会被拦截器保留
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param key 消息键（可选）
     * @param message 消息内容（不包含 "important" 关键词）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendImportantMessage(String key, String message) {
        // 为消息添加 "important" 关键词，确保被拦截器保留
        String importantMessage = "This is an important message: " + message;
        return kafkaTemplate.send(topicName, key, importantMessage)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送普通消息（用于测试拦截器过滤功能）
     * 
     * 执行流程：
     * 1) 发送不包含 "important" 关键词的消息
     * 2) 这些消息会被拦截器过滤掉
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param key 消息键（可选）
     * @param message 消息内容（不包含 "important" 关键词）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendNormalMessage(String key, String message) {
        // 发送普通消息，不包含 "important" 关键词，会被拦截器过滤
        return kafkaTemplate.send(topicName, key, message)
                .thenApply(recordMetadata -> null);
    }
}
