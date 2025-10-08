package com.action.kafka11batchconsumer.service;

import com.action.kafka0common.common.CommonUtils;
import com.action.kafka11batchconsumer.model.UserBehavior;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 批量消息发送演示生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供多种批量消息发送方式，便于测试批量消费
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
public class BatchProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 目标 Topic 名称
     * 从配置文件读取，默认 batch-consumer-demo
     */
    @Value("${kafka.topic.name:batch-consumer-demo}")
    private String topicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public BatchProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送单条消息到指定分区
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
     * 发送单条消息到指定分区（无键）
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
     * 发送单条消息到所有分区（让Kafka自动分区）
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
     * 发送单条消息到所有分区（无键，让Kafka自动分区）
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
     * 批量发送消息到所有分区（用于测试自动分区）
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
     * 发送批量消息到指定分区（模拟真实业务场景）
     * 
     * 执行流程：
     * 1) 模拟发送用户行为数据
     * 2) 为每个消息生成业务相关的键和值
     * 3) 使用CommonUtils进行对象到字符串的转换
     * 4) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param partition 目标分区号
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserBehaviorMessages(int partition, int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 生成业务相关的用户行为数据
            String userId = "user-" + (i % 100); // 模拟100个用户
            String action = i % 3 == 0 ? "login" : (i % 3 == 1 ? "view" : "purchase");
            Long timestamp = System.currentTimeMillis();
            
            // 创建用户行为对象
            UserBehavior userBehavior = new UserBehavior(userId, action, timestamp, partition);
            
            // 使用CommonUtils将对象转换为JSON字符串
            String key = userId;
            String value = CommonUtils.convert(userBehavior, String.class);
            
            // 发送消息到指定分区并存储 Future
            futures[i] = kafkaTemplate.send(topicName, partition, key, value)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }
}
