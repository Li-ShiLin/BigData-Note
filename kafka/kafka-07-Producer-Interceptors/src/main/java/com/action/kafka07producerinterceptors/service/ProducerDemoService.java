package com.action.kafka07producerinterceptors.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 生产者演示服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 消息发送逻辑
 * 2) 提供异步消息发送接口
 * 3) 演示拦截器在消息发送过程中的作用
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 返回 CompletableFuture 支持异步处理和回调
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
     * 从配置文件读取，默认 demo-interceptor-topic
     */
    @Value("${demo.topic.name:demo-interceptor-topic}")
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
     * 发送消息到 Kafka
     * 
     * 执行流程：
     * 1) 调用 KafkaTemplate.send() 发送消息
     * 2) 消息会经过配置的拦截器链处理
     * 3) 返回 CompletableFuture 用于异步处理结果
     * 
     * 拦截器执行顺序：
     * - ModifyRecordInterceptor: 修改消息内容，添加前缀和 Header
     * - MetricsProducerInterceptor: 统计发送成功/失败次数
     * 
     * @param key 消息键（可选，用于分区路由）
     * @param value 消息值（必填，实际的消息内容）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendMessage(String key, String value) {
        // 使用 KafkaTemplate 发送消息
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(topicName, key, value)
                .thenApply(recordMetadata -> null);
    }
}


