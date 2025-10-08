package com.action.kafka09objectmessage.service;

import com.action.kafka0common.common.CommonUtils;
import com.action.kafka09objectmessage.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * 对象消息生产者服务
 * 
 * 功能说明：
 * 1) 封装 KafkaTemplate 对象消息发送逻辑
 * 2) 提供多种对象消息发送方式
 * 3) 使用公共包中的 CommonUtils 进行对象序列化
 * 4) 支持异步消息发送
 * 
 * 实现细节：
 * - 使用构造函数注入 KafkaTemplate，确保线程安全
 * - 通过 @Value 注解读取配置的 Topic 名称
 * - 使用 CommonUtils.convert() 方法将对象转换为 JSON 字符串
 * - 返回 CompletableFuture 支持异步处理和回调
 * 
 * 关键参数说明：
 * - KafkaTemplate: Spring 提供的 Kafka 操作模板，线程安全
 * - CommonUtils: 公共包中的工具类，用于对象序列化
 * - CompletableFuture: 异步编程模型，支持链式操作和异常处理
 */
@Service
public class ObjectMessageProducerService {

    /**
     * Kafka 操作模板
     * 特点：线程安全，支持同步和异步发送
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 字符串消息 Topic 名称
     * 从配置文件读取，默认 string-message-topic
     */
    @Value("${kafka.topic.string:string-message-topic}")
    private String stringTopicName;

    /**
     * 用户对象消息 Topic 名称
     * 从配置文件读取，默认 user-message-topic
     */
    @Value("${kafka.topic.user:user-message-topic}")
    private String userTopicName;

    /**
     * 通用对象消息 Topic 名称
     * 从配置文件读取，默认 object-message-topic
     */
    @Value("${kafka.topic.object:object-message-topic}")
    private String objectTopicName;

    /**
     * 构造函数注入 KafkaTemplate
     * 
     * @param kafkaTemplate Spring 管理的 KafkaTemplate Bean
     */
    public ObjectMessageProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送简单字符串消息
     * 
     * 执行流程：
     * 1) 直接发送字符串消息到字符串消息主题
     * 2) 返回 CompletableFuture 用于异步处理结果
     * 
     * @param message 要发送的字符串消息
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendStringMessage(String message) {
        // 使用 KafkaTemplate 发送字符串消息到字符串消息主题
        // send() 方法直接返回 CompletableFuture<SendResult>
        // thenApply() 将结果转换为 Void，简化调用方处理
        return kafkaTemplate.send(stringTopicName, message)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送用户对象消息（无键）
     * 
     * 执行流程：
     * 1) 创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 发送 JSON 字符串到用户对象消息主题
     * 
     * @param user 要发送的用户对象
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserMessage(User user) {
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 发送 JSON 字符串到用户对象消息主题
        return kafkaTemplate.send(userTopicName, userJson)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 发送带键的用户对象消息
     * 
     * 执行流程：
     * 1) 创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 使用指定的键发送消息到用户对象消息主题
     * 
     * @param key 消息键（用于分区路由）
     * @param user 要发送的用户对象
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendUserMessageWithKey(String key, User user) {
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 使用指定的键发送 JSON 字符串到用户对象消息主题
        return kafkaTemplate.send(userTopicName, key, userJson)
                .thenApply(recordMetadata -> null);
    }

    /**
     * 批量发送用户对象消息
     * 
     * 执行流程：
     * 1) 循环创建多个 User 对象
     * 2) 为每个对象生成唯一的键
     * 3) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 4) 批量发送到用户对象消息主题
     * 
     * @param count 要发送的消息数量
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendBatchUserMessages(int count) {
        // 创建 CompletableFuture 数组，用于并行发送
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        
        for (int i = 0; i < count; i++) {
            // 创建用户对象
            User user = User.builder()
                    .id(i)
                    .phone("1370909090" + i)
                    .birthDay(new Date())
                    .build();
            
            // 生成唯一的键
            String key = "user-" + i;
            
            // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
            String userJson = CommonUtils.convert(user, String.class);
            
            // 发送消息到用户对象消息主题并存储 Future
            futures[i] = kafkaTemplate.send(userTopicName, key, userJson)
                    .thenApply(recordMetadata -> null);
        }
        
        // 等待所有消息发送完成
        return CompletableFuture.allOf(futures);
    }

    /**
     * 发送自定义用户对象消息
     * 
     * 执行流程：
     * 1) 根据参数创建 User 对象
     * 2) 使用 CommonUtils.convert() 将对象转换为 JSON 字符串
     * 3) 发送到用户对象消息主题
     * 
     * @param id 用户ID
     * @param phone 手机号码
     * @param key 消息键（可选）
     * @return CompletableFuture<Void> 异步发送结果
     */
    public CompletableFuture<Void> sendCustomUserMessage(int id, String phone, String key) {
        // 创建用户对象
        User user = User.builder()
                .id(id)
                .phone(phone)
                .birthDay(new Date())
                .build();
        
        // 使用公共包中的 CommonUtils 将 User 对象转换为 JSON 字符串
        String userJson = CommonUtils.convert(user, String.class);
        
        // 根据是否有键选择发送方式
        if (key != null && !key.isEmpty()) {
            return kafkaTemplate.send(userTopicName, key, userJson)
                    .thenApply(recordMetadata -> null);
        } else {
            return kafkaTemplate.send(userTopicName, userJson)
                    .thenApply(recordMetadata -> null);
        }
    }
}
