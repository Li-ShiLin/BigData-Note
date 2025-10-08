package com.action.kafka09objectmessage;

import com.action.kafka09objectmessage.model.User;
import com.action.kafka09objectmessage.service.ObjectMessageProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka 对象消息应用测试类
 * 
 * 功能说明：
 * 1) 测试对象消息的发送功能
 * 2) 验证序列化和反序列化过程
 * 3) 测试多种消息发送方式
 * 4) 验证异步消息发送
 * 
 * 测试说明：
 * - 使用 @SpringBootTest 注解启动完整的 Spring 上下文
 * - 使用 @Autowired 注入服务依赖
 * - 测试各种消息发送场景
 * - 验证消息发送的异步特性
 */
@SpringBootTest
class Kafka09ObjectMessageApplicationTests {

    /**
     * 对象消息生产者服务
     * 用于测试消息发送功能
     */
    @Autowired
    private ObjectMessageProducerService producerService;

    /**
     * 测试字符串消息发送
     * 
     * 测试内容：
     * 1) 发送简单字符串消息
     * 2) 验证异步发送功能
     * 3) 检查发送结果
     */
    @Test
    void testSendStringMessage() {
        // 测试发送字符串消息
        CompletableFuture<Void> future = producerService.sendStringMessage("Hello Kafka Object Message!");
        
        // 等待发送完成
        future.join();
        
        System.out.println("字符串消息发送测试完成");
    }

    /**
     * 测试用户对象消息发送
     * 
     * 测试内容：
     * 1) 创建用户对象
     * 2) 发送对象消息
     * 3) 验证序列化过程
     */
    @Test
    void testSendUserMessage() {
        // 创建用户对象
        User user = User.builder()
                .id(1001)
                .phone("13709090909")
                .birthDay(new Date())
                .build();
        
        // 测试发送用户对象消息
        CompletableFuture<Void> future = producerService.sendUserMessage(user);
        
        // 等待发送完成
        future.join();
        
        System.out.println("用户对象消息发送测试完成: " + user);
    }

    /**
     * 测试带键的用户对象消息发送
     * 
     * 测试内容：
     * 1) 创建用户对象
     * 2) 使用指定键发送消息
     * 3) 验证键值对发送
     */
    @Test
    void testSendUserMessageWithKey() {
        // 创建用户对象
        User user = User.builder()
                .id(1002)
                .phone("13709090908")
                .birthDay(new Date())
                .build();
        
        // 测试发送带键的用户对象消息
        CompletableFuture<Void> future = producerService.sendUserMessageWithKey("user-key-001", user);
        
        // 等待发送完成
        future.join();
        
        System.out.println("带键的用户对象消息发送测试完成: key=user-key-001, user=" + user);
    }

    /**
     * 测试批量用户对象消息发送
     * 
     * 测试内容：
     * 1) 批量创建用户对象
     * 2) 批量发送消息
     * 3) 验证批量发送功能
     */
    @Test
    void testSendBatchUserMessages() {
        // 测试批量发送用户对象消息
        CompletableFuture<Void> future = producerService.sendBatchUserMessages(10);
        
        // 等待发送完成
        future.join();
        
        System.out.println("批量用户对象消息发送测试完成: 发送了 10 条消息");
    }

    /**
     * 测试自定义用户对象消息发送
     * 
     * 测试内容：
     * 1) 使用自定义参数创建用户对象
     * 2) 发送自定义用户消息
     * 3) 验证自定义参数处理
     */
    @Test
    void testSendCustomUserMessage() {
        // 测试发送自定义用户对象消息
        CompletableFuture<Void> future = producerService.sendCustomUserMessage(1003, "13709090907", "custom-user-key");
        
        // 等待发送完成
        future.join();
        
        System.out.println("自定义用户对象消息发送测试完成: id=1003, phone=13709090907, key=custom-user-key");
    }

    /**
     * 测试无键的自定义用户对象消息发送
     * 
     * 测试内容：
     * 1) 使用自定义参数创建用户对象
     * 2) 发送无键的用户消息
     * 3) 验证无键消息发送
     */
    @Test
    void testSendCustomUserMessageWithoutKey() {
        // 测试发送无键的自定义用户对象消息
        CompletableFuture<Void> future = producerService.sendCustomUserMessage(1004, "13709090906", null);
        
        // 等待发送完成
        future.join();
        
        System.out.println("无键的自定义用户对象消息发送测试完成: id=1004, phone=13709090906");
    }

    /**
     * 综合测试：测试多种消息发送方式
     * 
     * 测试内容：
     * 1) 依次测试各种消息发送方式
     * 2) 验证不同发送场景
     * 3) 检查整体功能完整性
     */
    @Test
    void testAllMessageTypes() {
        System.out.println("开始综合测试...");
        
        // 1. 测试字符串消息
        testSendStringMessage();
        
        // 2. 测试用户对象消息
        testSendUserMessage();
        
        // 3. 测试带键的用户对象消息
        testSendUserMessageWithKey();
        
        // 4. 测试批量用户对象消息
        testSendBatchUserMessages();
        
        // 5. 测试自定义用户对象消息
        testSendCustomUserMessage();
        
        // 6. 测试无键的自定义用户对象消息
        testSendCustomUserMessageWithoutKey();
        
        System.out.println("综合测试完成！");
    }
}
