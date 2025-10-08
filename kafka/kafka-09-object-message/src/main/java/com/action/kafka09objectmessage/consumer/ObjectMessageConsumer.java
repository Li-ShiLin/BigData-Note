package com.action.kafka09objectmessage.consumer;

import com.action.kafka0common.common.CommonUtils;
import com.action.kafka09objectmessage.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 对象消息消费者
 * 
 * 功能说明：
 * 1) 监听 Kafka 主题，接收对象消息
 * 2) 使用公共包中的 CommonUtils 进行对象反序列化
 * 3) 支持手动确认消息处理
 * 4) 提供多种消息处理方式
 * 
 * 实现细节：
 * - 使用 @KafkaListener 注解监听指定主题
 * - 使用 @Payload 注解获取消息内容
 * - 使用 @Header 注解获取消息元数据
 * - 使用 Acknowledgment 进行手动消息确认
 * - 使用 CommonUtils.convertString() 将 JSON 字符串转换为对象
 * 
 * 关键参数说明：
 * - @KafkaListener: Spring Kafka 提供的消息监听注解
 * - @Payload: 获取消息体内容
 * - @Header: 获取消息头信息（主题、分区等）
 * - Acknowledgment: 手动确认消息处理完成
 */
@Component
public class ObjectMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(ObjectMessageConsumer.class);

    /**
     * 监听字符串消息主题，接收字符串消息
     * 
     * 执行流程：
     * 1) 监听 string-message-topic 主题
     * 2) 使用 stringGroup 消费者组
     * 3) 接收字符串消息并打印日志
     * 4) 手动确认消息处理完成
     * 
     * 参数说明：
     * - @Payload String event: 消息体内容（字符串）
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"string-message-topic"}, groupId = "stringGroup")
    public void onStringMessage(@Payload String event,
                               @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                               @Payload ConsumerRecord<String, String> record,
                               Acknowledgment ack) {
        try {
            // 打印接收到的字符串消息
            log.info("接收到字符串消息: {}, topic: {}, partition: {}", event, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 手动确认消息处理完成
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理字符串消息时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 监听用户对象消息主题，接收用户对象消息
     * 
     * 执行流程：
     * 1) 监听 user-message-topic 主题
     * 2) 使用 userGroup 消费者组
     * 3) 接收 JSON 字符串并转换为 User 对象
     * 4) 打印用户信息并手动确认消息
     * 
     * 参数说明：
     * - @Payload String userJson: 用户对象的 JSON 字符串
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"user-message-topic"}, groupId = "userGroup")
    public void onUserMessage(String userJson,
                             @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                             @Payload ConsumerRecord<String, String> record,
                             Acknowledgment ack) {
        try {
            // 使用公共包中的 CommonUtils 将 JSON 字符串转换为 User 对象
            User user = CommonUtils.convertString(userJson, User.class);
            
            // 打印接收到的用户对象消息
            log.info("接收到用户对象消息: {}, topic: {}, partition: {}", user, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 手动确认消息处理完成
            ack.acknowledge();
        } catch (Exception e) {
            log.error("处理用户对象消息时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 监听用户对象消息主题，接收用户对象消息（带异常处理演示）
     * 
     * 执行流程：
     * 1) 监听 user-message-topic 主题
     * 2) 使用 userExceptionGroup 消费者组
     * 3) 接收 JSON 字符串并转换为 User 对象
     * 4) 模拟业务处理（包含异常情况）
     * 5) 根据处理结果决定是否确认消息
     * 
     * 参数说明：
     * - @Payload String userJson: 用户对象的 JSON 字符串
     * - @Header KafkaHeaders.RECEIVED_TOPIC: 消息来源主题
     * - @Header KafkaHeaders.RECEIVED_PARTITION: 消息来源分区
     * - @Payload ConsumerRecord: 完整的消息记录
     * - Acknowledgment ack: 手动确认对象
     */
    @KafkaListener(topics = {"user-message-topic"}, groupId = "userExceptionGroup")
    public void onUserMessageWithExceptionHandling(String userJson,
                                                  @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
                                                  @Header(value = KafkaHeaders.RECEIVED_PARTITION) String partition,
                                                  @Payload ConsumerRecord<String, String> record,
                                                  Acknowledgment ack) {
        try {
            // 使用公共包中的 CommonUtils 将 JSON 字符串转换为 User 对象
            User user = CommonUtils.convertString(userJson, User.class);
            
            // 打印接收到的用户对象消息
            log.info("接收到用户对象消息（异常处理演示）: {}, topic: {}, partition: {}", user, topic, partition);
            log.info("完整消息记录: {}", record.toString());
            
            // 模拟业务处理
            // 这里可以添加实际的业务逻辑，如数据库操作、外部服务调用等
            processUserBusiness(user);
            
            // 业务处理完成，手动确认消息
            ack.acknowledge();
            log.info("用户对象消息处理完成并已确认: userId={}", user.getId());
            
        } catch (Exception e) {
            log.error("处理用户对象消息时发生业务错误: {}", e.getMessage(), e);
            // 注意：这里不调用 ack.acknowledge()，消息会被重新消费
            // 在实际生产环境中，可能需要实现重试机制或死信队列
        }
    }

    /**
     * 模拟用户业务处理
     * 
     * 功能说明：
     * 1) 模拟用户相关的业务处理逻辑
     * 2) 包含异常情况演示
     * 3) 用于演示消息处理失败时的行为
     * 
     * @param user 要处理的用户对象
     * @throws Exception 业务处理异常
     */
    private void processUserBusiness(User user) throws Exception {
        // 模拟业务处理逻辑
        log.info("开始处理用户业务: userId={}, phone={}", user.getId(), user.getPhone());
        
        // 模拟处理时间
        Thread.sleep(100);
        
        // 模拟异常情况（当用户ID为特定值时抛出异常）
        if (user.getId() == 999) {
            throw new RuntimeException("模拟业务处理异常：用户ID为999时处理失败");
        }
        
        // 模拟其他业务逻辑
        if (user.getPhone() == null || user.getPhone().isEmpty()) {
            throw new IllegalArgumentException("手机号码不能为空");
        }
        
        log.info("用户业务处理完成: userId={}", user.getId());
    }
}
