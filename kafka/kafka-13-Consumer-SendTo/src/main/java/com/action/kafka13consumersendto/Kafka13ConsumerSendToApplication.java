package com.action.kafka13consumersendto;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka 消息转发演示应用主启动类
 * 
 * 功能说明：
 * 1) Spring Boot 应用启动入口
 * 2) 自动配置 Kafka 相关组件
 * 3) 启动消息转发演示服务
 * 
 * 实现细节：
 * - 使用 @SpringBootApplication 注解标识为 Spring Boot 应用
 * - 自动扫描和配置相关组件
 * - 启动内嵌的 Web 服务器
 * 
 * 启动后功能：
 * - 自动创建 Kafka Topic
 * - 启动消息转发消费者
 * - 启动目标消息消费者
 * - 提供 REST API 接口用于测试
 */
@SpringBootApplication
public class Kafka13ConsumerSendToApplication {

    public static void main(String[] args) {
        SpringApplication.run(Kafka13ConsumerSendToApplication.class, args);
    }
}