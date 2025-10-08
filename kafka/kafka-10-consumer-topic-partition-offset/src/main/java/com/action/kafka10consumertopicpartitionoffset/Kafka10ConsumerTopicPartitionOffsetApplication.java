package com.action.kafka10consumertopicpartitionoffset;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 指定Topic、Partition、Offset消费演示应用主启动类
 * 
 * 功能说明：
 * 1) Spring Boot 应用的主入口
 * 2) 自动配置 Spring Boot 和 Kafka 相关组件
 * 3) 启动内嵌的 Tomcat 服务器
 * 4) 启动 Kafka 消费者监听器
 * 
 * 实现细节：
 * - 使用 @SpringBootApplication 注解标识为 Spring Boot 应用
 * - 自动扫描当前包及子包下的所有组件
 * - 自动配置 Kafka 相关的 Bean
 * - 启动后监听指定的 Topic、Partition、Offset
 * 
 * 启动后功能：
 * - 监听 topic-partition-offset-demo 主题的 0、1、2 号分区（从最新偏移量开始）
 * - 监听 topic-partition-offset-demo 主题的 3 号分区（从偏移量 3 开始）
 * - 监听 topic-partition-offset-demo 主题的 4 号分区（从偏移量 3 开始）
 * - 提供 HTTP 接口用于测试消息发送
 */
@SpringBootApplication
public class Kafka10ConsumerTopicPartitionOffsetApplication {

    /**
     * 应用主入口方法
     * 
     * 执行流程：
     * 1) 启动 Spring Boot 应用上下文
     * 2) 自动配置所有必要的 Bean
     * 3) 启动内嵌的 Tomcat 服务器（端口 9090）
     * 4) 启动 Kafka 消费者监听器
     * 5) 开始监听指定的 Topic、Partition、Offset
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        SpringApplication.run(Kafka10ConsumerTopicPartitionOffsetApplication.class, args);
    }
}