package com.action.kafka13consumersendto.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 * 
 * 功能说明：
 * 1) 配置 Kafka 主题创建
 * 2) 定义消息转发演示用的多个 Topic
 * 3) 支持自动创建 Topic，便于演示
 * 
 * 实现细节：
 * - 使用 @Configuration 注解标识为配置类
 * - 通过 @Value 注解读取配置文件中的 Topic 名称
 * - 使用 TopicBuilder 创建 Topic 定义
 * - 配置分区数和副本数，适合演示环境
 * 
 * 关键参数说明：
 * - partitions: 分区数，影响并行处理能力
 * - replicas: 副本数，影响可用性（生产环境建议 >= 3）
 */
@Configuration
public class KafkaConfig {

    /**
     * 源消息主题名称配置
     * 从 application.properties 读取，默认 source-topic
     */
    @Value("${kafka.topic.source:source-topic}")
    private String sourceTopicName;

    /**
     * 目标消息主题名称配置
     * 从 application.properties 读取，默认 target-topic
     */
    @Value("${kafka.topic.target:target-topic}")
    private String targetTopicName;

    /**
     * 消息转发演示用源 Topic Bean
     * 
     * 作用：自动创建用于接收原始消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic sourceTopic() {
        return TopicBuilder.name(sourceTopicName)
                .partitions(3)    // 分区数：影响并行处理能力，设置为3个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 消息转发演示用目标 Topic Bean
     * 
     * 作用：自动创建用于接收转发消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic targetTopic() {
        return TopicBuilder.name(targetTopicName)
                .partitions(3)    // 分区数：影响并行处理能力，设置为3个分区便于演示
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
