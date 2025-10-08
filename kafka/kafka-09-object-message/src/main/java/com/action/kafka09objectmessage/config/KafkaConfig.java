package com.action.kafka09objectmessage.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 * 
 * 功能说明：
 * 1) 配置多个 Kafka 主题创建
 * 2) 为不同类型的消息定义不同的 Topic
 * 3) 支持自动创建 Topic，便于演示
 * 
 * 实现细节：
 * - 使用 @Configuration 注解标识为配置类
 * - 通过 @Value 注解读取配置文件中的 Topic 名称
 * - 使用 TopicBuilder 创建多个 Topic 定义
 * - 配置分区数和副本数，适合演示环境
 * 
 * 关键参数说明：
 * - partitions: 分区数，影响并行处理能力
 * - replicas: 副本数，影响可用性（生产环境建议 >= 3）
 */
@Configuration
public class KafkaConfig {

    /**
     * 字符串消息主题名称配置
     * 从 application.yml 读取，默认 string-message-topic
     */
    @Value("${kafka.topic.string:string-message-topic}")
    private String stringTopicName;

    /**
     * 用户对象消息主题名称配置
     * 从 application.yml 读取，默认 user-message-topic
     */
    @Value("${kafka.topic.user:user-message-topic}")
    private String userTopicName;

    /**
     * 通用对象消息主题名称配置
     * 从 application.yml 读取，默认 object-message-topic
     */
    @Value("${kafka.topic.object:object-message-topic}")
    private String objectTopicName;

    /**
     * 字符串消息演示用 Topic Bean
     * 
     * 作用：自动创建用于字符串消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic stringMessageTopic() {
        return TopicBuilder.name(stringTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 用户对象消息演示用 Topic Bean
     * 
     * 作用：自动创建用于用户对象消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic userMessageTopic() {
        return TopicBuilder.name(userTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }

    /**
     * 通用对象消息演示用 Topic Bean
     * 
     * 作用：自动创建用于通用对象消息的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic objectMessageTopic() {
        return TopicBuilder.name(objectTopicName)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
