package com.action.kafka03offset.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka Topic配置类
 * 应用启动时自动创建Topic，避免手动命令行创建
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 创建演示Topic
     * 使用TopicBuilder构建Topic配置
     * 应用启动时会自动创建该Topic
     */
    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(KafkaConstants.TOPIC_DEMO)
                .partitions(KafkaConstants.DEMO_TOPIC_PARTITIONS)
                .replicas(KafkaConstants.DEMO_TOPIC_REPLICATION_FACTOR)
                .build();
    }
}


