package com.action.kafka15cluster.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 配置类
 *
 * 功能：
 * 1) 演示在 Kafka 集群下创建 Topic（包含分区与副本）
 * 2) 与 README 中代码片段保持一致
 */
@Configuration
public class KafkaConfig {

    /**
     * 主题名称（与 README、控制器、生产者、消费者一致）
     */
    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopicName;

    /**
     * 创建演示 Topic：3 分区，3 副本
     * 注意：副本数不得超过 broker 节点数
     */
    @Bean
    public NewTopic clusterTopic() {
        return TopicBuilder.name(clusterTopicName)
                .partitions(3)
                .replicas(3)
                .build();
    }
}


