package com.action.kafka02springboot.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(KafkaConstants.TOPIC_USERS)
                        .partitions(KafkaConstants.USERS_TOPIC_PARTITIONS)
                        .replicas(KafkaConstants.USERS_TOPIC_REPLICATION_FACTOR)
                        .build(),
                // 可以添加更多Topic
                TopicBuilder.name(KafkaConstants.TOPIC_ORDERS)
                        .partitions(5)
                        .replicas(1)
                        .build()
        );
    }
}