package com.action.kafka06producerpartitionstrategy.config;

import com.action.kafka06producerpartitionstrategy.partition.CustomPartitioner;
import com.action.kafka06producerpartitionstrategy.partition.KeyBasedPartitioner;
import com.action.kafka06producerpartitionstrategy.partition.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 配置类
 * 演示不同的分区策略
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * 默认分区策略的生产者工厂
     */
    @Bean("defaultProducerFactory")
    public ProducerFactory<String, String> defaultProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 使用默认分区策略（轮询）
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 自定义分区策略的生产者工厂
     */
    @Bean("customPartitionProducerFactory")
    public ProducerFactory<String, String> customPartitionProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 使用自定义分区策略
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 基于 Key 的分区策略生产者工厂
     */
    @Bean("keyBasedPartitionProducerFactory")
    public ProducerFactory<String, String> keyBasedPartitionProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 使用基于 Key 的分区策略
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KeyBasedPartitioner.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 轮询分区策略生产者工厂
     */
    @Bean("roundRobinPartitionProducerFactory")
    public ProducerFactory<String, String> roundRobinPartitionProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 使用轮询分区策略
        configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * 默认分区策略的 KafkaTemplate
     */
    @Bean("defaultKafkaTemplate")
    public KafkaTemplate<String, String> defaultKafkaTemplate() {
        return new KafkaTemplate<>(defaultProducerFactory());
    }

    /**
     * 自定义分区策略的 KafkaTemplate
     */
    @Bean("customPartitionKafkaTemplate")
    public KafkaTemplate<String, String> customPartitionKafkaTemplate() {
        return new KafkaTemplate<>(customPartitionProducerFactory());
    }

    /**
     * 基于 Key 分区策略的 KafkaTemplate
     */
    @Bean("keyBasedPartitionKafkaTemplate")
    public KafkaTemplate<String, String> keyBasedPartitionKafkaTemplate() {
        return new KafkaTemplate<>(keyBasedPartitionProducerFactory());
    }

    /**
     * 轮询分区策略的 KafkaTemplate
     */
    @Bean("roundRobinPartitionKafkaTemplate")
    public KafkaTemplate<String, String> roundRobinPartitionKafkaTemplate() {
        return new KafkaTemplate<>(roundRobinPartitionProducerFactory());
    }
}
