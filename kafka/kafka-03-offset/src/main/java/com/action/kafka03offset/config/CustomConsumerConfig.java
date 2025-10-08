package com.action.kafka03offset.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Kafka消费者配置类
 * 演示不同偏移量策略的消费者配置：
 * 1. latestOffsetFactory - 从最新位置开始消费（默认策略）
 * 2. earliestOffsetFactory - 从最早位置开始消费
 * 3. manualOffsetFactory - 手动控制偏移量提交
 */
@Configuration
public class CustomConsumerConfig {

    /**
     * 最新偏移量消费者工厂
     * 使用默认的auto.offset.reset=latest策略
     * 新消费者组从最新位置开始消费，不会收到历史消息
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> latestOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    /**
     * 最早偏移量消费者工厂
     * 显式设置auto.offset.reset=earliest策略
     * 新消费者组从最早位置开始消费，会收到所有历史消息
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> earliestOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {

        // 复制原有配置并添加earliest策略
        Map<String, Object> props = new HashMap<>();
        props.putAll(consumerFactory.getConfigurationProperties());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建新的消费者工厂
        ConsumerFactory<String, String> earliestConsumerFactory =
                new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(earliestConsumerFactory);
        return factory;
    }

    /**
     * 手动偏移量消费者工厂
     * 设置手动立即提交偏移量模式
     * 允许精确控制消息处理完成后再提交偏移量
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualOffsetFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 设置手动立即提交模式，需要手动调用ack.acknowledge()
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}


