package com.action.kafka12consumerinterceptor.config;

import com.action.kafka12consumerinterceptor.interceptor.MetricsConsumerInterceptor;
import com.action.kafka12consumerinterceptor.interceptor.MessageFilterInterceptor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 消费者配置类
 * 
 * 功能说明：
 * 1) 配置消费者基本参数：反序列化器、服务器地址等
 * 2) 注册自定义拦截器：按执行顺序配置拦截器链
 * 3) 创建消费者工厂和监听器容器工厂 Bean
 * 4) 自动创建演示用的 Topic
 * 
 * 关键配置说明：
 * - BOOTSTRAP_SERVERS_CONFIG: Kafka 集群地址
 * - KEY_DESERIALIZER_CLASS_CONFIG: 键反序列化器
 * - VALUE_DESERIALIZER_CLASS_CONFIG: 值反序列化器
 * - INTERCEPTOR_CLASSES_CONFIG: 拦截器类列表（按顺序执行）
 * - ENABLE_AUTO_COMMIT_CONFIG: 是否自动提交偏移量
 * - AUTO_OFFSET_RESET_CONFIG: 偏移量重置策略
 */
@Configuration
public class KafkaConfig {

    /**
     * Kafka 服务器地址配置
     * 从 application.properties 读取，默认 localhost:9092
     */
    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    /**
     * 演示用 Topic 名称配置
     * 从 application.properties 读取，默认 consumer-interceptor-demo
     */
    @Value("${demo.topic.name:consumer-interceptor-demo}")
    private String demoTopic;

    /**
     * 消费者组 ID 配置
     * 从 application.properties 读取，默认 consumer-interceptor-group
     */
    @Value("${demo.consumer.group:consumer-interceptor-group}")
    private String consumerGroup;

    /**
     * 消费者配置参数
     * 
     * 配置说明：
     * - 基本连接配置：服务器地址、反序列化器
     * - 拦截器配置：按顺序注册自定义拦截器
     * - 消费配置：消费者组、偏移量策略等
     * 
     * @return 消费者配置 Map
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        
        // 1. 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // 2. 反序列化器配置
        // 键和值都使用字符串反序列化器，适合演示场景
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // 3. 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        
        // 4. 偏移量配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // 手动提交偏移量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // 从最早的消息开始消费
        
        // 5. 拦截器配置 - 核心配置
        // 拦截器按列表顺序依次执行 onConsume() 方法
        // 如果某个拦截器抛出异常，后续拦截器不会执行
        List<Class<?>> interceptors = new ArrayList<>();
        interceptors.add(MessageFilterInterceptor.class);    // 第一个：过滤消息内容
        interceptors.add(MetricsConsumerInterceptor.class);  // 第二个：统计消费指标
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        
        // 6. 可选配置（可根据需要添加）
        // props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);     // 会话超时
        // props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);   // 心跳间隔
        // props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);         // 单次拉取最大记录数
        // props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);            // 最小拉取字节数
        // props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);        // 最大等待时间
        
        return props;
    }

    /**
     * 消费者工厂 Bean
     * 
     * 作用：创建 Kafka 消费者实例
     * 实现：使用 DefaultKafkaConsumerFactory 和上述配置参数
     * 
     * @return 消费者工厂实例
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * 监听器容器工厂 Bean
     * 
     * 作用：创建消息监听器容器，用于处理消费到的消息
     * 特点：支持并发消费，集成 Spring 事务管理
     * 
     * @param consumerFactory 消费者工厂
     * @return 监听器容器工厂实例
     */
    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory);
        
        // 配置手动确认模式
        listenerContainerFactory.getContainerProperties().setAckMode(
            org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );
        
        return listenerContainerFactory;
    }

    /**
     * 演示用 Topic Bean
     * 
     * 作用：自动创建用于演示的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * 
     * 注意：生产环境建议增加副本数和分区数
     * 
     * @return Topic 定义
     */
    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name(demoTopic)
                .partitions(3)    // 分区数：影响并行处理能力
                .replicas(1)      // 副本数：影响可用性（生产环境建议 >= 3）
                .build();
    }
}
