package com.action.kafka08eventconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消费者配置类
 * <p>
 * 功能说明：
 * 1) 配置消费者基本参数：反序列化器、服务器地址、消费者组等
 * 2) 配置多种监听器容器工厂：支持不同的消费模式
 * 3) 创建演示用的 Topic
 * 4) 支持自动提交和手动提交两种模式
 * <p>
 * 关键配置说明：
 * - BOOTSTRAP_SERVERS_CONFIG: Kafka 集群地址
 * - KEY_DESERIALIZER_CLASS_CONFIG: 键反序列化器
 * - VALUE_DESERIALIZER_CLASS_CONFIG: 值反序列化器
 * - GROUP_ID_CONFIG: 消费者组ID
 * - AUTO_OFFSET_RESET_CONFIG: 偏移量重置策略
 * - ENABLE_AUTO_COMMIT_CONFIG: 是否自动提交偏移量
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    /**
     * Kafka 服务器地址配置
     * 从 application.properties 读取，默认 192.168.56.10:9092
     */
    @Value("${spring.kafka.bootstrap-servers:192.168.56.10:9092}")
    private String bootstrapServers;

    /**
     * 消费者组ID配置
     * 从 application.properties 读取，默认 demo-consumer-group
     */
    @Value("${spring.kafka.consumer.group-id:demo-consumer-group}")
    private String groupId;

    /**
     * 演示用 Topic 名称配置
     * 从 application.properties 读取，默认 demo-consumer-topic
     */
    @Value("${demo.topic.name:demo-consumer-topic}")
    private String demoTopic;

    /**
     * 消费者配置参数（自动提交模式）
     * <p>
     * 配置说明：
     * - 基本连接配置：服务器地址、反序列化器
     * - 消费者组配置：组ID、偏移量重置策略
     * - 自动提交配置：启用自动提交、设置提交间隔
     * - 性能配置：拉取超时、最大记录数等
     *
     * @return 消费者配置 Map
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // 1. 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 2. 反序列化器配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 3. 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 4. 偏移量配置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);      // 启用自动提交
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // 自动提交间隔

        // 5. 性能配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);     // 会话超时
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);   // 心跳间隔
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);         // 最大拉取记录数
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);        // 拉取超时

        return props;
    }

    /**
     * 消费者配置参数（手动提交模式）
     * <p>
     * 配置说明：
     * - 基本配置与自动提交模式相同
     * - 关键差异：禁用自动提交，启用手动提交
     * - 适用于需要精确控制偏移量提交的场景
     *
     * @return 手动提交消费者配置 Map
     */
    @Bean
    public Map<String, Object> manualCommitConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // 1. 基本连接配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 2. 反序列化器配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 3. 消费者组配置
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-manual");

        // 4. 偏移量配置（手动提交）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);     // 禁用自动提交

        // 5. 性能配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);         // 手动提交时减少批处理大小
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        return props;
    }

    /**
     * 消费者工厂（自动提交模式）
     * <p>
     * 作用：创建自动提交模式的消费者实例
     * 实现：使用 DefaultKafkaConsumerFactory 和上述配置参数
     *
     * @return 消费者工厂实例
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * 消费者工厂（手动提交模式）
     * <p>
     * 作用：创建手动提交模式的消费者实例
     * 实现：使用 DefaultKafkaConsumerFactory 和手动提交配置参数
     *
     * @return 手动提交消费者工厂实例
     */
    @Bean
    public ConsumerFactory<String, String> manualCommitConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(manualCommitConsumerConfigs());
    }

    /**
     * 监听器容器工厂（自动提交模式）
     * <p>
     * 作用：配置消息监听器容器，支持自动提交偏移量
     * 特点：单线程消费，自动确认，适合简单消费场景
     *
     * @return 监听器容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // 配置监听器属性
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1); // 单线程消费

        return factory;
    }

    /**
     * 监听器容器工厂（手动提交模式）
     * <p>
     * 作用：配置消息监听器容器，支持手动提交偏移量
     * 特点：手动确认，适合需要精确控制偏移量提交的场景
     *
     * @return 手动提交监听器容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualCommitKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(manualCommitConsumerFactory());

        // 配置手动提交属性
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(1); // 单线程消费

        return factory;
    }

    /**
     * 监听器容器工厂（批量消费模式 - 自动提交）
     * <p>
     * 作用：配置批量消息监听器容器，支持批量消费消息
     * 特点：批量处理，提高消费效率，适合高吞吐量场景
     *
     * @return 批量消费监听器容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // 配置批量消费属性
        factory.setBatchListener(true); // 启用批量监听
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);

        return factory;
    }


    /**
     * 监听器容器工厂（批量消费模式 - 手动提交）
     * <p>
     * 作用：配置批量消息监听器容器，支持批量消费消息 + 手动提交偏移量
     * 特点：批量处理 + 手动提交，适合对消息丢失容忍度低的场景
     *
     * @return 手动提交批量消费监听器容器工厂
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualBatchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(manualCommitConsumerFactory()); // 使用手动提交消费者工厂

        // 配置手动提交批量消费属性
        factory.setBatchListener(true); // 启用批量监听
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(1);

        return factory;
    }

    /**
     * 演示用 Topic Bean
     * <p>
     * 作用：自动创建用于演示的 Kafka Topic
     * 配置：3个分区，1个副本（适合单节点开发环境）
     * <p>
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
