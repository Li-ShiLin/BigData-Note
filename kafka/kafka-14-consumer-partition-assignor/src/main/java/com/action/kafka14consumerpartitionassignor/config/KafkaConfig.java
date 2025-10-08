package com.action.kafka14consumerpartitionassignor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 配置类 - 分区分配策略演示
 * <p>
 * 功能说明：
 * 1) 创建演示 Topic
 * 2) 基于不同分区分配策略创建四套 ListenerContainerFactory
 * 3) 提供 KafkaTemplate 方便通过 REST 发送测试消息
 * <p>
 * 关键参数：
 * - ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG：分区分配策略
 * RangeAssignor / RoundRobinAssignor / StickyAssignor / CooperativeStickyAssignor
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // =================== Topic 创建 ===================
    @Bean
    public NewTopic rangeAssignorTopic() {
        return TopicBuilder.name("range-assignor-topic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic roundRobinAssignorTopic() {
        return TopicBuilder.name("roundrobin-assignor-topic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic stickyAssignorTopic() {
        return TopicBuilder.name("sticky-assignor-topic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic coopStickyAssignorTopic() {
        return TopicBuilder.name("coop-sticky-assignor-topic")
                .partitions(10)
                .replicas(1)
                .build();
    }

    // =================== Producer ===================
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // =================== Common Consumer Base ===================
    private Map<String, Object> baseConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> buildFactory(Map<String, Object> props) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);
        factory.setConsumerFactory(cf);
//        // 统一设置并发为3，便于观察同组内多个实例的分配
//        factory.setConcurrency(3);
        // 并发数通过@KafkaListener注解的concurrency属性指定
        factory.getContainerProperties().setPollTimeout(2000);
        // 设置手动确认模式，支持Acknowledgment参数
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    // =================== Range ===================
    @Value("${kafka.group.range:range-assignor-group}")
    private String rangeGroup;

    @Bean("rangeKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> rangeFactory() {
        Map<String, Object> props = baseConsumerProps(rangeGroup);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(RangeAssignor.class.getName()));
        return buildFactory(props);
    }

    // =================== RoundRobin ===================
    @Value("${kafka.group.rr:roundrobin-assignor-group}")
    private String rrGroup;

    @Bean("rrKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> rrFactory() {
        Map<String, Object> props = baseConsumerProps(rrGroup);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(RoundRobinAssignor.class.getName()));
        return buildFactory(props);
    }

    // =================== Sticky ===================
    @Value("${kafka.group.sticky:sticky-assignor-group}")
    private String stickyGroup;

    @Bean("stickyKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> stickyFactory() {
        Map<String, Object> props = baseConsumerProps(stickyGroup);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(StickyAssignor.class.getName()));
        return buildFactory(props);
    }

    // =================== Cooperative Sticky ===================
    @Value("${kafka.group.coop:coop-sticky-assignor-group}")
    private String coopGroup;

    @Bean("coopStickyKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> coopStickyFactory() {
        Map<String, Object> props = baseConsumerProps(coopGroup);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class.getName()));
        return buildFactory(props);
    }
}


