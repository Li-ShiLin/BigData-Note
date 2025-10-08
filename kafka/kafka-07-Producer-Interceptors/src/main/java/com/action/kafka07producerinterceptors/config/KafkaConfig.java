package com.action.kafka07producerinterceptors.config;

import com.action.kafka07producerinterceptors.interceptor.MetricsProducerInterceptor;
import com.action.kafka07producerinterceptors.interceptor.ModifyRecordInterceptor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 生产者配置类
 * 
 * 功能说明：
 * 1) 配置生产者基本参数：序列化器、服务器地址等
 * 2) 注册自定义拦截器：按执行顺序配置拦截器链
 * 3) 创建生产者工厂和 KafkaTemplate Bean
 * 4) 自动创建演示用的 Topic
 * 
 * 关键配置说明：
 * - BOOTSTRAP_SERVERS_CONFIG: Kafka 集群地址
 * - KEY_SERIALIZER_CLASS_CONFIG: 键序列化器
 * - VALUE_SERIALIZER_CLASS_CONFIG: 值序列化器
 * - INTERCEPTOR_CLASSES_CONFIG: 拦截器类列表（按顺序执行）
 */
@Configuration
public class KafkaConfig {

    /**
     * Kafka 服务器地址配置
     * 从 application.properties 读取，默认 localhost:9097
     */
    @Value("${spring.kafka.bootstrap-servers:localhost:9097}")
    private String bootstrapServers;

    /**
     * 演示用 Topic 名称配置
     * 从 application.properties 读取，默认 demo-interceptor-topic
     */
    @Value("${demo.topic.name:demo-interceptor-topic}")
    private String demoTopic;

    /**
     * 生产者配置参数
     * 
     * 配置说明：
     * - 基本连接配置：服务器地址、序列化器
     * - 拦截器配置：按顺序注册自定义拦截器
     * - 其他配置：可根据需要添加重试、超时等参数
     * 
     * @return 生产者配置 Map
     */
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        
        // 1. 基本连接配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // 2. 序列化器配置
        // 键和值都使用字符串序列化器，适合演示场景
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // 3. 拦截器配置 - 核心配置
        // 拦截器按列表顺序依次执行 onSend() 方法
        // 如果某个拦截器抛出异常，后续拦截器不会执行
        List<Class<?>> interceptors = new ArrayList<>();
        interceptors.add(ModifyRecordInterceptor.class);    // 第一个：修改消息内容
        interceptors.add(MetricsProducerInterceptor.class); // 第二个：统计发送指标
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        
        // 4. 可选配置（可根据需要添加）
        // props.put(ProducerConfig.RETRIES_CONFIG, 3);                    // 重试次数
        // props.put(ProducerConfig.ACKS_CONFIG, "all");                   // 确认机制
        // props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);     // 请求超时
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);             // 批处理大小
        // props.put(ProducerConfig.LINGER_MS_CONFIG, 5);                  // 等待时间
        
        return props;
    }

    /**
     * 生产者工厂 Bean
     * 
     * 作用：创建 Kafka 生产者实例
     * 实现：使用 DefaultKafkaProducerFactory 和上述配置参数
     * 
     * @return 生产者工厂实例
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * KafkaTemplate Bean
     * 
     * 作用：Spring 提供的 Kafka 操作模板，简化消息发送
     * 特点：线程安全，支持异步发送，集成 Spring 事务管理
     * 
     * @return KafkaTemplate 实例
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
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


