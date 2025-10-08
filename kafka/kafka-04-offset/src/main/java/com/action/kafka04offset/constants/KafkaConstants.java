package com.action.kafka04offset.constants;

/**
 * Kafka 常量定义
 * 
 * @author action
 * @since 2024
 */
public class KafkaConstants {
    
    /**
     * 主题名称
     */
    public static final String TEST_TOPIC = "kafka-04-offset-test";
    public static final String EARLIEST_TOPIC = "kafka-04-offset-earliest";
    public static final String LATEST_TOPIC = "kafka-04-offset-latest";
    public static final String NONE_TOPIC = "kafka-04-offset-none";
    
    /**
     * 消费者组名称
     */
    public static final String EARLIEST_GROUP = "kafka-04-earliest-group";
    public static final String LATEST_GROUP = "kafka-04-latest-group";
    public static final String NONE_GROUP = "kafka-04-none-group";
    
    /**
     * 偏移量策略
     */
    public static final String OFFSET_EARLIEST = "earliest";
    public static final String OFFSET_LATEST = "latest";
    public static final String OFFSET_NONE = "none";
    
    /**
     * 消息键前缀
     */
    public static final String MESSAGE_KEY_PREFIX = "offset-test-";
    
    /**
     * 消息值前缀
     */
    public static final String MESSAGE_VALUE_PREFIX = "Offset Test Message - ";
}
