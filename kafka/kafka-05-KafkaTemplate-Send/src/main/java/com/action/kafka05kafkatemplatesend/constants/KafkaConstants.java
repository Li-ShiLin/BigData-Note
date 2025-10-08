package com.action.kafka05kafkatemplatesend.constants;

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
    public static final String TOPIC_SIMPLE = "kafka-05-simple-topic";
    public static final String TOPIC_KEY_VALUE = "kafka-05-key-value-topic";
    public static final String TOPIC_PARTITION = "kafka-05-partition-topic";
    public static final String TOPIC_TIMESTAMP = "kafka-05-timestamp-topic";
    public static final String TOPIC_PRODUCER_RECORD = "kafka-05-producer-record-topic";
    public static final String TOPIC_MESSAGE = "kafka-05-message-topic";
    public static final String TOPIC_DEFAULT = "kafka-05-default-topic";
    
    /**
     * 消费者组名称
     */
    public static final String CONSUMER_GROUP = "kafka-05-template-send-group";
    
    /**
     * 消息键前缀
     */
    public static final String MESSAGE_KEY_PREFIX = "template-send-";
    
    /**
     * 消息值前缀
     */
    public static final String MESSAGE_VALUE_PREFIX = "KafkaTemplate Send Test - ";
}
