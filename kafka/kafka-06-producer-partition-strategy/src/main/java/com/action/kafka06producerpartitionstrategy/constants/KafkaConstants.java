package com.action.kafka06producerpartitionstrategy.constants;

/**
 * Kafka 常量类
 */
public class KafkaConstants {

    /**
     * 主题名称
     */
    public static final String TOPIC_DEFAULT_PARTITION = "topic-default-partition";
    public static final String TOPIC_CUSTOM_PARTITION = "topic-custom-partition";
    public static final String TOPIC_KEY_BASED_PARTITION = "topic-key-based-partition";
    public static final String TOPIC_ROUND_ROBIN_PARTITION = "topic-round-robin-partition";

    /**
     * 消费者组
     */
    public static final String CONSUMER_GROUP = "kafka-06-partition-strategy-group";

    /**
     * 分区数量
     */
    public static final int PARTITION_COUNT = 3;
}
