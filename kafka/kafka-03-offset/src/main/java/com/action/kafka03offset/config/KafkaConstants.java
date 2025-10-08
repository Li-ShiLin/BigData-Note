package com.action.kafka03offset.config;

/**
 * Kafka常量定义类
 * 统一管理Topic名称、消费者组、分区和副本等配置常量
 */
public class KafkaConstants {
    
    /** 演示Topic名称 */
    public static final String TOPIC_DEMO = "offset-demo-topic";

    /** 最新偏移量策略消费者组 */
    public static final String CONSUMER_GROUP_LATEST = "demo-group-latest";
    /** 最早偏移量策略消费者组 */
    public static final String CONSUMER_GROUP_EARLIEST = "demo-group-earliest";
    /** 手动偏移量控制消费者组 */
    public static final String CONSUMER_GROUP_MANUAL = "demo-group-manual";

    /** Topic分区数量 */
    public static final int DEMO_TOPIC_PARTITIONS = 2;
    /** Topic副本因子 */
    public static final short DEMO_TOPIC_REPLICATION_FACTOR = 1;
}


