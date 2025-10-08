package com.action.kafka02springboot.config;


public class KafkaConstants {
    // Topic名称
    public static final String TOPIC_USERS = "users";
    public static final String TOPIC_ORDERS = "orders";

    // 消费者组
    public static final String CONSUMER_GROUP = "demo-group";

    // Topic配置
    public static final int USERS_TOPIC_PARTITIONS = 3;
    public static final short USERS_TOPIC_REPLICATION_FACTOR = 1;
}