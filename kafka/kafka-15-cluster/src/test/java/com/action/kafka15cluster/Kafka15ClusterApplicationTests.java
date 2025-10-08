package com.action.kafka15cluster;

import com.action.kafka15cluster.producer.ClusterEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Kafka15ClusterApplicationTests {

    @Autowired
    private ClusterEventProducer producer;

    @Test
    void sendBatchForCluster() {
        producer.sendBatchUsers(5);
    }
}


