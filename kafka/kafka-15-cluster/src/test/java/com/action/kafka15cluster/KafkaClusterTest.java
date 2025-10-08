package com.action.kafka15cluster;

import com.action.kafka15cluster.producer.ClusterEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 3, topics = "clusterTopic")
class KafkaClusterTest {

    @Autowired
    private ClusterEventProducer producer;

    @Test
    void testSendMessage() {
        // 发送测试消息
        producer.sendBatchUsers(5);
        
        // 等待一段时间让消费者处理消息
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
