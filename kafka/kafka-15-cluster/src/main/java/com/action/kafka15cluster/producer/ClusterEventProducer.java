package com.action.kafka15cluster.producer;

import com.action.kafka15cluster.model.User;
import com.action.kafka15cluster.util.JSONUtils;
import jakarta.annotation.Resource;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@Component
public class ClusterEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventProducer.class);

    // 加入 spring-kafka 依赖 + 配置后，KafkaTemplate 会自动装配
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.cluster:clusterTopic}")
    private String clusterTopic;

    /**
     * 发送 N 条用户消息到集群 Topic
     */
    public void sendBatchUsers(int count) {
        for (int i = 0; i < count; i++) {
            User user = User.builder().id(1000 + i).phone("1370000" + i).birthDay(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            String key = "k" + i;
            kafkaTemplate.send(clusterTopic, key, userJSON)
                    .whenComplete((SendResult<String, String> result, Throwable ex) -> {
                        if (ex != null) {
                            log.error("发送失败: key={}, error={}", key, ex.getMessage(), ex);
                            return;
                        }
                        RecordMetadata md = result.getRecordMetadata();
                        log.info("发送成功: topic={}, partition={}, offset={}", md.topic(), md.partition(), md.offset());
                    });
        }
    }
}


