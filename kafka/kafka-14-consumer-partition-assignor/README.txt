消息消费的分区策略
Kafka消费消息时的分区策略：是指Kafka主题topic中哪些分区应该由哪些消费者来消费；

Kafka有多种分区分配策略，默认的分区分配策略是RangeAssignor，除了RangeAssignor策略外，Kafka还有其他分区分配策略：
RoundRobinAssignor、
StickyAssignor
CooperativeStickyAssignor，
这些策略各有特点，可以根据实际的应用场景和需求来选择适合的分区分配策略；


消息消息的分区策略
Kafka默认的消费分区分配策略：RangeAssignor；假设如下：
一个主题myTopic有10个分区；（p0 - p9）
一个消费者组内有3个消费者：consumer1、consumer2、consumer3；
RangeAssignor消费分区策略：
1、计算每个消费者应得的分区数：分区总数（10）/  消费者数量（3）= 3 ... 余1；
每个消费者理论上应该得到3个分区，但由于有余数1，所以前1个消费者会多得到一个分区；
consumer1（作为第一个消费者）将得到 3 + 1 = 4 个分区；
consumer2 和 consumer3 将各得到 3 个分区；
2、具体分配：分区编号从0到9，按照编号顺序为消费者分配分区：
    consumer1 将分配得到分区 0、1、2、3；
    consumer2 将分配得到分区 4、5、6；
    consumer3 将分配得到分区 7、8、9；


RangeAssignor策略是根据消费者组内的消费者数量和主题的分区数量，来均匀地为每个消费者分配分区。

消息消息的分区策略
继续以前面的例子数据，采用RoundRobinAssignor策略进行测试，得到的结果如下：
C1： 0,    3， 6， 9
C2： 1， 4， 7
C3： 2， 5， 8


消息消息的分区策略
StickyAssignor消费分区策略：
尽可能保持消费者与分区之间的分配关系不变，即使消费组的消费者成员发生变化，减少不必要的分区重分配；
尽量保持现有的分区分配不变，仅对新加入的消费者或离开的消费者进行分区调整。这样，大多数消费者可以继续消费它们之前消费的分区，只有少数消费者需要处理额外的分区；所以叫“粘性”分配；
CooperativeStickyAssignor消费分区策略：
与 StickyAssignor 类似，但增加了对协作式重新平衡的支持，即消费者可以在它离开消费者组之前通知协调器，以便协调器可以预先计划分区迁移，而不是在消费者突然离开时立即进行分区重分配；



package com.bjpowernode.config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.key-deserializer}")
    private String keyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer}")
    private String valueDeserializer;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    /**
     * 消费者相关配置
     *
     * @return
     */
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        //指定使用轮询的消息消费分区器
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        return props;
    }

    /**
     * 消费者创建工厂
     *
     * @return
     */
    @Bean
    public ConsumerFactory<String, String> ourConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(producerConfigs());
    }

    /**
     * 创建监听器容器工厂
     *
     * @param ourConsumerFactory
     * @return
     */
    @Bean
    public KafkaListenerContainerFactory<?> ourKafkaListenerContainerFactory(ConsumerFactory<String, String> ourConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(ourConsumerFactory);
        return listenerContainerFactory;
    }

    @Bean
    public NewTopic newTopic() {
        return new NewTopic("myTopic", 10,  (short)1);
    }
}




package com.bjpowernode.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {

    @KafkaListener(topics = {"myTopic"}, groupId = "myGroup", concurrency = "3", containerFactory = "ourKafkaListenerContainerFactory")
    public void onEvent(ConsumerRecord<String, String> record) {
        System.out.println(Thread.currentThread().getId() + " -->消息消费，records = " + record);
    }
}



package com.bjpowernode.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {

    private int id;

    private String phone;

    private Date birthDay;

}


package com.bjpowernode.producer;

import com.bjpowernode.model.User;
import com.bjpowernode.util.JSONUtils;
import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class EventProducer {

    //加入了spring-kafka依赖 + .yml配置信息，springboot自动配置好了kafka，自动装配好了KafkaTemplate这个Bean
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvent() {
        for (int i = 0; i < 100; i++) {
            User user = User.builder().id(1028+i).phone("1370909090"+i).birthDay(new Date()).build();
            String userJSON = JSONUtils.toJSON(user);
            kafkaTemplate.send("myTopic", "k" + i, userJSON);
        }
    }
}




package com.bjpowernode.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONUtils {

    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();

    public static String toJSON(Object object) {
        try {
            return OBJECTMAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toBean(String json, Class<T> clazz) {
        try {
            return OBJECTMAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}





package com.bjpowernode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.Map;

@SpringBootApplication
public class KafkaBaseApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaBaseApplication.class, args);

        Map<String, ConsumerFactory> beansOfType = context.getBeansOfType(ConsumerFactory.class);
        beansOfType.forEach((k, v) -> {
            System.out.println(k + " -- " + v); //DefaultKafkaConsumerFactory
        });

        System.out.println("----------------------------------------");

        Map<String, KafkaListenerContainerFactory> beansOfType2 = context.getBeansOfType(KafkaListenerContainerFactory.class);
        beansOfType2.forEach((k, v) -> {
            System.out.println(k + " -- " + v); // ConcurrentKafkaListenerContainerFactory
        });
    }
}






package com.bjpowernode;

public class Test {

    public static void main(String[] args) {
        int partition = Math.abs("myGroupx2".hashCode()) % 50;
        System.out.println(partition);
    }
}



spring:
  application:
    #应用名称
    name: spring-boot-06-kafka-base

  #kafka连接地址（ip+port）
  kafka:
    bootstrap-servers: 192.168.11.128:9092
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest



























