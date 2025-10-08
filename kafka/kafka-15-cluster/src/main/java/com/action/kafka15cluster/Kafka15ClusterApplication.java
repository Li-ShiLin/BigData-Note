package com.action.kafka15cluster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class Kafka15ClusterApplication {

    public static void main(String[] args) {
        SpringApplication.run(Kafka15ClusterApplication.class, args);
    }

}
