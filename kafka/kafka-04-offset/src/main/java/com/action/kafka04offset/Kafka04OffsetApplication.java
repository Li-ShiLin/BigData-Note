package com.action.kafka04offset;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka 偏移量策略演示应用
 *
 * @author action
 * @since 2024
 */
@Slf4j
@SpringBootApplication
public class Kafka04OffsetApplication {
    public static void main(String[] args) {
        SpringApplication.run(Kafka04OffsetApplication.class, args);
    }

}
