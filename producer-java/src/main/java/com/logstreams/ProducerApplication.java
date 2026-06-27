package com.logstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point.
 *
 * In IntelliJ: right-click this class -> Run 'ProducerApplication'.
 * (Or use the green ▶ gutter arrow next to main.)
 *
 * Spring Boot auto-configures the KafkaTemplate from application.yml; the
 * actual log-streaming logic lives in {@link LogProducerRunner}.
 */
@SpringBootApplication
public class ProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
