package com.logstreams;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Runs once on startup and streams simulated logs into the "raw-logs" topic.
 *
 * Workload mode is controlled by config (application.yml, or override at run
 * time — see below):
 *
 *   app.mode=steady  app.rate-ms=500   -> one log every 500ms (worker keeps up)
 *   app.mode=spike   app.count=10000   -> blast COUNT logs as fast as possible
 *
 * Override without editing files:
 *   - IntelliJ Run config -> "Program arguments":  --app.mode=spike --app.count=10000
 *   - or environment variables:                    APP_MODE=spike APP_COUNT=10000
 */
@Component
public class LogProducerRunner implements ApplicationRunner {

    private static final String TOPIC = "raw-logs";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConfigurableApplicationContext context;

    @Value("${app.mode:steady}")
    private String mode;

    @Value("${app.rate-ms:500}")
    private long rateMs;

    @Value("${app.count:10000}")
    private int count;

    public LogProducerRunner(KafkaTemplate<String, String> kafkaTemplate,
                             ConfigurableApplicationContext context) {
        this.kafkaTemplate = kafkaTemplate;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if ("spike".equalsIgnoreCase(mode)) {
            runSpike();
            // Spike is a finite job: flush and shut the app down cleanly.
            kafkaTemplate.flush();
            System.exit(SpringApplication.exit(context));
        } else {
            runSteady();
        }
    }

    /** Continuous stream at a fixed pace until you stop the app. */
    private void runSteady() throws InterruptedException {
        System.out.printf("STEADY mode: 1 log every %dms -> topic '%s'. Stop the app to end.%n", rateMs, TOPIC);
        long sent = 0;
        while (true) {
            send(LogSimulator.generateLog());
            if (++sent % 20 == 0) System.out.printf("  sent %d logs%n", sent);
            Thread.sleep(rateMs);
        }
    }

    /** Fire a fixed number of logs as fast as possible. */
    private void runSpike() {
        System.out.printf("SPIKE mode: blasting %d logs -> topic '%s'...%n", count, TOPIC);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            send(LogSimulator.generateLog());
        }
        kafkaTemplate.flush();
        long ms = System.currentTimeMillis() - start;
        System.out.printf("Done: %d logs in %dms (%.0f logs/sec)%n", count, ms, count * 1000.0 / ms);
    }

    private void send(String json) {
        kafkaTemplate.send(TOPIC, LogSimulator.keyFor(json), json);
    }
}
