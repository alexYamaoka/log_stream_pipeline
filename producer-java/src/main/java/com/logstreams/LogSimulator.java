package com.logstreams;

import java.util.Random;
import java.util.UUID;

/**
 * Generates realistic, structured log lines as JSON strings.
 *
 * Most logs are INFO/DEBUG (the boring majority); a smaller slice are
 * WARN/ERROR, so the stream looks like a real service — mostly healthy
 * with occasional problems worth searching for later.
 */
public final class LogSimulator {

    private static final String[] SERVICES = {
        "auth-service", "payment-api", "inventory-core", "user-profile", "api-gateway"
    };

    private static final String[] HOSTS = {
        "node-1", "node-2", "node-3", "node-4"
    };

    private static final String[] INFO_MESSAGES = {
        "Request handled in %dms",
        "User session refreshed successfully",
        "Cache hit for key user:%d",
        "Health check passed",
        "Processed batch of %d records"
    };

    private static final String[] WARN_MESSAGES = {
        "Slow query detected: %dms on table 'orders'",
        "Connection pool at 85%% capacity",
        "Retrying downstream call (attempt %d)",
        "Deprecated API endpoint used by client"
    };

    private static final String[] ERROR_MESSAGES = {
        "Connection timeout to database cluster db-node-%d after 5000ms",
        "NullPointerException in user session validation",
        "Database deadlock detected on table 'orders'. Transaction rolled back",
        "Out of memory error: Java heap space during batch processing",
        "API Gateway failed to route request due to downstream network timeout"
    };

    private static final Random RANDOM = new Random();

    private LogSimulator() {}

    /** Produces one log event as a JSON string. */
    public static String generateLog() {
        String service = pick(SERVICES);
        String host = pick(HOSTS);
        long timestamp = System.currentTimeMillis();
        String id = UUID.randomUUID().toString();

        // Weighted level distribution: ~70% INFO, ~20% WARN, ~10% ERROR.
        int roll = RANDOM.nextInt(100);
        String level;
        String message;
        if (roll < 70) {
            level = "INFO";
            message = String.format(pick(INFO_MESSAGES), RANDOM.nextInt(500));
        } else if (roll < 90) {
            level = "WARN";
            message = String.format(pick(WARN_MESSAGES), RANDOM.nextInt(500));
        } else {
            level = "ERROR";
            message = String.format(pick(ERROR_MESSAGES), 1 + RANDOM.nextInt(4));
        }

        // Hand-built JSON keeps the producer dependency-free and fast.
        return String.format(
            "{\"id\":\"%s\",\"timestamp\":%d,\"service\":\"%s\",\"host\":\"%s\",\"level\":\"%s\",\"message\":\"%s\"}",
            id, timestamp, service, host, level, escape(message)
        );
    }

    /** The id doubles as the Kafka message key (for partition routing). */
    public static String keyFor(String json) {
        int start = json.indexOf("\"id\":\"") + 6;
        int end = json.indexOf('"', start);
        return json.substring(start, end);
    }

    private static String pick(String[] arr) {
        return arr[RANDOM.nextInt(arr.length)];
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
