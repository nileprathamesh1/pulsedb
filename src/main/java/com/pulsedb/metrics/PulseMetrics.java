package com.pulsedb.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class PulseMetrics {

    private final MeterRegistry registry;

    // Cache counter/timer instances to avoid repeated registry lookups on the hot path
    private final ConcurrentHashMap<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> timerCache = new ConcurrentHashMap<>();

    public PulseMetrics(MeterRegistry registry) {
        this.registry = registry;
        log.info("PulseMetrics initialized — metrics available at /actuator/prometheus");
    }

    public void incrementEventsCaptures(String table, String operation) {
        getOrCreateCounter("pulsedb_events_captured_total",
                "table", table,
                "operation", operation)
                .increment();
    }

    public void incrementEventsPublished(String table, String operation) {
        getOrCreateCounter("pulsedb_events_published_total",
                "table", table,
                "operation", operation)
                .increment();
    }

    public void incrementEventsConsumed(String consumer, String table) {
        getOrCreateCounter("pulsedb_events_consumed_total",
                "consumer", consumer,
                "table", table)
                .increment();
    }

    public void recordCaptureLatency(String table, long latencyMs) {
        getOrCreateTimer("pulsedb_capture_latency", "table", table)
                .record(Duration.ofMillis(latencyMs));
    }

    public void incrementErrors(String component) {
        getOrCreateCounter("pulsedb_errors_total", "component", component, "dummy", "x")
                .increment();
    }

    public void incrementWebhooksDelivered(String table) {
        getOrCreateCounter("pulsedb_webhooks_delivered_total", "table", table, "dummy", "x")
                .increment();
    }

    public void incrementWebhooksFailed(String table) {
        getOrCreateCounter("pulsedb_webhooks_failed_total", "table", table, "dummy", "x")
                .increment();
    }

    private Counter getOrCreateCounter(String name, String... tags) {
        String cacheKey = name + ":" + String.join(",", tags);
        return counterCache.computeIfAbsent(cacheKey, k -> {
            Counter.Builder builder = Counter.builder(name);
            for (int i = 0; i < tags.length - 1; i += 2) {
                builder.tag(tags[i], tags[i + 1]);
            }
            return builder.register(registry);
        });
    }

    private Timer getOrCreateTimer(String name, String... tags) {
        String cacheKey = name + ":" + String.join(",", tags);
        return timerCache.computeIfAbsent(cacheKey, k -> {
            Timer.Builder builder = Timer.builder(name);
            for (int i = 0; i < tags.length - 1; i += 2) {
                builder.tag(tags[i], tags[i + 1]);
            }
            return builder.register(registry);
        });
    }
}
