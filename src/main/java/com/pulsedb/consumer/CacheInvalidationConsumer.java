package com.pulsedb.consumer;

import com.pulsedb.metrics.PulseMetrics;
import com.pulsedb.model.ChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CacheInvalidationConsumer {

    private final StringRedisTemplate redisTemplate;
    private final PulseMetrics metrics;

    @KafkaListener(
            topics = {"${pulsedb.kafka.topics.users}",
                      "${pulsedb.kafka.topics.orders}",
                      "${pulsedb.kafka.topics.products}"},
            groupId = "cache-invalidation-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, ChangeEvent> record, Acknowledgment ack) {
        ChangeEvent event = record.value();

        try {
            String cacheKey = buildCacheKey(event.getTableName(), event.getRowId());

            log.debug("Invalidating cache key={} due to {}:{} on table={}",
                    cacheKey, event.getOperation(), event.getRowId(), event.getTableName());

            Boolean deleted = redisTemplate.delete(cacheKey);

            if (Boolean.TRUE.equals(deleted)) {
                log.info("Cache invalidated: key={} table={} op={}",
                        cacheKey, event.getTableName(), event.getOperation());
            } else {
                log.debug("Cache key not found (already absent): {}", cacheKey);
            }

            metrics.incrementEventsConsumed("cache_invalidation", event.getTableName());
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Failed to invalidate cache for table={} rowId={}: {}",
                    event.getTableName(), event.getRowId(), e.getMessage(), e);
            metrics.incrementErrors("cache_consumer");
            // Don't acknowledge — Kafka will redeliver
        }
    }

    private String buildCacheKey(String tableName, String rowId) {
        // Map plural table names to singular cache key prefixes
        String singular = switch (tableName) {
            case "users"    -> "user";
            case "orders"   -> "order";
            case "products" -> "product";
            default         -> tableName;
        };
        return singular + ":" + rowId;
    }
}
