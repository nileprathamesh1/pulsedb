package com.pulsedb.kafka;

import com.pulsedb.metrics.PulseMetrics;
import com.pulsedb.model.ChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChangeEventProducer {

    private final KafkaTemplate<String, ChangeEvent> kafkaTemplate;
    private final PulseMetrics metrics;

    @Value("${pulsedb.kafka.topics.users}")
    private String usersTopic;

    @Value("${pulsedb.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${pulsedb.kafka.topics.products}")
    private String productsTopic;

    @Value("${pulsedb.kafka.topics.dead-letter}")
    private String deadLetterTopic;

    // Built lazily on first use — @Value fields aren't injected at construction time
    private Map<String, String> topicMap;

    public void publish(ChangeEvent event) {
        if (topicMap == null) {
            topicMap = Map.of(
                    "users",    usersTopic,
                    "orders",   ordersTopic,
                    "products", productsTopic
            );
        }

        String topic = topicMap.getOrDefault(event.getTableName(), deadLetterTopic);

        if (deadLetterTopic.equals(topic)) {
            log.warn("No topic configured for table='{}' — routing to dead-letter", event.getTableName());
        }

        // Use table:rowId as the key so all changes to the same row land on the same partition
        String messageKey = event.getTableName() + ":" + event.getRowId();

        CompletableFuture<SendResult<String, ChangeEvent>> future =
                kafkaTemplate.send(topic, messageKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                var metadata = result.getRecordMetadata();
                log.debug("Published {}.{} rowId={} → topic={} partition={} offset={}",
                        event.getTableName(),
                        event.getOperation(),
                        event.getRowId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
                metrics.incrementEventsPublished(event.getTableName(), event.getOperation().name());
            } else {
                // Publish failed — the WAL event will be lost unless the caller retries
                log.error("Failed to publish {}.{} rowId={} to topic={} : {}",
                        event.getTableName(),
                        event.getOperation(),
                        event.getRowId(),
                        topic,
                        ex.getMessage(), ex);
                metrics.incrementErrors("producer");
            }
        });
    }
}
