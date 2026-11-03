package com.pulsedb.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsedb.metrics.PulseMetrics;
import com.pulsedb.model.ChangeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class AuditLogConsumer {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final PulseMetrics metrics;

    private static final String INSERT_SQL =
            "INSERT INTO pulse_audit_log (source_table, operation, row_id, payload, captured_at) " +
            "VALUES (?, ?, ?, ?::jsonb, ?)";

    @KafkaListener(
            topics = {"${pulsedb.kafka.topics.users}",
                      "${pulsedb.kafka.topics.orders}",
                      "${pulsedb.kafka.topics.products}"},
            groupId = "audit-log-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, ChangeEvent> record, Acknowledgment ack) {
        ChangeEvent event = record.value();

        try {
            // Store before/after as JSONB — gives full visibility into what changed
            String payloadJson = serializePayload(event);

            jdbcTemplate.update(INSERT_SQL,
                    event.getTableName(),
                    event.getOperation().name(),
                    event.getRowId(),
                    payloadJson,
                    Timestamp.from(event.getCapturedAt() != null ? event.getCapturedAt() : Instant.now())
            );

            log.info("Audit log written: table={} op={} rowId={}",
                    event.getTableName(), event.getOperation(), event.getRowId());

            metrics.incrementEventsConsumed("audit_log", event.getTableName());
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Failed to write audit log for table={} rowId={}: {}",
                    event.getTableName(), event.getRowId(), e.getMessage(), e);
            metrics.incrementErrors("audit_consumer");
            // Don't ack — let Kafka retry
        }
    }

    private String serializePayload(ChangeEvent event) throws JsonProcessingException {
        // Only store before/after in the JSONB column — metadata is in dedicated columns
        var payload = objectMapper.createObjectNode();

        if (event.getBefore() != null) {
            payload.set("before", event.getBefore());
        }
        if (event.getAfter() != null) {
            payload.set("after", event.getAfter());
        }
        if (event.getLsn() != null) {
            payload.put("lsn", event.getLsn());
        }

        return objectMapper.writeValueAsString(payload);
    }
}
