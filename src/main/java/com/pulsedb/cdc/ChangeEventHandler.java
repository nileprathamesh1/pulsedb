package com.pulsedb.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsedb.kafka.ChangeEventProducer;
import com.pulsedb.metrics.PulseMetrics;
import com.pulsedb.model.ChangeEvent;
import com.pulsedb.model.OperationType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChangeEventHandler {

    private final ChangeEventProducer producer;
    private final PulseMetrics metrics;
    private final ObjectMapper objectMapper;

    public void handleChangeEvent(io.debezium.engine.ChangeEvent<String, String> record) {
        // Heartbeat records have null values — skip them
        if (record.value() == null) {
            log.debug("Skipping heartbeat event for topic: {}", record.destination());
            return;
        }

        long startTime = System.currentTimeMillis();

        try {
            JsonNode payload = objectMapper.readTree(record.value());

            // Heartbeat events have a value but no op field — skip them silently
            String op = payload.path("payload").path("op").asText("");
            if (op.isEmpty()) {
                log.debug("Skipping non-data event for topic: {}", record.destination());
                return;
            }

            OperationType operation = parseOperation(payload);
            String tableName = extractTableName(payload);
            JsonNode before = payload.path("payload").path("before");
            JsonNode after  = payload.path("payload").path("after");

            JsonNode beforeNode = before.isMissingNode() || before.isNull() ? null : before;
            JsonNode afterNode  = after.isMissingNode()  || after.isNull()  ? null : after;

            String rowId = extractRowId(afterNode != null ? afterNode : beforeNode);
            Long lsn = extractLsn(payload);

            // READ events are initial snapshot rows — not actual changes, skip them
            if (operation == OperationType.READ) {
                log.debug("Skipping READ event for table={} rowId={}", tableName, rowId);
                return;
            }

            com.pulsedb.model.ChangeEvent changeEvent = com.pulsedb.model.ChangeEvent.builder()
                    .tableName(tableName)
                    .operation(operation)
                    .before(beforeNode)
                    .after(afterNode)
                    .rowId(rowId)
                    .capturedAt(Instant.now())
                    .connectorName("pulsedb-connector")
                    .lsn(lsn)
                    .build();

            log.info("Captured {} on table={} rowId={} lsn={}", operation, tableName, rowId, lsn);

            producer.publish(changeEvent);

            metrics.recordCaptureLatency(tableName, System.currentTimeMillis() - startTime);
            metrics.incrementEventsCaptures(tableName, operation.name());

        } catch (Exception e) {
            log.error("Failed to handle change event: {}", e.getMessage(), e);
            metrics.incrementErrors("handler");
        }
    }

    // Debezium uses single-letter codes: c=insert, u=update, d=delete, r=read/snapshot
    private OperationType parseOperation(JsonNode payload) {
        String op = payload.path("payload").path("op").asText("");
        return switch (op) {
            case "c" -> OperationType.INSERT;
            case "u" -> OperationType.UPDATE;
            case "d" -> OperationType.DELETE;
            case "r" -> OperationType.READ;
            default  -> {
                log.warn("Unknown operation code: {}", op);
                yield OperationType.UNKNOWN;
            }
        };
    }

    private String extractTableName(JsonNode payload) {
        return payload.path("payload").path("source").path("table").asText("unknown");
    }

    // Assumes all tracked tables have a column named "id" as the primary key
    private String extractRowId(JsonNode rowData) {
        if (rowData == null || rowData.isMissingNode()) return "unknown";
        JsonNode idNode = rowData.path("id");
        return idNode.isMissingNode() ? "unknown" : idNode.asText();
    }

    private Long extractLsn(JsonNode payload) {
        JsonNode lsnNode = payload.path("payload").path("source").path("lsn");
        return lsnNode.isMissingNode() ? null : lsnNode.asLong();
    }
}