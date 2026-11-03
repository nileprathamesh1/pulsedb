package com.pulsedb.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChangeEvent {

    private String tableName;
    private OperationType operation;

    // null for INSERT (no prior state)
    private JsonNode before;

    // null for DELETE (row no longer exists)
    private JsonNode after;

    // Primary key value as string — extracted separately so consumers don't need to parse before/after
    private String rowId;

    private Instant capturedAt;
    private String connectorName;

    // WAL Log Sequence Number — monotonically increasing, useful for gap detection
    private Long lsn;
}
