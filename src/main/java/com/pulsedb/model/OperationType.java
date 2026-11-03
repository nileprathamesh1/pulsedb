package com.pulsedb.model;

public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    READ,    // initial snapshot rows — skipped by the handler
    UNKNOWN  // defensive fallback for anything Debezium adds later
}
