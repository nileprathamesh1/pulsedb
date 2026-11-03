package com.pulsedb.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WebhookSubscription {

    private String id;
    private String tableName;
    private String targetUrl;
    private List<OperationType> operations;

    // for HMAC-SHA256 signing — not implemented yet
    private String secret;

    private Instant createdAt;

    @Builder.Default
    private boolean active = true;
}
