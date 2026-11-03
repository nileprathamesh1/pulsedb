package com.pulsedb.service;

import com.pulsedb.model.OperationType;
import com.pulsedb.model.WebhookSubscription;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WebhookSubscriptionService {

    // In-memory store — a production version would persist these to a database
    private final Map<String, WebhookSubscription> subscriptions = new ConcurrentHashMap<>();

    public WebhookSubscription register(String tableName, String targetUrl,
                                         List<OperationType> operations) {
        String id = UUID.randomUUID().toString();

        // Default to all operations if none specified
        List<OperationType> ops = (operations == null || operations.isEmpty())
                ? List.of(OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE)
                : operations;

        WebhookSubscription subscription = WebhookSubscription.builder()
                .id(id)
                .tableName(tableName)
                .targetUrl(targetUrl)
                .operations(ops)
                .createdAt(Instant.now())
                .active(true)
                .build();

        subscriptions.put(id, subscription);

        log.info("Webhook registered: id={} table={} url={} ops={}",
                id, tableName, targetUrl, ops);

        return subscription;
    }

    public List<WebhookSubscription> getSubscriptions(String tableName, OperationType operation) {
        return subscriptions.values().stream()
                .filter(s -> s.isActive())
                .filter(s -> s.getTableName().equals(tableName))
                .filter(s -> s.getOperations().contains(operation))
                .collect(Collectors.toList());
    }

    public List<WebhookSubscription> getAllSubscriptions() {
        return List.copyOf(subscriptions.values());
    }

    public boolean deactivate(String id) {
        WebhookSubscription sub = subscriptions.get(id);
        if (sub == null) {
            return false;
        }
        sub.setActive(false);
        log.info("Webhook deactivated: id={} table={}", id, sub.getTableName());
        return true;
    }

    public boolean delete(String id) {
        WebhookSubscription removed = subscriptions.remove(id);
        if (removed != null) {
            log.info("Webhook deleted: id={} table={}", id, removed.getTableName());
        }
        return removed != null;
    }
}
