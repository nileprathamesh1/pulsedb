package com.pulsedb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsedb.metrics.PulseMetrics;
import com.pulsedb.model.ChangeEvent;
import com.pulsedb.service.WebhookSubscriptionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebhookDeliveryConsumer {

    private final WebhookSubscriptionService subscriptionService;
    private final ObjectMapper objectMapper;
    private final PulseMetrics metrics;

    @Value("${pulsedb.webhook.retry-attempts:3}")
    private int retryAttempts;

    @Value("${pulsedb.webhook.retry-backoff-ms:2000}")
    private long retryBackoffMs;

    @Value("${pulsedb.webhook.timeout-ms:5000}")
    private int timeoutMs;

    // HttpClient is thread-safe and expensive to create — reuse it
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @KafkaListener(
            topics = {"${pulsedb.kafka.topics.users}",
                      "${pulsedb.kafka.topics.orders}",
                      "${pulsedb.kafka.topics.products}"},
            groupId = "webhook-delivery-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, ChangeEvent> record, Acknowledgment ack) {
        ChangeEvent event = record.value();

        try {
            var subscriptions = subscriptionService.getSubscriptions(
                    event.getTableName(), event.getOperation());

            if (subscriptions.isEmpty()) {
                log.debug("No webhook subscribers for table={} op={}",
                        event.getTableName(), event.getOperation());
                ack.acknowledge();
                return;
            }

            // Serialize once, deliver to all matching subscribers
            String payload = objectMapper.writeValueAsString(event);

            for (var subscription : subscriptions) {
                deliverWithRetry(subscription.getTargetUrl(), payload, event);
            }

            metrics.incrementEventsConsumed("webhook_delivery", event.getTableName());
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Webhook delivery consumer error for table={} rowId={}: {}",
                    event.getTableName(), event.getRowId(), e.getMessage(), e);
            metrics.incrementErrors("webhook_consumer");
            // Don't ack — retry the whole batch
        }
    }

    private void deliverWithRetry(String targetUrl, String payload, ChangeEvent event) {
        int attempt = 0;

        while (attempt < retryAttempts) {
            attempt++;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .timeout(Duration.ofMillis(timeoutMs))
                        .header("Content-Type", "application/json")
                        .header("X-PulseDB-Table", event.getTableName())
                        .header("X-PulseDB-Operation", event.getOperation().name())
                        .header("X-PulseDB-Row-Id", event.getRowId())
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();

                HttpResponse<String> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofString());

                int statusCode = response.statusCode();

                if (statusCode >= 200 && statusCode < 300) {
                    log.info("Webhook delivered to {} — HTTP {} (attempt {}/{})",
                            targetUrl, statusCode, attempt, retryAttempts);
                    metrics.incrementWebhooksDelivered(event.getTableName());
                    return; // Success — stop retrying
                } else {
                    log.warn("Webhook returned HTTP {} from {} (attempt {}/{})",
                            statusCode, targetUrl, attempt, retryAttempts);
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error("Webhook delivery interrupted for URL={}", targetUrl);
                break;
            } catch (Exception e) {
                log.warn("Webhook delivery attempt {}/{} failed for URL={}: {}",
                        attempt, retryAttempts, targetUrl, e.getMessage());
            }

            if (attempt < retryAttempts) {
                try {
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.error("Webhook delivery FAILED after {} attempts for URL={} table={} rowId={}",
                retryAttempts, targetUrl, event.getTableName(), event.getRowId());
        metrics.incrementWebhooksFailed(event.getTableName());
    }
}
