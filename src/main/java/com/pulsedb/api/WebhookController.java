package com.pulsedb.api;

import com.pulsedb.model.OperationType;
import com.pulsedb.model.WebhookSubscription;
import com.pulsedb.service.WebhookSubscriptionService;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/webhooks")
@RequiredArgsConstructor
public class WebhookController {

    private final WebhookSubscriptionService subscriptionService;

    @PostMapping
    public ResponseEntity<WebhookSubscription> registerWebhook(
            @Validated @RequestBody WebhookRegistrationRequest request) {

        log.info("Registering webhook: table={} url={} ops={}",
                request.getTableName(), request.getTargetUrl(), request.getOperations());

        WebhookSubscription subscription = subscriptionService.register(
                request.getTableName(),
                request.getTargetUrl(),
                request.getOperations()
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(subscription);
    }

    @GetMapping
    public ResponseEntity<List<WebhookSubscription>> listWebhooks() {
        List<WebhookSubscription> all = subscriptionService.getAllSubscriptions();
        return ResponseEntity.ok(all);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteWebhook(@PathVariable String id) {
        boolean deleted = subscriptionService.delete(id);
        if (deleted) {
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/{id}/pause")
    public ResponseEntity<String> pauseWebhook(@PathVariable String id) {
        boolean paused = subscriptionService.deactivate(id);
        if (paused) {
            return ResponseEntity.ok("Webhook " + id + " paused successfully");
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @Data
    public static class WebhookRegistrationRequest {

        @NotBlank(message = "tableName is required")
        private String tableName;

        @NotBlank(message = "targetUrl is required")
        private String targetUrl;

        // Optional — defaults to all operations (INSERT, UPDATE, DELETE) if not set
        private List<OperationType> operations;
    }
}
