package com.pulsedb.cdc;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Component
@RequiredArgsConstructor
public class DebeziumEngineManager {

    private final Properties debeziumProperties;
    private final ChangeEventHandler changeEventHandler;

    // Debezium's run() is blocking, so it gets its own thread
    private final Executor debeziumExecutor = Executors.newSingleThreadExecutor(
            r -> {
                Thread t = new Thread(r, "debezium-engine-thread");
                t.setDaemon(true); // Don't prevent JVM shutdown
                return t;
            }
    );

    private DebeziumEngine<ChangeEvent<String, String>> engine;

    @PostConstruct
    public void startEngine() {
        log.info("Starting Debezium engine — connecting to PostgreSQL WAL...");

        engine = DebeziumEngine
                .create(Json.class)
                .using(debeziumProperties)
                .notifying(changeEventHandler::handleChangeEvent)
                .using((success, message, error) -> {
                    if (success) {
                        log.info("Debezium engine stopped cleanly: {}", message);
                    } else {
                        log.error("Debezium engine stopped with error: {}", message, error);
                    }
                })
                .build();

        // engine.run() is blocking — it only returns when stopped
        debeziumExecutor.execute(engine);

        log.info("Debezium engine submitted to executor — WAL streaming will begin shortly");
    }

    @PreDestroy
    public void stopEngine() {
        if (engine != null) {
            log.info("Stopping Debezium engine...");
            try {
                engine.close();
                log.info("Debezium engine stopped successfully");
            } catch (IOException e) {
                log.error("Error stopping Debezium engine", e);
            }
        }
    }
}