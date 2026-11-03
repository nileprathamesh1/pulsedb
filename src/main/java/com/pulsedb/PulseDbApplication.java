package com.pulsedb;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class PulseDbApplication {

    public static void main(String[] args) {
        log.info("Starting PulseDB — Real-time CDC Pipeline");
        log.info("Connecting to PostgreSQL WAL via Debezium embedded engine...");
        SpringApplication.run(PulseDbApplication.class, args);
        log.info("PulseDB is live. Streaming changes from PostgreSQL → Kafka.");
    }
}
