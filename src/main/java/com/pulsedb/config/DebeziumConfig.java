package com.pulsedb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class DebeziumConfig {

    @Value("${pulsedb.debezium.connector-name}")
    private String connectorName;

    @Value("${pulsedb.debezium.topic-prefix}")
    private String topicPrefix;

    @Value("${pulsedb.debezium.database-host}")
    private String dbHost;

    @Value("${pulsedb.debezium.database-port}")
    private int dbPort;

    @Value("${pulsedb.debezium.database-name}")
    private String dbName;

    @Value("${pulsedb.debezium.database-user}")
    private String dbUser;

    @Value("${pulsedb.debezium.database-password}")
    private String dbPassword;

    @Value("${pulsedb.debezium.table-include-list}")
    private String tableIncludeList;

    @Value("${pulsedb.debezium.offset-storage-file}")
    private String offsetStorageFile;

    @Value("${pulsedb.debezium.slot-name}")
    private String slotName;

    @Bean
    public Properties debeziumProperties() {
        Properties props = new Properties();

        props.setProperty("name", connectorName);
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

        // File-based offset storage — survives restarts so no events are replayed
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetStorageFile);
        props.setProperty("offset.flush.interval.ms", "60000");

        props.setProperty("database.hostname", dbHost);
        props.setProperty("database.port", String.valueOf(dbPort));
        props.setProperty("database.user", dbUser);
        props.setProperty("database.password", dbPassword);
        props.setProperty("database.dbname", dbName);

        // pgoutput is built into PostgreSQL 10+ — no extra extension needed
        props.setProperty("plugin.name", "pgoutput");

        // Replication slot name — PostgreSQL uses this to track our WAL read position
        props.setProperty("slot.name", slotName);

        // Only watch these three tables — excludes pulse_audit_log to avoid infinite loops
        props.setProperty("table.include.list", tableIncludeList);

        // topic.prefix replaces database.server.name in Debezium 2.x
        props.setProperty("topic.prefix", topicPrefix);

        // Take a one-time snapshot of existing rows on first connect, then stream changes
        props.setProperty("snapshot.mode", "initial");

        props.setProperty("replica.identity", "DEFAULT");
        props.setProperty("column.propagate.source.type", ".*");

        // Heartbeat keeps the replication slot alive during quiet periods
        props.setProperty("heartbeat.interval.ms", "10000");

        props.setProperty("decimal.handling.mode", "double");
        props.setProperty("time.precision.mode", "adaptive_time_microseconds");

        return props;
    }
}