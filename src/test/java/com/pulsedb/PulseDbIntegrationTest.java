package com.pulsedb;

import com.pulsedb.model.ChangeEvent;
import com.pulsedb.model.OperationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// Full end-to-end test: real PostgreSQL via Testcontainers, in-process Kafka via EmbeddedKafka
// Startup takes 20-30s — Debezium needs time for its initial snapshot before streaming kicks in
@SpringBootTest
@Testcontainers
@EmbeddedKafka(
        partitions = 1,
        topics = {"pulsedb.users", "pulsedb.orders", "pulsedb.products"}
)
class PulseDbIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("pulsedb")
            .withUsername("pulsedb_user")
            .withPassword("pulsedb_pass")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=5",
                    "-c", "max_wal_senders=5"
            )
            .withInitScript("docker/init.sql");

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ConsumerFactory<String, ChangeEvent> consumerFactory;

    // Wire the Spring context to the Testcontainer's dynamic URL — this runs before the app starts
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("pulsedb.debezium.database-host", postgres::getHost);
        registry.add("pulsedb.debezium.database-port", () -> postgres.getMappedPort(5432).toString());
        registry.add("pulsedb.debezium.database-name", postgres::getDatabaseName);
        registry.add("pulsedb.debezium.database-user", postgres::getUsername);
        registry.add("pulsedb.debezium.database-password", postgres::getPassword);
    }

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("ALTER USER pulsedb_user REPLICATION");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void whenUserInserted_thenChangeEventPublishedToKafka() {
        // GIVEN: a consumer listening on the users topic
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-group", "true", embeddedKafka);
        var consumer = consumerFactory.createConsumer("test-group", "client-1");
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "pulsedb.users");

        // WHEN: insert a new user
        jdbcTemplate.update(
                "INSERT INTO users (email, name, status) VALUES (?, ?, ?)",
                "charlie@example.com", "Charlie Davis", "ACTIVE"
        );

        // THEN: ChangeEvent should appear on pulsedb.users
        ConsumerRecord<String, ChangeEvent> record = KafkaTestUtils.getSingleRecord(
                consumer, "pulsedb.users");

        assertThat(record).isNotNull();
        assertThat(record.value()).isNotNull();
        assertThat(record.value().getOperation()).isEqualTo(OperationType.INSERT);
        assertThat(record.value().getTableName()).isEqualTo("users");
        assertThat(record.value().getAfter()).isNotNull();
        assertThat(record.value().getAfter().get("email").asText())
                .isEqualTo("charlie@example.com");

        consumer.close();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void whenUserUpdated_thenChangeEventContainsBothBeforeAndAfter() {
        // GIVEN: Alice exists (seeded in init.sql)
        var consumer = consumerFactory.createConsumer("update-test-group", "client-2");
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "pulsedb.users");

        // WHEN: update Alice's name
        int updated = jdbcTemplate.update(
                "UPDATE users SET name = ? WHERE email = ?",
                "Alice Johnson-Smith", "alice@example.com"
        );
        assertThat(updated).isEqualTo(1);

        // THEN: UPDATE event with both before and after populated
        ConsumerRecord<String, ChangeEvent> record = KafkaTestUtils.getSingleRecord(
                consumer, "pulsedb.users");

        assertThat(record.value().getOperation()).isEqualTo(OperationType.UPDATE);
        assertThat(record.value().getBefore()).isNotNull();
        assertThat(record.value().getAfter()).isNotNull();
        assertThat(record.value().getAfter().get("name").asText())
                .isEqualTo("Alice Johnson-Smith");

        consumer.close();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void whenProductDeleted_thenChangeEventHasNullAfter() {
        // GIVEN: a product we're about to delete
        jdbcTemplate.update(
                "INSERT INTO products (name, price, stock) VALUES (?, ?, ?)",
                "Test Gadget", 1999.00, 5
        );

        var consumer = consumerFactory.createConsumer("delete-test-group", "client-3");
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "pulsedb.products");

        // WHEN: delete the product
        jdbcTemplate.update("DELETE FROM products WHERE name = ?", "Test Gadget");

        // THEN: DELETE event — before has the row data, after is null
        ConsumerRecord<String, ChangeEvent> record = KafkaTestUtils.getSingleRecord(
                consumer, "pulsedb.products");

        assertThat(record.value().getOperation()).isEqualTo(OperationType.DELETE);
        assertThat(record.value().getBefore()).isNotNull();
        assertThat(record.value().getBefore().get("name").asText()).isEqualTo("Test Gadget");
        assertThat(record.value().getAfter()).isNull();

        consumer.close();
    }
}
