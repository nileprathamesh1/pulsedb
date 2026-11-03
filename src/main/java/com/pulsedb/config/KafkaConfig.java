package com.pulsedb.config;

import com.pulsedb.model.ChangeEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConfig {

    @Value("${pulsedb.kafka.topics.users}")
    private String usersTopic;

    @Value("${pulsedb.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${pulsedb.kafka.topics.products}")
    private String productsTopic;

    @Value("${pulsedb.kafka.topics.dead-letter}")
    private String deadLetterTopic;

    // ---- Topic creation ----
    // Spring Kafka creates these on startup if they don't exist

    @Bean
    public NewTopic usersTopic() {
        return TopicBuilder.name(usersTopic)
                .partitions(3)   // 3 partitions allows up to 3 consumers in parallel
                .replicas(1)     // single replica for local dev
                .build();
    }

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic productsTopic() {
        return TopicBuilder.name(productsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaTemplate<String, ChangeEvent> kafkaTemplate(
            ProducerFactory<String, ChangeEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // Manual ack mode so we control exactly when offsets are committed
    // Default BATCH ack would commit even if processing failed mid-batch
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ChangeEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, ChangeEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, ChangeEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3); // one thread per partition
        factory.setCommonErrorHandler(kafkaErrorHandler());

        return factory;
    }

    private org.springframework.kafka.listener.CommonErrorHandler kafkaErrorHandler() {
        org.springframework.kafka.listener.DefaultErrorHandler handler =
                new org.springframework.kafka.listener.DefaultErrorHandler(
                        new org.springframework.util.backoff.FixedBackOff(2000L, 3L)
                );
        // Deserialization errors are unrecoverable — don't retry corrupt messages
        handler.addNotRetryableExceptions(
                org.apache.kafka.common.errors.SerializationException.class
        );
        return handler;
    }
}
