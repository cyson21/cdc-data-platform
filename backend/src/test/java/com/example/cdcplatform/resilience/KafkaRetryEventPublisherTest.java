package com.example.cdcplatform.resilience;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.retry.KafkaRetryEventPublisher;
import com.example.cdcplatform.retry.RetryEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class KafkaRetryEventPublisherTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String RETRY_TOPIC = "cdc.retry.applicants";
    private static final String EVENT_ID = "candidate-cdc-evt-kafka-1";
    private static final BigInteger SOURCE_LSN = new BigInteger("88432400");
    private static final String SOURCE_OFFSET_JSON = "{\"file\":\"000000010000000000000058\",\"pos\":32400,\"lsn\":88432400}";
    private static final String RAW_PAYLOAD_JSON = "{\"op\":\"u\",\"after\":{\"id\":\"19c14e85-9840-7a5a-bb16-3c7f70547d9f\"}}";
    private static final Path EVIDENCE_FILE = Path.of("target", "kafka-retry-publisher-evidence.json");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static EmbeddedKafkaBroker embeddedKafka;
    private static JdbcTemplate jdbcTemplate;
    private static RetryEventRepository retryEventRepository;
    private static KafkaTemplate<String, String> kafkaTemplate;
    private static KafkaRetryEventPublisher publisher;

    @BeforeAll
    static void setUp() throws Exception {
        embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1, RETRY_TOPIC)
            .brokerProperty("auto.create.topics.enable", "false")
            .adminTimeout(30);
        embeddedKafka.afterPropertiesSet();

        Flyway.configure()
            .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
            .load()
            .migrate();

        DriverManagerDataSource dataSource = new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(),
            POSTGRES.getUsername(),
            POSTGRES.getPassword()
        );
        jdbcTemplate = new JdbcTemplate(dataSource);
        retryEventRepository = new RetryEventRepository(dataSource);

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", StringSerializer.class);
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        publisher = new KafkaRetryEventPublisher(retryEventRepository, kafkaTemplate);

        Files.deleteIfExists(EVIDENCE_FILE);
    }

    @AfterAll
    static void tearDown() {
        if (kafkaTemplate != null) {
            kafkaTemplate.destroy();
        }
        if (embeddedKafka != null) {
            embeddedKafka.destroy();
        }
    }

    @Test
    void publishesReadyRetryEventToKafkaBeforeMarkingPublished() throws Exception {
        retryEventRepository.insertIfAbsent(new RetryEventRepository.RetryEvent(
            EVENT_ID,
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            SOURCE_LSN,
            SOURCE_OFFSET_JSON,
            RAW_PAYLOAD_JSON,
            "lakehouse sink unavailable",
            RETRY_TOPIC,
            OffsetDateTime.parse("2026-06-09T10:00:00Z"),
            3
        ));

        try (Consumer<String, String> consumer = newConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, RETRY_TOPIC);

            KafkaRetryEventPublisher.KafkaPublishResult publishResult = publisher.publishReady(
                OffsetDateTime.parse("2026-06-09T11:00:00Z"),
                10
            );

            assertThat(publishResult.publishedEvents()).hasSize(1);
            KafkaRetryEventPublisher.PublishedKafkaRetryEvent publishedEvent = publishResult.publishedEvents().getFirst();
            assertThat(publishedEvent.eventId()).isEqualTo(EVENT_ID);
            assertThat(publishedEvent.retryTopic()).isEqualTo(RETRY_TOPIC);
            assertThat(publishedEvent.sourceLsn()).isEqualTo(SOURCE_LSN);
            assertThat(publishedEvent.brokerOffset()).isGreaterThanOrEqualTo(0L);
            assertThat(publishedEvent.attemptCount()).isOne();

            ConsumerRecord<String, String> brokerRecord = KafkaTestUtils.getSingleRecord(
                consumer,
                RETRY_TOPIC,
                Duration.ofSeconds(10)
            );
            assertThat(brokerRecord.key()).isEqualTo(EVENT_ID);
            assertThat(OBJECT_MAPPER.readTree(brokerRecord.value()).get("op").asText()).isEqualTo("u");

            String retryStatus = jdbcTemplate.queryForObject(
                "select retry_status from cdc_retry_events where event_id = ?",
                String.class,
                EVENT_ID
            );
            Integer attemptCount = jdbcTemplate.queryForObject(
                "select attempt_count from cdc_retry_events where event_id = ?",
                Integer.class,
                EVENT_ID
            );
            assertThat(retryStatus).isEqualTo("PUBLISHED");
            assertThat(attemptCount).isOne();

            writeEvidence(publishedEvent, brokerRecord);
        }
    }

    private Consumer<String, String> newConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
            "cdc-platform-retry-publisher-test",
            "false",
            embeddedKafka
        );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(
            consumerProps,
            new StringDeserializer(),
            new StringDeserializer()
        ).createConsumer();
    }

    private void writeEvidence(
        KafkaRetryEventPublisher.PublishedKafkaRetryEvent publishedEvent,
        ConsumerRecord<String, String> brokerRecord
    ) throws Exception {
        Files.createDirectories(EVIDENCE_FILE.getParent());
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("scenario", "kafka-backed-sink-failure-replay");
        evidence.put("evidenceLabel", "embedded-kafka-broker");
        evidence.put("eventId", publishedEvent.eventId());
        evidence.put("sourceLsn", publishedEvent.sourceLsn());
        evidence.put("sourceOffset", OBJECT_MAPPER.readTree(publishedEvent.sourceOffsetJson()));
        evidence.put("retryTopic", publishedEvent.retryTopic());
        evidence.put("brokerPartition", brokerRecord.partition());
        evidence.put("brokerOffset", brokerRecord.offset());
        evidence.put("brokerAccepted", true);
        evidence.put("retryStatus", "PUBLISHED");
        evidence.put("attemptCount", publishedEvent.attemptCount());
        evidence.put("testResult", "passed");
        Files.writeString(EVIDENCE_FILE, OBJECT_MAPPER.writeValueAsString(evidence));
    }
}
