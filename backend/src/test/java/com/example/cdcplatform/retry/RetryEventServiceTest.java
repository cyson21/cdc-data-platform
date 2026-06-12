package com.example.cdcplatform.retry;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RetryEventServiceTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;
    private static RetryEventService service;

    @BeforeAll
    static void setUpDatabase() {
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
        service = new RetryEventService(new RetryEventRepository(dataSource));
    }

    @Test
    void schedulesRetryEventOnceUsingSourceMetadata() {
        RetryEventService.ScheduleRetryCommand command = retryCommand(
            "candidate-cdc-evt-1",
            new BigInteger("88432240"),
            OffsetDateTime.parse("2026-06-09T12:00:00Z")
        );

        RetryEventService.RetryEventResult first = service.scheduleRetry(command);
        RetryEventService.RetryEventResult duplicate = service.scheduleRetry(command);

        assertThat(first.created()).isTrue();
        assertThat(duplicate.created()).isFalse();
        assertThat(duplicate.retryEventId()).isEqualTo(first.retryEventId());
        assertThat(duplicate.eventId()).isEqualTo("candidate-cdc-evt-1");
        assertThat(duplicate.sourceLsn()).isEqualTo(new BigInteger("88432240"));
        assertThat(duplicate.retryStatus()).isEqualTo("SCHEDULED");

        Integer count = jdbcTemplate.queryForObject(
            """
                select count(*)
                from cdc_retry_events
                where source_connector = ?
                  and source_table = ?
                  and source_primary_key = ?
                  and source_lsn = ?
                """,
            Integer.class,
            "ats-source",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432240")
        );
        assertThat(count).isOne();
    }

    @Test
    void publishesReadyRetryEventsAndRecordsAttempt() {
        RetryEventService.RetryEventResult scheduled = service.scheduleRetry(retryCommand(
            "candidate-cdc-evt-2",
            new BigInteger("88432241"),
            OffsetDateTime.parse("2026-06-09T10:00:00Z")
        ));

        RetryEventService.PublishReadyResult publishResult = service.publishReady(
            OffsetDateTime.parse("2026-06-09T11:00:00Z"),
            10
        );

        assertThat(publishResult.publishedEvents()).hasSize(1);
        RetryEventService.PublishedRetryEvent event = publishResult.publishedEvents().getFirst();
        assertThat(event.retryEventId()).isEqualTo(scheduled.retryEventId());
        assertThat(event.eventId()).isEqualTo("candidate-cdc-evt-2");
        assertThat(event.retryTopic()).isEqualTo("cdc.retry.applicants");
        assertThat(parseJson(event.sourceOffsetJson()).get("lsn").asLong()).isEqualTo(88432241L);
        assertThat(parseJson(event.rawPayloadJson()).get("op").asText()).isEqualTo("u");

        Integer attemptCount = jdbcTemplate.queryForObject(
            "select attempt_count from cdc_retry_events where id = ?",
            Integer.class,
            scheduled.retryEventId()
        );
        String retryStatus = jdbcTemplate.queryForObject(
            "select retry_status from cdc_retry_events where id = ?",
            String.class,
            scheduled.retryEventId()
        );
        assertThat(attemptCount).isOne();
        assertThat(retryStatus).isEqualTo("PUBLISHED");
    }

    private RetryEventService.ScheduleRetryCommand retryCommand(String eventId) {
        return retryCommand(eventId, new BigInteger("88432240"), OffsetDateTime.parse("2026-06-09T10:00:00Z"));
    }

    private RetryEventService.ScheduleRetryCommand retryCommand(
        String eventId,
        BigInteger sourceLsn,
        OffsetDateTime nextAttemptAt
    ) {
        return new RetryEventService.ScheduleRetryCommand(
            eventId,
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            sourceLsn,
            "{\"lsn\":" + sourceLsn + "}",
            "{\"op\":\"u\",\"after\":{\"id\":\"19c14e85-9840-7a5a-bb16-3c7f70547d9f\"}}",
            "canonical publisher unavailable",
            "cdc.retry.applicants",
            nextAttemptAt,
            3
        );
    }

    private JsonNode parseJson(String value) {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception exc) {
            throw new AssertionError("invalid json payload", exc);
        }
    }
}
