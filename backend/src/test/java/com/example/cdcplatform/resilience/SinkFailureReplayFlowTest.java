package com.example.cdcplatform.resilience;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.event.CdcEventId;
import com.example.cdcplatform.ledger.CdcEventLedgerRepository;
import com.example.cdcplatform.replay.DlqEventRepository;
import com.example.cdcplatform.replay.ReplayRequestRepository;
import com.example.cdcplatform.replay.ReplayRequestService;
import com.example.cdcplatform.retry.RetryEventRepository;
import com.example.cdcplatform.retry.RetryEventService;
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
class SinkFailureReplayFlowTest {

    private static final String SOURCE_CONNECTOR = "ats-source";
    private static final String SOURCE_SCHEMA = "public";
    private static final String SOURCE_TABLE = "applicants";
    private static final String SOURCE_PRIMARY_KEY = "19c14e85-9840-7a5a-bb16-3c7f70547d9f";
    private static final BigInteger SOURCE_LSN = new BigInteger("88432240");
    private static final String SOURCE_OFFSET_JSON = "{\"file\":\"000000010000000000000058\",\"pos\":32240,\"lsn\":88432240}";
    private static final String RAW_PAYLOAD_JSON = "{\"op\":\"u\",\"after\":{\"id\":\"19c14e85-9840-7a5a-bb16-3c7f70547d9f\"}}";
    private static final String RETRY_TOPIC = "cdc.retry.applicants";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;
    private static CdcEventLedgerRepository ledgerRepository;
    private static ReplayRequestService replayRequestService;
    private static RetryEventService retryEventService;

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
        ledgerRepository = new CdcEventLedgerRepository(dataSource);
        replayRequestService = new ReplayRequestService(
            new DlqEventRepository(dataSource),
            new ReplayRequestRepository(dataSource)
        );
        retryEventService = new RetryEventService(new RetryEventRepository(dataSource));
    }

    @Test
    void sinkFailureReplayFlowKeepsSourceMetadataIdempotency() {
        CdcEventId eventId = CdcEventId.fromSourceMetadata(
            SOURCE_CONNECTOR,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            SOURCE_LSN,
            "u"
        );

        boolean firstLedgerInsert = ledgerRepository.insertIfAbsent(ledgerEvent(eventId));
        boolean duplicateLedgerInsert = ledgerRepository.insertIfAbsent(ledgerEvent(eventId));
        assertThat(firstLedgerInsert).isTrue();
        assertThat(duplicateLedgerInsert).isFalse();

        long dlqEventId = insertDlqEvent(eventId);
        ReplayRequestService.ReplayRequestResult replayResult = replayRequestService.requestReplay(
            dlqEventId,
            "local-operator"
        );

        assertThat(replayResult.replayStatus()).isEqualTo("REQUESTED");
        assertThat(replayResult.sourceTable()).isEqualTo(SOURCE_TABLE);
        assertThat(replayResult.sourcePrimaryKey()).isEqualTo(SOURCE_PRIMARY_KEY);
        assertThat(replayResult.sourceLsn()).isEqualTo(SOURCE_LSN);

        RetryEventService.RetryEventResult retryResult = retryEventService.scheduleRetry(new RetryEventService.ScheduleRetryCommand(
            eventId.value(),
            SOURCE_CONNECTOR,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            SOURCE_LSN,
            SOURCE_OFFSET_JSON,
            RAW_PAYLOAD_JSON,
            "lakehouse sink unavailable",
            RETRY_TOPIC,
            OffsetDateTime.parse("2026-06-09T10:00:00Z"),
            3
        ));
        RetryEventService.PublishReadyResult publishResult = retryEventService.publishReady(
            OffsetDateTime.parse("2026-06-09T11:00:00Z"),
            10
        );

        assertThat(retryResult.created()).isTrue();
        assertThat(publishResult.publishedEvents()).hasSize(1);
        RetryEventService.PublishedRetryEvent publishedEvent = publishResult.publishedEvents().getFirst();
        assertThat(publishedEvent.eventId()).isEqualTo(eventId.value());
        assertThat(publishedEvent.retryTopic()).isEqualTo(RETRY_TOPIC);
        assertThat(publishedEvent.sourceLsn()).isEqualTo(SOURCE_LSN);
        assertThat(publishedEvent.attemptCount()).isOne();

        assertThat(countRows("cdc_event_ledger")).isOne();
        assertThat(countRows("cdc_dlq_events where replay_status = 'REPLAY_REQUESTED'")).isOne();
        assertThat(countRows("cdc_replay_requests where replay_status = 'REQUESTED'")).isOne();
        assertThat(countRows("cdc_retry_events where retry_status = 'PUBLISHED'")).isOne();
    }

    private CdcEventLedgerRepository.LedgerEvent ledgerEvent(CdcEventId eventId) {
        return new CdcEventLedgerRepository.LedgerEvent(
            eventId,
            SOURCE_CONNECTOR,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            SOURCE_LSN,
            SOURCE_OFFSET_JSON,
            "u",
            "FAILED"
        );
    }

    private long insertDlqEvent(CdcEventId eventId) {
        return jdbcTemplate.queryForObject(
            """
                insert into cdc_dlq_events(
                    event_id,
                    source_connector,
                    source_table,
                    source_primary_key,
                    source_lsn,
                    source_offset,
                    raw_payload,
                    failure_reason,
                    replay_status
                )
                values (?, ?, ?, ?, ?, cast(? as jsonb), cast(? as jsonb), 'lakehouse sink unavailable', 'FAILED')
                returning id
                """,
            Long.class,
            eventId.value(),
            SOURCE_CONNECTOR,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            SOURCE_LSN,
            SOURCE_OFFSET_JSON,
            RAW_PAYLOAD_JSON
        );
    }

    private int countRows(String tableExpression) {
        return jdbcTemplate.queryForObject("select count(*) from " + tableExpression, Integer.class);
    }
}
