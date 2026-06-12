package com.example.cdcplatform.replay;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ReplayRequestServiceTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;
    private static ReplayRequestService service;

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
        service = new ReplayRequestService(
            new DlqEventRepository(dataSource),
            new ReplayRequestRepository(dataSource)
        );
    }

    @Test
    void createsReplayRequestAndMarksDlqEventForReplay() {
        long dlqEventId = insertDlqEvent();

        ReplayRequestService.ReplayRequestResult result = service.requestReplay(dlqEventId, "local-operator");

        assertThat(result.requestId()).startsWith("replay_");
        assertThat(result.sourceTable()).isEqualTo("applicants");
        assertThat(result.sourcePrimaryKey()).isEqualTo("19c14e85-9840-7a5a-bb16-3c7f70547d9f");
        assertThat(result.replayStatus()).isEqualTo("REQUESTED");

        String dlqStatus = jdbcTemplate.queryForObject(
            "select replay_status from cdc_dlq_events where id = ?",
            String.class,
            dlqEventId
        );
        assertThat(dlqStatus).isEqualTo("REPLAY_REQUESTED");

        Integer requestCount = jdbcTemplate.queryForObject(
            "select count(*) from cdc_replay_requests where request_id = ? and requested_by = ?",
            Integer.class,
            result.requestId(),
            "local-operator"
        );
        assertThat(requestCount).isOne();
    }

    private long insertDlqEvent() {
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
                values (
                    'cdc_failed_applicant',
                    'ats-source',
                    'applicants',
                    '19c14e85-9840-7a5a-bb16-3c7f70547d9f',
                    ?,
                    cast(? as jsonb),
                    cast(? as jsonb),
                    'canonical publisher unavailable',
                    'FAILED'
                )
                returning id
                """,
            Long.class,
            new BigInteger("88432240"),
            "{\"lsn\":88432240}",
            "{\"op\":\"u\"}"
        );
    }
}
