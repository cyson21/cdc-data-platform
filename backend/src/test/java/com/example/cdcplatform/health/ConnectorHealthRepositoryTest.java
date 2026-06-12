package com.example.cdcplatform.health;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.health.ConnectorHealthRepository.ConnectorHealthSnapshot;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ConnectorHealthRepositoryTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String CONNECTOR_NAME = "ats-source";
    private static final OffsetDateTime EARLY_CAPTURED_AT = OffsetDateTime.parse("2026-06-08T09:00:00Z");
    private static final OffsetDateTime LATEST_CAPTURED_AT = OffsetDateTime.parse("2026-06-08T10:00:00Z");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;
    private static ConnectorHealthRepository repository;

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
        repository = new ConnectorHealthRepository(dataSource);
    }

    @Test
    void findsLatestSnapshotByConnectorNameAndCapturedAt() {
        insertSnapshot(CONNECTOR_NAME, "RUNNING", "RUNNING", 120L, "{\"committed\":42}", EARLY_CAPTURED_AT);
        insertSnapshot(CONNECTOR_NAME, "PAUSED", "RUNNING", 240L, "{\"committed\":84}", LATEST_CAPTURED_AT);
        insertSnapshot("analytics-source", "FAILED", "RETRYING", 500L, "{\"committed\":1}", OffsetDateTime.parse("2026-06-08T08:00:00Z"));

        Optional<ConnectorHealthSnapshot> latest = repository.findLatestByConnectorName(CONNECTOR_NAME);

        assertThat(latest).isPresent();
        assertThat(latest.get().connectorName()).isEqualTo(CONNECTOR_NAME);
        assertThat(latest.get().connectorStatus()).isEqualTo("PAUSED");
        assertThat(latest.get().taskStatus()).isEqualTo("RUNNING");
        assertThat(latest.get().lagMillis()).isEqualTo(240L);
        JsonNode offsetSummary = parseJson(latest.get().offsetSummaryJson());
        assertThat(offsetSummary.get("committed").asLong()).isEqualTo(84L);
        assertThat(latest.get().capturedAt()).isEqualTo(LATEST_CAPTURED_AT);
    }

    private void insertSnapshot(
        String connectorName,
        String connectorStatus,
        String taskStatus,
        Long lagMillis,
        String offsetSummary,
        OffsetDateTime capturedAt
    ) {
        jdbcTemplate.update(
            """
                insert into connector_health_snapshots(
                    connector_name,
                    connector_status,
                    task_status,
                    lag_millis,
                    offset_summary,
                    captured_at
                )
                values (?, ?, ?, ?, cast(? as jsonb), ?)
                """,
            connectorName,
            connectorStatus,
            taskStatus,
            lagMillis,
            offsetSummary,
            capturedAt
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
