package com.example.cdcplatform.quality;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.example.cdcplatform.quality.PipelineQualitySlaRepository.NewSlaEvaluation;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository.SlaEvaluationRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PipelineQualitySlaRepositoryTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static PipelineQualitySlaRepository repository;

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
        repository = new PipelineQualitySlaRepository(dataSource);
    }

    @Test
    void insertsSlaEvaluationAndFindsLatestByNameAndSourceTable() throws Exception {
        repository.insert(newSlaEvaluation(
            "WARN",
            3,
            4,
            3,
            1,
            0,
            1.0,
            0.25,
            30_000,
            45_000,
            "[\"duplicate-rate\"]",
            "{\"sourceLsnFrom\":88432200,\"sourceLsnTo\":88432239,\"eventIds\":[\"cdc_old\"]}"
        ));
        SlaEvaluationRecord inserted = repository.insert(newSlaEvaluation(
            "FAILED",
            100,
            102,
            98,
            2,
            2,
            0.98,
            0.0196,
            90_000,
            180_000,
            "[\"completeness\",\"duplicate-rate\",\"freshness\",\"lag\"]",
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_new_1\",\"cdc_new_2\"]}"
        ));
        repository.insert(new NewSlaEvaluation(
            "agent-task-cdc-sla",
            "agent_tasks",
            1,
            1,
            1,
            0,
            0,
            1.0,
            0.0,
            5_000,
            7_000,
            0.99,
            0.01,
            60_000,
            120_000,
            "PASSED",
            "[]",
            "[]",
            "{\"sourceLsnFrom\":88432000,\"sourceLsnTo\":88432008,\"eventIds\":[]}"
        ));

        Optional<SlaEvaluationRecord> latest = repository.findLatestByCheckNameAndSourceTable(
            "applicant-cdc-sla",
            "applicants"
        );

        assertThat(latest).isPresent();
        assertThat(latest.get().id()).isEqualTo(inserted.id());
        assertThat(latest.get().slaStatus()).isEqualTo("FAILED");
        assertThat(latest.get().sourceRowCount()).isEqualTo(100);
        assertThat(latest.get().rawEventCount()).isEqualTo(102);
        assertThat(latest.get().canonicalEventCount()).isEqualTo(98);
        assertThat(latest.get().duplicateEventCount()).isEqualTo(2);
        assertThat(latest.get().missingEventCount()).isEqualTo(2);
        assertThat(latest.get().completenessRatio()).isEqualTo(0.98);
        assertThat(latest.get().duplicateRatio()).isCloseTo(0.0196, within(0.0001));
        assertThat(latest.get().freshnessLagMillis()).isEqualTo(90_000);
        assertThat(latest.get().lagMillis()).isEqualTo(180_000);
        JsonNode violated = OBJECT_MAPPER.readTree(latest.get().violatedSloIdsJson());
        assertThat(violated).hasSize(4);
        JsonNode details = OBJECT_MAPPER.readTree(latest.get().detailsJson());
        assertThat(details.get("sourceLsnFrom").asLong()).isEqualTo(88432240L);
        assertThat(details.get("sourceLsnTo").asLong()).isEqualTo(88432320L);
        assertThat(details.get("eventIds")).hasSize(2);
        assertThat(latest.get().evaluatedAt()).isAfterOrEqualTo(OffsetDateTime.parse("2026-06-09T00:00:00Z"));
    }

    private static NewSlaEvaluation newSlaEvaluation(
        String slaStatus,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        double completenessRatio,
        double duplicateRatio,
        long freshnessLagMillis,
        long lagMillis,
        String violatedSloIdsJson,
        String detailsJson
    ) {
        return new NewSlaEvaluation(
            "applicant-cdc-sla",
            "applicants",
            sourceRowCount,
            rawEventCount,
            canonicalEventCount,
            duplicateEventCount,
            missingEventCount,
            completenessRatio,
            duplicateRatio,
            freshnessLagMillis,
            lagMillis,
            0.99,
            0.01,
            60_000,
            120_000,
            slaStatus,
            violatedSloIdsJson,
            "[]",
            detailsJson
        );
    }
}
