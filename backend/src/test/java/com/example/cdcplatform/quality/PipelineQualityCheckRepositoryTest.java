package com.example.cdcplatform.quality;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.quality.PipelineQualityCheckRepository.NewQualityCheck;
import com.example.cdcplatform.quality.PipelineQualityCheckRepository.QualityCheckRecord;
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
class PipelineQualityCheckRepositoryTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static PipelineQualityCheckRepository repository;

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
        repository = new PipelineQualityCheckRepository(dataSource);
    }

    @Test
    void insertsQualityCheckAndFindsLatestByNameAndSourceTable() throws Exception {
        repository.insert(newQualityCheck(
            "applicant-cdc-completeness",
            "applicants",
            "WARN",
            4,
            1,
            "{\"sourceLsnFrom\":88432200,\"sourceLsnTo\":88432239,\"eventIds\":[\"cdc_old\"]}"
        ));
        QualityCheckRecord inserted = repository.insert(newQualityCheck(
            "applicant-cdc-completeness",
            "applicants",
            "PASSED",
            3,
            0,
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432288,\"eventIds\":[\"cdc_new_1\",\"cdc_new_2\"]}"
        ));
        repository.insert(newQualityCheck(
            "agent-task-cdc-completeness",
            "agent_tasks",
            "FAILED",
            1,
            1,
            "{\"sourceLsnFrom\":88432000,\"sourceLsnTo\":88432008,\"eventIds\":[]}"
        ));

        Optional<QualityCheckRecord> latest = repository.findLatestByCheckNameAndSourceTable(
            "applicant-cdc-completeness",
            "applicants"
        );

        assertThat(latest).isPresent();
        assertThat(latest.get().id()).isEqualTo(inserted.id());
        assertThat(latest.get().checkStatus()).isEqualTo("PASSED");
        assertThat(latest.get().sourceRowCount()).isEqualTo(3);
        assertThat(latest.get().rawEventCount()).isEqualTo(3);
        assertThat(latest.get().canonicalEventCount()).isEqualTo(3);
        assertThat(latest.get().missingEventCount()).isZero();
        JsonNode details = OBJECT_MAPPER.readTree(latest.get().detailsJson());
        assertThat(details.get("sourceLsnFrom").asLong()).isEqualTo(88432240L);
        assertThat(details.get("sourceLsnTo").asLong()).isEqualTo(88432288L);
        assertThat(details.get("eventIds")).hasSize(2);
        assertThat(latest.get().checkedAt()).isAfterOrEqualTo(OffsetDateTime.parse("2026-06-09T00:00:00Z"));
    }

    private static NewQualityCheck newQualityCheck(
        String checkName,
        String sourceTable,
        String checkStatus,
        long rawEventCount,
        long missingEventCount,
        String detailsJson
    ) {
        return new NewQualityCheck(
            checkName,
            sourceTable,
            3,
            rawEventCount,
            3,
            rawEventCount - 3,
            missingEventCount,
            checkStatus,
            detailsJson
        );
    }
}
