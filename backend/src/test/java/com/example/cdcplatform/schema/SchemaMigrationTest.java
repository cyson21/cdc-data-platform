package com.example.cdcplatform.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.TreeSet;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SchemaMigrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    @Test
    void migratesSourceAndControlPlaneTables() throws Exception {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();

        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("""
                     select table_name
                     from information_schema.tables
                     where table_schema = 'public'
                     order by table_name
                     """)) {

            Set<String> tableNames = new TreeSet<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("table_name"));
            }

            assertThat(tableNames).contains(
                    "agent_tasks",
                    "applicants",
                    "cdc_dlq_events",
                    "cdc_event_ledger",
                    "cdc_replay_requests",
                    "cdc_retry_events",
                    "connector_health_snapshots",
                    "evaluations",
                    "job_postings",
                    "pipeline_quality_checks",
                    "pipeline_quality_sla_evaluations"
            );
        }

        assertColumnsExist("job_postings", "id", "title", "status", "updated_at");
        assertColumnsExist("applicants", "id", "job_posting_id", "stage", "deleted_at", "updated_at");
        assertColumnsExist("evaluations", "id", "applicant_id", "score", "grade", "updated_at");
        assertColumnsExist("agent_tasks", "id", "applicant_id", "task_type", "attempt_count", "updated_at");
        assertColumnsExist("cdc_event_ledger", "event_id", "source_lsn", "source_offset", "processed_status");
        assertColumnsExist("cdc_dlq_events", "event_id", "source_lsn", "failure_reason", "replay_status");
        assertColumnsExist("cdc_retry_events", "event_id", "source_lsn", "source_offset", "retry_topic", "next_attempt_at", "retry_status");
        assertColumnsExist(
            "pipeline_quality_sla_evaluations",
            "check_name",
            "source_table",
            "completeness_ratio",
            "duplicate_ratio",
            "freshness_lag_millis",
            "lag_millis",
            "min_completeness_ratio",
            "max_duplicate_ratio",
            "max_freshness_lag_millis",
            "max_lag_millis",
            "sla_status",
            "violated_slo_ids",
            "warning_slo_ids",
            "details"
        );
    }

    private void assertColumnsExist(String tableName, String... expectedColumns) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("""
                     select column_name
                     from information_schema.columns
                     where table_schema = 'public'
                       and table_name = '%s'
                     order by column_name
                     """.formatted(tableName))) {

            Set<String> columnNames = new TreeSet<>();
            while (resultSet.next()) {
                columnNames.add(resultSet.getString("column_name"));
            }

            assertThat(columnNames).contains(expectedColumns);
        }
    }
}
