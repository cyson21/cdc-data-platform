package com.example.cdcplatform.quality;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class PipelineQualitySlaRepository {

    private final JdbcTemplate jdbcTemplate;

    public PipelineQualitySlaRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    public SlaEvaluationRecord insert(NewSlaEvaluation evaluation) {
        return jdbcTemplate.queryForObject(
            """
                insert into pipeline_quality_sla_evaluations(
                    check_name,
                    source_table,
                    source_row_count,
                    raw_event_count,
                    canonical_event_count,
                    duplicate_event_count,
                    missing_event_count,
                    completeness_ratio,
                    duplicate_ratio,
                    freshness_lag_millis,
                    lag_millis,
                    min_completeness_ratio,
                    max_duplicate_ratio,
                    max_freshness_lag_millis,
                    max_lag_millis,
                    sla_status,
                    violated_slo_ids,
                    warning_slo_ids,
                    details
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb)
                returning
                    id,
                    check_name,
                    source_table,
                    source_row_count,
                    raw_event_count,
                    canonical_event_count,
                    duplicate_event_count,
                    missing_event_count,
                    completeness_ratio,
                    duplicate_ratio,
                    freshness_lag_millis,
                    lag_millis,
                    min_completeness_ratio,
                    max_duplicate_ratio,
                    max_freshness_lag_millis,
                    max_lag_millis,
                    sla_status,
                    violated_slo_ids::text,
                    warning_slo_ids::text,
                    details::text,
                    evaluated_at
                """,
            new SlaEvaluationRecordRowMapper(),
            evaluation.checkName(),
            evaluation.sourceTable(),
            evaluation.sourceRowCount(),
            evaluation.rawEventCount(),
            evaluation.canonicalEventCount(),
            evaluation.duplicateEventCount(),
            evaluation.missingEventCount(),
            evaluation.completenessRatio(),
            evaluation.duplicateRatio(),
            evaluation.freshnessLagMillis(),
            evaluation.lagMillis(),
            evaluation.minCompletenessRatio(),
            evaluation.maxDuplicateRatio(),
            evaluation.maxFreshnessLagMillis(),
            evaluation.maxLagMillis(),
            evaluation.slaStatus(),
            evaluation.violatedSloIdsJson(),
            evaluation.warningSloIdsJson(),
            evaluation.detailsJson()
        );
    }

    public Optional<SlaEvaluationRecord> findLatestByCheckNameAndSourceTable(String checkName, String sourceTable) {
        try {
            SlaEvaluationRecord record = jdbcTemplate.queryForObject(
                """
                    select
                        id,
                        check_name,
                        source_table,
                        source_row_count,
                        raw_event_count,
                        canonical_event_count,
                        duplicate_event_count,
                        missing_event_count,
                        completeness_ratio,
                        duplicate_ratio,
                        freshness_lag_millis,
                        lag_millis,
                        min_completeness_ratio,
                        max_duplicate_ratio,
                        max_freshness_lag_millis,
                        max_lag_millis,
                        sla_status,
                        violated_slo_ids::text,
                        warning_slo_ids::text,
                        details::text,
                        evaluated_at
                    from pipeline_quality_sla_evaluations
                    where check_name = ?
                      and source_table = ?
                    order by evaluated_at desc, id desc
                    limit 1
                    """,
                new SlaEvaluationRecordRowMapper(),
                checkName,
                sourceTable
            );
            return Optional.ofNullable(record);
        } catch (EmptyResultDataAccessException exc) {
            return Optional.empty();
        }
    }

    public record NewSlaEvaluation(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        double completenessRatio,
        double duplicateRatio,
        long freshnessLagMillis,
        long lagMillis,
        double minCompletenessRatio,
        double maxDuplicateRatio,
        long maxFreshnessLagMillis,
        long maxLagMillis,
        String slaStatus,
        String violatedSloIdsJson,
        String warningSloIdsJson,
        String detailsJson
    ) {
    }

    public record SlaEvaluationRecord(
        long id,
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        double completenessRatio,
        double duplicateRatio,
        long freshnessLagMillis,
        long lagMillis,
        double minCompletenessRatio,
        double maxDuplicateRatio,
        long maxFreshnessLagMillis,
        long maxLagMillis,
        String slaStatus,
        String violatedSloIdsJson,
        String warningSloIdsJson,
        String detailsJson,
        OffsetDateTime evaluatedAt
    ) {
    }

    private static class SlaEvaluationRecordRowMapper implements RowMapper<SlaEvaluationRecord> {
        @Override
        public SlaEvaluationRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new SlaEvaluationRecord(
                rs.getLong("id"),
                rs.getString("check_name"),
                rs.getString("source_table"),
                rs.getLong("source_row_count"),
                rs.getLong("raw_event_count"),
                rs.getLong("canonical_event_count"),
                rs.getLong("duplicate_event_count"),
                rs.getLong("missing_event_count"),
                rs.getDouble("completeness_ratio"),
                rs.getDouble("duplicate_ratio"),
                rs.getLong("freshness_lag_millis"),
                rs.getLong("lag_millis"),
                rs.getDouble("min_completeness_ratio"),
                rs.getDouble("max_duplicate_ratio"),
                rs.getLong("max_freshness_lag_millis"),
                rs.getLong("max_lag_millis"),
                rs.getString("sla_status"),
                rs.getString("violated_slo_ids"),
                rs.getString("warning_slo_ids"),
                rs.getString("details"),
                rs.getObject("evaluated_at", OffsetDateTime.class)
            );
        }
    }
}
