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

public class PipelineQualityCheckRepository {

    private final JdbcTemplate jdbcTemplate;

    public PipelineQualityCheckRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(Objects.requireNonNull(dataSource, "dataSource"));
    }

    public QualityCheckRecord insert(NewQualityCheck check) {
        return jdbcTemplate.queryForObject(
            """
                insert into pipeline_quality_checks(
                    check_name,
                    source_table,
                    source_row_count,
                    raw_event_count,
                    canonical_event_count,
                    duplicate_event_count,
                    missing_event_count,
                    check_status,
                    details
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                returning
                    id,
                    check_name,
                    source_table,
                    source_row_count,
                    raw_event_count,
                    canonical_event_count,
                    duplicate_event_count,
                    missing_event_count,
                    check_status,
                    details::text,
                    checked_at
                """,
            new QualityCheckRecordRowMapper(),
            check.checkName(),
            check.sourceTable(),
            check.sourceRowCount(),
            check.rawEventCount(),
            check.canonicalEventCount(),
            check.duplicateEventCount(),
            check.missingEventCount(),
            check.checkStatus(),
            check.detailsJson()
        );
    }

    public Optional<QualityCheckRecord> findLatestByCheckNameAndSourceTable(String checkName, String sourceTable) {
        try {
            QualityCheckRecord record = jdbcTemplate.queryForObject(
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
                        check_status,
                        details::text,
                        checked_at
                    from pipeline_quality_checks
                    where check_name = ?
                      and source_table = ?
                    order by checked_at desc, id desc
                    limit 1
                    """,
                new QualityCheckRecordRowMapper(),
                checkName,
                sourceTable
            );
            return Optional.ofNullable(record);
        } catch (EmptyResultDataAccessException exc) {
            return Optional.empty();
        }
    }

    public record NewQualityCheck(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        String checkStatus,
        String detailsJson
    ) {
    }

    public record QualityCheckRecord(
        long id,
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        String checkStatus,
        String detailsJson,
        OffsetDateTime checkedAt
    ) {
    }

    private static class QualityCheckRecordRowMapper implements RowMapper<QualityCheckRecord> {
        @Override
        public QualityCheckRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new QualityCheckRecord(
                rs.getLong("id"),
                rs.getString("check_name"),
                rs.getString("source_table"),
                rs.getLong("source_row_count"),
                rs.getLong("raw_event_count"),
                rs.getLong("canonical_event_count"),
                rs.getLong("duplicate_event_count"),
                rs.getLong("missing_event_count"),
                rs.getString("check_status"),
                rs.getString("details"),
                rs.getObject("checked_at", OffsetDateTime.class)
            );
        }
    }
}
