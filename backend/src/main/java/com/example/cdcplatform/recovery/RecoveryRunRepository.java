package com.example.cdcplatform.recovery;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnBean(DataSource.class)
public class RecoveryRunRepository {

    private final JdbcTemplate jdbcTemplate;

    public RecoveryRunRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public boolean insertIfAbsent(RecoveryRun recoveryRun) {
        int inserted = jdbcTemplate.update(
            """
                insert into cdc_recovery_runs(
                    recovery_run_id,
                    scenario,
                    connector_name,
                    source_table,
                    source_primary_key,
                    source_lsn_from,
                    source_lsn_to,
                    source_offset_from,
                    source_offset_to,
                    event_id,
                    recovery_status,
                    recovered_event_count,
                    duplicate_event_count,
                    gap_detected,
                    details,
                    outage_started_at,
                    recovered_at
                )
                values (?, ?, ?, ?, ?, ?, ?, cast(? as jsonb), cast(? as jsonb), ?, ?, ?, ?, ?, cast(? as jsonb), ?, ?)
                on conflict (
                    scenario,
                    connector_name,
                    source_table,
                    source_primary_key,
                    source_lsn_from,
                    source_lsn_to,
                    event_id
                )
                do nothing
                """,
            recoveryRun.recoveryRunId(),
            recoveryRun.scenario(),
            recoveryRun.connectorName(),
            recoveryRun.sourceTable(),
            recoveryRun.sourcePrimaryKey(),
            recoveryRun.sourceLsnFrom(),
            recoveryRun.sourceLsnTo(),
            recoveryRun.sourceOffsetFromJson(),
            recoveryRun.sourceOffsetToJson(),
            recoveryRun.eventId(),
            recoveryRun.recoveryStatus(),
            recoveryRun.recoveredEventCount(),
            recoveryRun.duplicateEventCount(),
            recoveryRun.gapDetected(),
            recoveryRun.detailsJson(),
            recoveryRun.outageStartedAt(),
            recoveryRun.recoveredAt()
        );
        return inserted == 1;
    }

    public Optional<RecoveryRun> findLatestByScenarioAndConnector(String scenario, String connectorName) {
        try {
            RecoveryRun recoveryRun = jdbcTemplate.queryForObject(
                """
                    select
                        recovery_run_id,
                        scenario,
                        connector_name,
                        source_table,
                        source_primary_key,
                        source_lsn_from,
                        source_lsn_to,
                        source_offset_from::text,
                        source_offset_to::text,
                        event_id,
                        recovery_status,
                        recovered_event_count,
                        duplicate_event_count,
                        gap_detected,
                        details::text,
                        outage_started_at,
                        recovered_at,
                        created_at
                    from cdc_recovery_runs
                    where scenario = ?
                      and connector_name = ?
                    order by recovered_at desc nulls last, created_at desc
                    limit 1
                    """,
                new RecoveryRunRowMapper(),
                scenario,
                connectorName
            );
            return Optional.ofNullable(recoveryRun);
        } catch (EmptyResultDataAccessException exc) {
            return Optional.empty();
        }
    }

    public record RecoveryRun(
        String recoveryRunId,
        String scenario,
        String connectorName,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsnFrom,
        BigInteger sourceLsnTo,
        String sourceOffsetFromJson,
        String sourceOffsetToJson,
        String eventId,
        String recoveryStatus,
        long recoveredEventCount,
        long duplicateEventCount,
        boolean gapDetected,
        String detailsJson,
        OffsetDateTime outageStartedAt,
        OffsetDateTime recoveredAt,
        OffsetDateTime createdAt
    ) {
    }

    private static class RecoveryRunRowMapper implements RowMapper<RecoveryRun> {
        @Override
        public RecoveryRun mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new RecoveryRun(
                rs.getString("recovery_run_id"),
                rs.getString("scenario"),
                rs.getString("connector_name"),
                rs.getString("source_table"),
                rs.getString("source_primary_key"),
                rs.getBigDecimal("source_lsn_from").toBigIntegerExact(),
                rs.getBigDecimal("source_lsn_to").toBigIntegerExact(),
                rs.getString("source_offset_from"),
                rs.getString("source_offset_to"),
                rs.getString("event_id"),
                rs.getString("recovery_status"),
                rs.getLong("recovered_event_count"),
                rs.getLong("duplicate_event_count"),
                rs.getBoolean("gap_detected"),
                rs.getString("details"),
                rs.getObject("outage_started_at", OffsetDateTime.class),
                rs.getObject("recovered_at", OffsetDateTime.class),
                rs.getObject("created_at", OffsetDateTime.class)
            );
        }
    }
}
