package com.example.cdcplatform.retry;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnBean(DataSource.class)
public class RetryEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public RetryEventRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public InsertResult insertIfAbsent(RetryEvent event) {
        try {
            RetryEventRecord inserted = jdbcTemplate.queryForObject(
                """
                    insert into cdc_retry_events(
                        event_id,
                        source_connector,
                        source_schema,
                        source_table,
                        source_primary_key,
                        source_lsn,
                        source_offset,
                        raw_payload,
                        failure_reason,
                        retry_topic,
                        max_attempts,
                        next_attempt_at,
                        retry_status
                    )
                    values (?, ?, ?, ?, ?, ?, cast(? as jsonb), cast(? as jsonb), ?, ?, ?, ?, 'SCHEDULED')
                    returning
                        id,
                        event_id,
                        source_connector,
                        source_schema,
                        source_table,
                        source_primary_key,
                        source_lsn,
                        source_offset::text,
                        raw_payload::text,
                        failure_reason,
                        retry_topic,
                        attempt_count,
                        max_attempts,
                        next_attempt_at,
                        retry_status
                    """,
                new RetryEventRecordRowMapper(),
                event.eventId(),
                event.sourceConnector(),
                event.sourceSchema(),
                event.sourceTable(),
                event.sourcePrimaryKey(),
                new BigDecimal(event.sourceLsn()),
                event.sourceOffsetJson(),
                event.rawPayloadJson(),
                event.failureReason(),
                event.retryTopic(),
                event.maxAttempts(),
                event.nextAttemptAt()
            );
            return new InsertResult(inserted, true);
        } catch (DuplicateKeyException exc) {
            return new InsertResult(findBySourceEvent(event).orElseThrow(), false);
        }
    }

    public List<RetryEventRecord> findReadyForPublish(OffsetDateTime readyAt, int limit) {
        return jdbcTemplate.query(
            """
                select
                    id,
                    event_id,
                    source_connector,
                    source_schema,
                    source_table,
                    source_primary_key,
                    source_lsn,
                    source_offset::text,
                    raw_payload::text,
                    failure_reason,
                    retry_topic,
                    attempt_count,
                    max_attempts,
                    next_attempt_at,
                    retry_status
                from cdc_retry_events
                where retry_status = 'SCHEDULED'
                  and next_attempt_at <= ?
                  and attempt_count < max_attempts
                order by next_attempt_at, id
                limit ?
                """,
            new RetryEventRecordRowMapper(),
            readyAt,
            limit
        );
    }

    public int markPublished(long retryEventId) {
        return jdbcTemplate.queryForObject(
            """
                update cdc_retry_events
                set retry_status = 'PUBLISHED',
                    attempt_count = attempt_count + 1,
                    published_at = now(),
                    updated_at = now()
                where id = ?
                  and retry_status = 'SCHEDULED'
                returning attempt_count
                """,
            Integer.class,
            retryEventId
        );
    }

    private Optional<RetryEventRecord> findBySourceEvent(RetryEvent event) {
        try {
            RetryEventRecord record = jdbcTemplate.queryForObject(
                """
                    select
                        id,
                        event_id,
                        source_connector,
                        source_schema,
                        source_table,
                        source_primary_key,
                        source_lsn,
                        source_offset::text,
                        raw_payload::text,
                        failure_reason,
                        retry_topic,
                        attempt_count,
                        max_attempts,
                        next_attempt_at,
                        retry_status
                    from cdc_retry_events
                    where source_connector = ?
                      and source_table = ?
                      and source_primary_key = ?
                      and source_lsn = ?
                    """,
                new RetryEventRecordRowMapper(),
                event.sourceConnector(),
                event.sourceTable(),
                event.sourcePrimaryKey(),
                new BigDecimal(event.sourceLsn())
            );
            return Optional.ofNullable(record);
        } catch (EmptyResultDataAccessException exc) {
            return Optional.empty();
        }
    }

    public record InsertResult(RetryEventRecord record, boolean created) {
    }

    public record RetryEvent(
        String eventId,
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String sourceOffsetJson,
        String rawPayloadJson,
        String failureReason,
        String retryTopic,
        OffsetDateTime nextAttemptAt,
        int maxAttempts
    ) {
    }

    public record RetryEventRecord(
        long id,
        String eventId,
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String sourceOffsetJson,
        String rawPayloadJson,
        String failureReason,
        String retryTopic,
        int attemptCount,
        int maxAttempts,
        OffsetDateTime nextAttemptAt,
        String retryStatus
    ) {
    }

    private static class RetryEventRecordRowMapper implements RowMapper<RetryEventRecord> {
        @Override
        public RetryEventRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new RetryEventRecord(
                rs.getLong("id"),
                rs.getString("event_id"),
                rs.getString("source_connector"),
                rs.getString("source_schema"),
                rs.getString("source_table"),
                rs.getString("source_primary_key"),
                rs.getBigDecimal("source_lsn").toBigIntegerExact(),
                rs.getString("source_offset"),
                rs.getString("raw_payload"),
                rs.getString("failure_reason"),
                rs.getString("retry_topic"),
                rs.getInt("attempt_count"),
                rs.getInt("max_attempts"),
                rs.getObject("next_attempt_at", OffsetDateTime.class),
                rs.getString("retry_status")
            );
        }
    }
}
