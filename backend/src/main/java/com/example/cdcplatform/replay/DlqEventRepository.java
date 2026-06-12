package com.example.cdcplatform.replay;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnBean(DataSource.class)
public class DlqEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public DlqEventRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public Optional<DlqEvent> findById(long id) {
        return jdbcTemplate.query(
            """
                select id, event_id, source_table, source_primary_key, source_lsn, replay_status
                from cdc_dlq_events
                where id = ?
                """,
            resultSet -> {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(new DlqEvent(
                    resultSet.getLong("id"),
                    resultSet.getString("event_id"),
                    resultSet.getString("source_table"),
                    resultSet.getString("source_primary_key"),
                    resultSet.getBigDecimal("source_lsn").toBigIntegerExact(),
                    resultSet.getString("replay_status")
                ));
            },
            id
        );
    }

    public void markReplayRequested(long id) {
        int updated = jdbcTemplate.update(
            """
                update cdc_dlq_events
                set replay_status = 'REPLAY_REQUESTED'
                where id = ?
                """,
            id
        );
        if (updated != 1) {
            throw new IllegalArgumentException("DLQ event not found: " + id);
        }
    }

    public record DlqEvent(
        long id,
        String eventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String replayStatus
    ) {
    }
}
