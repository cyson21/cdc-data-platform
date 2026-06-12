package com.example.cdcplatform.replay;

import java.math.BigDecimal;
import java.math.BigInteger;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnBean(DataSource.class)
public class ReplayRequestRepository {

    private final JdbcTemplate jdbcTemplate;

    public ReplayRequestRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void insert(ReplayRequest request) {
        jdbcTemplate.update(
            """
                insert into cdc_replay_requests(
                    request_id,
                    requested_by,
                    source_table,
                    source_primary_key,
                    source_lsn_from,
                    source_lsn_to,
                    replay_status,
                    result_summary
                )
                values (?, ?, ?, ?, ?, ?, ?, cast(? as jsonb))
                """,
            request.requestId(),
            request.requestedBy(),
            request.sourceTable(),
            request.sourcePrimaryKey(),
            new BigDecimal(request.sourceLsn()),
            new BigDecimal(request.sourceLsn()),
            request.replayStatus(),
            "{\"dlqEventId\":" + request.dlqEventId() + "}"
        );
    }

    public record ReplayRequest(
        String requestId,
        String requestedBy,
        long dlqEventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String replayStatus
    ) {
    }
}
