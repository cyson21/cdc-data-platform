package com.example.cdcplatform.ledger;

import com.example.cdcplatform.event.CdcEventId;
import java.math.BigDecimal;
import java.math.BigInteger;
import javax.sql.DataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

public class CdcEventLedgerRepository {

    private final JdbcTemplate jdbcTemplate;

    public CdcEventLedgerRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public boolean insertIfAbsent(LedgerEvent event) {
        try {
            jdbcTemplate.update(
                """
                    insert into cdc_event_ledger(
                        event_id,
                        source_connector,
                        source_schema,
                        source_table,
                        source_primary_key,
                        source_lsn,
                        source_offset,
                        operation,
                        processed_status
                    )
                    values (?, ?, ?, ?, ?, ?, cast(? as jsonb), ?, ?)
                    """,
                event.eventId().value(),
                event.sourceConnector(),
                event.sourceSchema(),
                event.sourceTable(),
                event.sourcePrimaryKey(),
                new BigDecimal(event.sourceLsn()),
                event.sourceOffsetJson(),
                event.operation(),
                event.processedStatus()
            );
            return true;
        } catch (DuplicateKeyException exc) {
            return false;
        }
    }

    public record LedgerEvent(
        CdcEventId eventId,
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String sourceOffsetJson,
        String operation,
        String processedStatus
    ) {
    }
}
