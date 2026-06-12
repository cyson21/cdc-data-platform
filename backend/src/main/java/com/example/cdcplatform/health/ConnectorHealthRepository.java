package com.example.cdcplatform.health;

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
public class ConnectorHealthRepository {

    private final JdbcTemplate jdbcTemplate;

    public ConnectorHealthRepository(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public Optional<ConnectorHealthSnapshot> findLatestByConnectorName(String connectorName) {
        try {
            ConnectorHealthSnapshot snapshot = jdbcTemplate.queryForObject(
                """
                    select
                        connector_name,
                        connector_status,
                        task_status,
                        lag_millis,
                        offset_summary::text,
                        captured_at
                    from connector_health_snapshots
                    where connector_name = ?
                    order by captured_at desc
                    limit 1
                    """,
                new ConnectorHealthSnapshotRowMapper(),
                connectorName
            );
            return Optional.ofNullable(snapshot);
        } catch (EmptyResultDataAccessException exc) {
            return Optional.empty();
        }
    }

    public record ConnectorHealthSnapshot(
        String connectorName,
        String connectorStatus,
        String taskStatus,
        Long lagMillis,
        String offsetSummaryJson,
        OffsetDateTime capturedAt
    ) {
    }

    private static class ConnectorHealthSnapshotRowMapper implements RowMapper<ConnectorHealthSnapshot> {
        @Override
        public ConnectorHealthSnapshot mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new ConnectorHealthSnapshot(
                rs.getString("connector_name"),
                rs.getString("connector_status"),
                rs.getString("task_status"),
                rs.getObject("lag_millis", Long.class),
                rs.getString("offset_summary"),
                rs.getObject("captured_at", OffsetDateTime.class)
            );
        }
    }
}
