package com.example.cdcplatform.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.event.CdcEventId;
import java.math.BigInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class CdcEventLedgerRepositoryTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static JdbcTemplate jdbcTemplate;
    private static CdcEventLedgerRepository repository;

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
        jdbcTemplate = new JdbcTemplate(dataSource);
        repository = new CdcEventLedgerRepository(dataSource);
    }

    @Test
    void insertIfAbsentReturnsFalseForDuplicateSourceEvent() {
        CdcEventLedgerRepository.LedgerEvent event = new CdcEventLedgerRepository.LedgerEvent(
            CdcEventId.fromSourceMetadata(
                "ats-source",
                "public",
                "applicants",
                "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                new BigInteger("88432240"),
                "u"
            ),
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432240"),
            "{\"lsn\":88432240,\"snapshot\":false}",
            "u",
            "PROCESSED"
        );

        assertThat(repository.insertIfAbsent(event)).isTrue();
        assertThat(repository.insertIfAbsent(event)).isFalse();

        Integer rowCount = jdbcTemplate.queryForObject(
            "select count(*) from cdc_event_ledger where event_id = ?",
            Integer.class,
            event.eventId().value()
        );
        assertThat(rowCount).isOne();
    }
}
