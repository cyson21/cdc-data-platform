package com.example.cdcplatform.resilience;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.event.CdcEventId;
import com.example.cdcplatform.recovery.RecoveryRunRepository;
import com.example.cdcplatform.recovery.RecoveryRunService;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RecoveryRunServiceTest {

    private static final String SOURCE_CONNECTOR = "ats-source";
    private static final String SOURCE_SCHEMA = "public";
    private static final String SOURCE_TABLE = "applicants";
    private static final String SOURCE_PRIMARY_KEY = "19c14e85-9840-7a5a-bb16-3c7f70547d9f";
    private static final String OFFSET_FROM_JSON = "{\"file\":\"000000010000000000000058\",\"pos\":32240,\"lsn\":88432240}";
    private static final String OFFSET_TO_JSON = "{\"file\":\"000000010000000000000058\",\"pos\":32288,\"lsn\":88432288}";
    private static final BigInteger LSN_FROM = new BigInteger("88432240");
    private static final BigInteger LSN_TO = new BigInteger("88432288");
    private static final String DETAILS_JSON = "{\"rawTopic\":\"cdc.raw.public.applicants\",\"recoveredBy\":\"local-control-plane\"}";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    private static RecoveryRunService recoveryRunService;

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
        recoveryRunService = new RecoveryRunService(new RecoveryRunRepository(dataSource));
    }

    @Test
    void kafkaOutageRecoveryIsIdempotentBySourceMetadataBoundary() {
        CdcEventId eventId = CdcEventId.fromSourceMetadata(
            SOURCE_CONNECTOR,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            LSN_TO,
            "u"
        );

        RecoveryRunService.RecoveryRunResult first = recoveryRunService.recordRecovery(command(
            "recovery-kafka-001",
            "KAFKA_OUTAGE",
            eventId.value()
        ));
        RecoveryRunService.RecoveryRunResult duplicate = recoveryRunService.recordRecovery(command(
            "recovery-kafka-duplicate",
            "KAFKA_OUTAGE",
            eventId.value()
        ));

        assertThat(first.created()).isTrue();
        assertThat(duplicate.created()).isFalse();
        assertThat(duplicate.scenario()).isEqualTo("KAFKA_OUTAGE");
        assertThat(duplicate.connectorName()).isEqualTo(SOURCE_CONNECTOR);
        assertThat(duplicate.sourceLsnFrom()).isEqualTo(LSN_FROM);
        assertThat(duplicate.sourceLsnTo()).isEqualTo(LSN_TO);
        assertThat(duplicate.sourceOffsetFromJson()).contains("\"lsn\": 88432240");
        assertThat(duplicate.sourceOffsetToJson()).contains("\"lsn\": 88432288");
        assertThat(duplicate.eventId()).isEqualTo(eventId.value());
        assertThat(duplicate.recoveryStatus()).isEqualTo("RECOVERED");
        assertThat(duplicate.recoveredEventCount()).isEqualTo(3);
        assertThat(duplicate.duplicateEventCount()).isOne();
        assertThat(duplicate.gapDetected()).isFalse();
    }

    @Test
    void connectorRestartRecoveryKeepsSourceOffsetBoundary() {
        CdcEventId eventId = CdcEventId.fromSourceMetadata(
            SOURCE_CONNECTOR,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            new BigInteger("88432320"),
            "r"
        );

        RecoveryRunService.RecoveryRunResult result = recoveryRunService.recordRecovery(new RecoveryRunService.RecordRecoveryCommand(
            "recovery-connect-001",
            "CONNECTOR_RESTART",
            SOURCE_CONNECTOR,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            new BigInteger("88432288"),
            new BigInteger("88432320"),
            OFFSET_TO_JSON,
            "{\"file\":\"000000010000000000000058\",\"pos\":32320,\"lsn\":88432320}",
            eventId.value(),
            "RECOVERED",
            1,
            0,
            false,
            "{\"connectorStatus\":\"RUNNING\",\"taskStatus\":\"RUNNING\"}",
            OffsetDateTime.parse("2026-06-09T10:05:00Z"),
            OffsetDateTime.parse("2026-06-09T10:06:30Z")
        ));

        assertThat(result.created()).isTrue();
        assertThat(result.scenario()).isEqualTo("CONNECTOR_RESTART");
        assertThat(result.sourceLsnTo()).isEqualTo(new BigInteger("88432320"));
        assertThat(result.sourceOffsetToJson()).contains("\"lsn\": 88432320");
        assertThat(result.eventId()).isEqualTo(eventId.value());
        assertThat(result.recoveryStatus()).isEqualTo("RECOVERED");
    }

    private RecoveryRunService.RecordRecoveryCommand command(String recoveryRunId, String scenario, String eventId) {
        return new RecoveryRunService.RecordRecoveryCommand(
            recoveryRunId,
            scenario,
            SOURCE_CONNECTOR,
            SOURCE_TABLE,
            SOURCE_PRIMARY_KEY,
            LSN_FROM,
            LSN_TO,
            OFFSET_FROM_JSON,
            OFFSET_TO_JSON,
            eventId,
            "RECOVERED",
            3,
            1,
            false,
            DETAILS_JSON,
            OffsetDateTime.parse("2026-06-09T10:00:00Z"),
            OffsetDateTime.parse("2026-06-09T10:03:00Z")
        );
    }
}
