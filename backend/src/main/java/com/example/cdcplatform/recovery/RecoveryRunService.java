package com.example.cdcplatform.recovery;

import com.example.cdcplatform.recovery.RecoveryRunRepository.RecoveryRun;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(RecoveryRunRepository.class)
public class RecoveryRunService {

    private final RecoveryRunRepository recoveryRunRepository;

    public RecoveryRunService(RecoveryRunRepository recoveryRunRepository) {
        this.recoveryRunRepository = recoveryRunRepository;
    }

    public RecoveryRunResult recordRecovery(RecordRecoveryCommand command) {
        RecoveryRun candidate = new RecoveryRun(
            command.recoveryRunId(),
            command.scenario(),
            command.connectorName(),
            command.sourceTable(),
            command.sourcePrimaryKey(),
            command.sourceLsnFrom(),
            command.sourceLsnTo(),
            command.sourceOffsetFromJson(),
            command.sourceOffsetToJson(),
            command.eventId(),
            command.recoveryStatus(),
            command.recoveredEventCount(),
            command.duplicateEventCount(),
            command.gapDetected(),
            command.detailsJson(),
            command.outageStartedAt(),
            command.recoveredAt(),
            null
        );
        boolean created = recoveryRunRepository.insertIfAbsent(candidate);
        RecoveryRun recoveryRun = recoveryRunRepository
            .findLatestByScenarioAndConnector(command.scenario(), command.connectorName())
            .orElse(candidate);
        return RecoveryRunResult.from(created, recoveryRun);
    }

    public record RecordRecoveryCommand(
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
        OffsetDateTime recoveredAt
    ) {
    }

    public record RecoveryRunResult(
        boolean created,
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
        OffsetDateTime outageStartedAt,
        OffsetDateTime recoveredAt
    ) {
        static RecoveryRunResult from(boolean created, RecoveryRun recoveryRun) {
            return new RecoveryRunResult(
                created,
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
                recoveryRun.outageStartedAt(),
                recoveryRun.recoveredAt()
            );
        }
    }
}
