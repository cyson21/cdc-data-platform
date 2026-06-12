package com.example.cdcplatform.quality;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;
import java.util.List;

public record PipelineQualitySlaRecordDto(
    long id,
    String checkName,
    String sourceTable,
    long sourceRowCount,
    long rawEventCount,
    long canonicalEventCount,
    long duplicateEventCount,
    long missingEventCount,
    double completenessRatio,
    double duplicateRatio,
    long freshnessLagMillis,
    long lagMillis,
    double minCompletenessRatio,
    double maxDuplicateRatio,
    long maxFreshnessLagMillis,
    long maxLagMillis,
    String slaStatus,
    List<String> violatedSloIds,
    List<String> warningSloIds,
    JsonNode details,
    OffsetDateTime evaluatedAt
) {
}
