package com.example.cdcplatform.quality;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;

public record PipelineQualityCheckDto(
    long id,
    String checkName,
    String sourceTable,
    long sourceRowCount,
    long rawEventCount,
    long canonicalEventCount,
    long duplicateEventCount,
    long missingEventCount,
    String checkStatus,
    JsonNode details,
    OffsetDateTime checkedAt
) {
}
