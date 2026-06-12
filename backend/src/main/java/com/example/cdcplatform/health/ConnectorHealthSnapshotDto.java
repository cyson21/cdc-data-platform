package com.example.cdcplatform.health;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;

public record ConnectorHealthSnapshotDto(
    String connectorName,
    String connectorStatus,
    String taskStatus,
    Long lagMillis,
    JsonNode offsetSummary,
    OffsetDateTime capturedAt
) {
}
