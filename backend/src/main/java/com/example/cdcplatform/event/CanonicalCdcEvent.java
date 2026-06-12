package com.example.cdcplatform.event;

import java.math.BigInteger;
import java.util.Map;

public record CanonicalCdcEvent(
    CdcEventId eventId,
    String sourceConnector,
    String sourceSchema,
    String sourceTable,
    String sourcePrimaryKey,
    BigInteger sourceLsn,
    String operation,
    Map<String, Object> before,
    Map<String, Object> after
) {

    public static CanonicalCdcEvent fromEnvelope(DebeziumEnvelope envelope) {
        return new CanonicalCdcEvent(
            envelope.eventId(),
            envelope.sourceConnector(),
            envelope.sourceSchema(),
            envelope.sourceTable(),
            envelope.sourcePrimaryKey(),
            envelope.sourceLsn(),
            envelope.operation(),
            envelope.before(),
            envelope.after()
        );
    }
}
