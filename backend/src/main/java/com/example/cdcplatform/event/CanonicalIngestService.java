package com.example.cdcplatform.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;

public class CanonicalIngestService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final CanonicalEventPublisher canonicalEventPublisher;

    public CanonicalIngestService(CanonicalEventPublisher canonicalEventPublisher) {
        this.canonicalEventPublisher = canonicalEventPublisher;
    }

    public CanonicalIngestResult ingestRawEnvelope(String rawEnvelopeJson) {
        DebeziumEnvelope envelope = DebeziumEnvelope.parse(rawEnvelopeJson);
        CanonicalCdcEvent event = CanonicalCdcEvent.fromEnvelope(envelope);
        String sourceOffsetJson = sourceOffsetJson(envelope);
        boolean created = canonicalEventPublisher.recordCanonicalEvent(event, sourceOffsetJson);
        return new CanonicalIngestResult(
            event.eventId().value(),
            event.sourceTable(),
            event.sourcePrimaryKey(),
            event.sourceLsn(),
            sourceOffsetJson,
            event.operation(),
            created,
            created ? 1 : 0,
            created ? 0 : 1
        );
    }

    private String sourceOffsetJson(DebeziumEnvelope envelope) {
        try {
            return OBJECT_MAPPER.writeValueAsString(envelope.sourceOffset());
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Debezium source offset is not JSON serializable", exc);
        }
    }

    public record CanonicalIngestResult(
        String eventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String sourceOffsetJson,
        String operation,
        boolean created,
        int canonicalEventCount,
        int duplicateEventCount
    ) {
    }
}
