package com.example.cdcplatform.lakehouse;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.event.CanonicalCdcEvent;
import com.example.cdcplatform.event.DebeziumEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalLakehouseObjectSinkTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Path DEFAULT_OUTPUT_ROOT = Path.of("target", "lakehouse-object-sink-smoke");
    private static final Path EVIDENCE_FILE = Path.of("target", "lakehouse-object-sink-evidence.json");
    private static final String BUCKET = "cdc-lakehouse";

    @BeforeEach
    void cleanGeneratedEvidence() throws Exception {
        deleteRecursively(outputRoot());
        Files.deleteIfExists(EVIDENCE_FILE);
    }

    @Test
    void writesCanonicalEventAsS3CompatibleJsonLineWithSourceMetadata() throws Exception {
        DebeziumEnvelope envelope = DebeziumEnvelope.parse(rawApplicantEnvelope());
        CanonicalCdcEvent canonicalEvent = CanonicalCdcEvent.fromEnvelope(envelope);
        String sourceOffsetJson = OBJECT_MAPPER.writeValueAsString(envelope.sourceOffset());
        LocalLakehouseObjectSink sink = new LocalLakehouseObjectSink(outputRoot(), BUCKET);

        LocalLakehouseObjectSink.WriteResult result = sink.write(
            LocalLakehouseObjectSink.CuratedEvent.fromCanonicalEvent(
                canonicalEvent,
                sourceOffsetJson,
                LocalDate.parse("2026-06-09")
            )
        );

        assertThat(result.bucket()).isEqualTo(BUCKET);
        assertThat(result.objectKey()).isEqualTo("curated/hiring_events/event_date=2026-06-09/part-0001.jsonl");
        assertThat(result.s3CompatibleUri()).isEqualTo(
            "s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl"
        );
        assertThat(result.objectEventCount()).isOne();
        assertThat(result.localObjectPath()).exists();

        JsonNode writtenEvent = OBJECT_MAPPER.readTree(Files.readString(result.localObjectPath()).trim());
        assertThat(writtenEvent.get("eventId").asText()).isEqualTo(canonicalEvent.eventId().value());
        assertThat(writtenEvent.get("eventId").asText()).startsWith("cdc_");
        assertThat(writtenEvent.get("sourceConnector").asText()).isEqualTo("cdc.raw");
        assertThat(writtenEvent.get("sourceLsn").bigIntegerValue()).isEqualTo(new BigInteger("88432240"));
        assertThat(writtenEvent.get("eventSourceOffset").get("lsn").asLong()).isEqualTo(88432240L);
        assertThat(writtenEvent.get("eventSourceOffset").get("txId").asLong()).isEqualTo(773L);
        assertThat(writtenEvent.get("eventSourceOffset").get("sequence").get(0).asText()).isEqualTo("26740416");
        assertThat(writtenEvent.get("operation").asText()).isEqualTo("u");
        assertThat(writtenEvent.get("after").get("stage").asText()).isEqualTo("SCREENING");

        writeEvidence(result, writtenEvent);
    }

    private static Path outputRoot() {
        String configured = System.getProperty("lakehouse.object.outputDir");
        if (configured == null || configured.isBlank()) {
            return DEFAULT_OUTPUT_ROOT;
        }
        return Path.of(configured);
    }

    private static String rawApplicantEnvelope() {
        return """
            {
              "before": {
                "id": "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                "stage": "APPLIED"
              },
              "after": {
                "id": "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                "stage": "SCREENING"
              },
              "source": {
                "name": "cdc.raw",
                "schema": "public",
                "table": "applicants",
                "sequence": ["26740416", "26740416"],
                "txId": 773,
                "lsn": 88432240
              },
              "op": "u",
              "ts_ms": 1780975060707
            }
            """;
    }

    private static void writeEvidence(LocalLakehouseObjectSink.WriteResult result, JsonNode writtenEvent) throws Exception {
        Files.createDirectories(EVIDENCE_FILE.getParent());
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("scenario", "canonical-object-sink");
        evidence.put("evidenceLabel", "local-s3-compatible-backend");
        evidence.put("bucket", result.bucket());
        evidence.put("objectKey", result.objectKey());
        evidence.put("s3CompatibleUri", result.s3CompatibleUri());
        evidence.put("localObjectPath", result.localObjectPath().toString());
        evidence.put("objectEventCount", result.objectEventCount());
        evidence.put("eventId", writtenEvent.get("eventId").asText());
        evidence.put("sourceLsn", writtenEvent.get("sourceLsn").bigIntegerValue());
        evidence.put("eventSourceOffset", OBJECT_MAPPER.convertValue(writtenEvent.get("eventSourceOffset"), Map.class));
        evidence.put("operation", writtenEvent.get("operation").asText());
        evidence.put("testResult", "passed");
        Files.writeString(EVIDENCE_FILE, OBJECT_MAPPER.writeValueAsString(evidence));
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            for (Path current : stream.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(current);
            }
        }
    }
}
