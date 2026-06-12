package com.example.cdcplatform.lakehouse;

import com.example.cdcplatform.event.CanonicalCdcEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Map;
import java.util.Objects;

public class LocalLakehouseObjectSink {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TABLE_NAME = "hiring_events";

    private final Path outputRoot;
    private final String bucket;

    public LocalLakehouseObjectSink(Path outputRoot, String bucket) {
        this.outputRoot = Objects.requireNonNull(outputRoot, "outputRoot");
        this.bucket = requireText(bucket, "bucket");
    }

    public WriteResult write(CuratedEvent event) {
        Objects.requireNonNull(event, "event");
        String objectKey = objectKey(event.eventDate());
        Path objectPath = outputRoot.resolve(bucket).resolve(objectKey);
        try {
            Files.createDirectories(objectPath.getParent());
            String line = OBJECT_MAPPER.writeValueAsString(event) + "\n";
            Files.writeString(
                objectPath,
                line,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
            long objectEventCount;
            try (var lines = Files.lines(objectPath)) {
                objectEventCount = lines.count();
            }
            return new WriteResult(
                bucket,
                objectKey,
                "s3://" + bucket + "/" + objectKey,
                objectPath,
                objectEventCount
            );
        } catch (IOException exc) {
            throw new IllegalStateException("Lakehouse object write failed", exc);
        }
    }

    private static String objectKey(String eventDate) {
        return "curated/" + TABLE_NAME + "/event_date=" + eventDate + "/part-0001.jsonl";
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static JsonNode parseSourceOffset(String sourceOffsetJson) {
        try {
            return OBJECT_MAPPER.readTree(requireText(sourceOffsetJson, "sourceOffsetJson"));
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("sourceOffsetJson must be valid JSON", exc);
        }
    }

    public record CuratedEvent(
        String eventId,
        String eventDate,
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        JsonNode eventSourceOffset,
        String operation,
        Map<String, Object> before,
        Map<String, Object> after
    ) {

        public static CuratedEvent fromCanonicalEvent(
            CanonicalCdcEvent event,
            String sourceOffsetJson,
            LocalDate eventDate
        ) {
            Objects.requireNonNull(event, "event");
            return new CuratedEvent(
                event.eventId().value(),
                Objects.requireNonNull(eventDate, "eventDate").toString(),
                event.sourceConnector(),
                event.sourceSchema(),
                event.sourceTable(),
                event.sourcePrimaryKey(),
                event.sourceLsn(),
                parseSourceOffset(sourceOffsetJson),
                event.operation(),
                event.before(),
                event.after()
            );
        }
    }

    public record WriteResult(
        String bucket,
        String objectKey,
        String s3CompatibleUri,
        Path localObjectPath,
        long objectEventCount
    ) {
    }
}
