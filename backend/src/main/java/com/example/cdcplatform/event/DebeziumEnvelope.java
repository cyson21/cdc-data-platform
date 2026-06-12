package com.example.cdcplatform.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public record DebeziumEnvelope(
    Map<String, Object> before,
    Map<String, Object> after,
    Source source,
    String operation,
    Long tsMs
) {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    public static DebeziumEnvelope parse(String json) {
        try {
            Map<String, Object> root = OBJECT_MAPPER.readValue(json, MAP_TYPE);
            Map<String, Object> envelope = objectMap(root.getOrDefault("payload", root), "payload");
            return fromMap(envelope);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Debezium envelope JSON is invalid", exc);
        }
    }

    public static DebeziumEnvelope fromMap(Map<String, Object> envelope) {
        Map<String, Object> source = objectMap(envelope.get("source"), "source");
        return new DebeziumEnvelope(
            nullableObjectMap(envelope.get("before")),
            nullableObjectMap(envelope.get("after")),
            Source.fromMap(source),
            text(envelope.get("op"), "op"),
            optionalLong(envelope.get("ts_ms"))
        );
    }

    public String sourceConnector() {
        return source.name();
    }

    public String sourceSchema() {
        return source.schema();
    }

    public String sourceTable() {
        return source.table();
    }

    public BigInteger sourceLsn() {
        return source.lsn();
    }

    public Map<String, Object> sourceOffset() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("lsn", source.lsn());
        source.sequence().ifPresent(value -> offset.put("sequence", value));
        source.txId().ifPresent(value -> offset.put("txId", value));
        source.snapshot().ifPresent(value -> offset.put("snapshot", value));
        return offset;
    }

    public String sourcePrimaryKey() {
        Object id = after != null ? after.get("id") : null;
        if (id == null && before != null) {
            id = before.get("id");
        }
        return text(id, "id");
    }

    public Optional<String> afterValue(String fieldName) {
        if (after == null || !after.containsKey(fieldName)) {
            return Optional.empty();
        }
        return Optional.ofNullable(after.get(fieldName)).map(String::valueOf);
    }

    public CdcEventId eventId() {
        return CdcEventId.fromSourceMetadata(
            sourceConnector(),
            sourceSchema(),
            sourceTable(),
            sourcePrimaryKey(),
            sourceLsn(),
            operation
        );
    }

    private static Map<String, Object> nullableObjectMap(Object value) {
        if (value == null) {
            return null;
        }
        return objectMap(value, "object");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectMap(Object value, String fieldName) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException(fieldName + " must be an object");
    }

    private static String text(Object value, String fieldName) {
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return String.valueOf(value);
    }

    private static Long optionalLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    public record Source(
        String name,
        String schema,
        String table,
        BigInteger lsn,
        Optional<Object> sequence,
        Optional<BigInteger> txId,
        Optional<Object> snapshot
    ) {

        public static Source fromMap(Map<String, Object> source) {
            return new Source(
                text(source.get("name"), "source.name"),
                text(source.get("schema"), "source.schema"),
                text(source.get("table"), "source.table"),
                bigInteger(source.get("lsn"), "source.lsn"),
                Optional.ofNullable(source.get("sequence")),
                optionalBigInteger(source.get("txId")),
                Optional.ofNullable(source.get("snapshot"))
            );
        }

        private static BigInteger bigInteger(Object value, String fieldName) {
            if (value == null) {
                throw new IllegalArgumentException(fieldName + " must not be null");
            }
            if (value instanceof BigInteger bigInteger) {
                return bigInteger;
            }
            if (value instanceof Number number) {
                return BigInteger.valueOf(number.longValue());
            }
            return new BigInteger(String.valueOf(value));
        }

        private static Optional<BigInteger> optionalBigInteger(Object value) {
            if (value == null) {
                return Optional.empty();
            }
            return Optional.of(bigInteger(value, "source.txId"));
        }
    }
}
