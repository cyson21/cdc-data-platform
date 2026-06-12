package com.example.cdcplatform.event;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

public record CdcEventId(String value) {

    public CdcEventId {
        requireText(value, "value");
    }

    public static CdcEventId fromSourceMetadata(
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String operation
    ) {
        String material = String.join(
            "\n",
            requireText(sourceConnector, "sourceConnector"),
            requireText(sourceSchema, "sourceSchema"),
            requireText(sourceTable, "sourceTable"),
            requireText(sourcePrimaryKey, "sourcePrimaryKey"),
            Objects.requireNonNull(sourceLsn, "sourceLsn").toString(),
            requireText(operation, "operation")
        );
        return new CdcEventId("cdc_" + sha256Hex(material).substring(0, 40));
    }

    private static String sha256Hex(String material) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(material.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exc) {
            throw new IllegalStateException("SHA-256 digest is unavailable", exc);
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
