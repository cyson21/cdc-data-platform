package com.example.cdcplatform.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class DebeziumEnvelopeTest {

    @Test
    void parsesApplicantUpdateEnvelope() {
        DebeziumEnvelope envelope = DebeziumEnvelope.parse("""
            {
              "payload": {
                "before": {
                  "id": "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                  "stage": "APPLIED"
                },
                "after": {
                  "id": "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                  "stage": "SCREENING"
                },
                "source": {
                  "name": "ats-source",
                  "schema": "public",
                  "table": "applicants",
                  "lsn": 88432240
                },
                "op": "u",
                "ts_ms": 1780960123456
              }
            }
            """);

        assertThat(envelope.sourceConnector()).isEqualTo("ats-source");
        assertThat(envelope.sourceSchema()).isEqualTo("public");
        assertThat(envelope.sourceTable()).isEqualTo("applicants");
        assertThat(envelope.sourceLsn()).isEqualTo(new BigInteger("88432240"));
        assertThat(envelope.operation()).isEqualTo("u");
        assertThat(envelope.sourcePrimaryKey()).isEqualTo("19c14e85-9840-7a5a-bb16-3c7f70547d9f");
        assertThat(envelope.afterValue("stage")).contains("SCREENING");
    }

    @Test
    void usesBeforePrimaryKeyForDeleteEnvelope() {
        DebeziumEnvelope envelope = DebeziumEnvelope.parse("""
            {
              "payload": {
                "before": {
                  "id": "1d30dc23-0831-7db1-b6da-454efaa61a88",
                  "stage": "SCREENING"
                },
                "after": null,
                "source": {
                  "name": "ats-source",
                  "schema": "public",
                  "table": "applicants",
                  "lsn": 88432301
                },
                "op": "d",
                "ts_ms": 1780961123456
              }
            }
            """);

        assertThat(envelope.sourcePrimaryKey()).isEqualTo("1d30dc23-0831-7db1-b6da-454efaa61a88");
        assertThat(envelope.eventId().value()).isEqualTo(
            CdcEventId.fromSourceMetadata(
                "ats-source",
                "public",
                "applicants",
                "1d30dc23-0831-7db1-b6da-454efaa61a88",
                new BigInteger("88432301"),
                "d"
            ).value()
        );
    }
}
