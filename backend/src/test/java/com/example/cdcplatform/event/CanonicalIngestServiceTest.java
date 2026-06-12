package com.example.cdcplatform.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.cdcplatform.ledger.CdcEventLedgerRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class CanonicalIngestServiceTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void ingestsRawDebeziumEnvelopeIntoCanonicalLedgerRecord() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher(true);
        CanonicalIngestService service = new CanonicalIngestService(publisher);

        CanonicalIngestService.CanonicalIngestResult result = service.ingestRawEnvelope(rawApplicantEnvelope());

        assertThat(result.created()).isTrue();
        assertThat(result.canonicalEventCount()).isOne();
        assertThat(result.duplicateEventCount()).isZero();
        assertThat(result.sourceTable()).isEqualTo("applicants");
        assertThat(result.sourcePrimaryKey()).isEqualTo("19c14e85-9840-7a5a-bb16-3c7f70547d9f");
        assertThat(result.sourceLsn()).isEqualTo(new BigInteger("88432240"));
        assertThat(result.eventId()).startsWith("cdc_");
        assertThat(OBJECT_MAPPER.readTree(result.sourceOffsetJson()).get("lsn").asLong()).isEqualTo(88432240L);
        assertThat(OBJECT_MAPPER.readTree(result.sourceOffsetJson()).get("txId").asLong()).isEqualTo(773L);
        assertThat(OBJECT_MAPPER.readTree(result.sourceOffsetJson()).get("sequence").get(0).asText()).isEqualTo("26740416");

        assertThat(publisher.event.sourceConnector()).isEqualTo("cdc.raw");
        assertThat(publisher.event.operation()).isEqualTo("u");
        assertThat(OBJECT_MAPPER.readTree(publisher.sourceOffsetJson).get("lsn").asLong()).isEqualTo(88432240L);
    }

    @Test
    void duplicateRawEnvelopeDoesNotIncreaseCanonicalEventCount() {
        CanonicalIngestService service = new CanonicalIngestService(new CapturingPublisher(false));

        CanonicalIngestService.CanonicalIngestResult result = service.ingestRawEnvelope(rawApplicantEnvelope());

        assertThat(result.created()).isFalse();
        assertThat(result.canonicalEventCount()).isZero();
        assertThat(result.duplicateEventCount()).isOne();
        assertThat(result.eventId()).startsWith("cdc_");
        assertThat(result.sourceLsn()).isEqualTo(new BigInteger("88432240"));
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

    private static class CapturingPublisher extends CanonicalEventPublisher {

        private final boolean created;
        private CanonicalCdcEvent event;
        private String sourceOffsetJson;

        CapturingPublisher(boolean created) {
            super(null);
            this.created = created;
        }

        @Override
        public boolean recordCanonicalEvent(CanonicalCdcEvent event, String sourceOffsetJson) {
            this.event = event;
            this.sourceOffsetJson = sourceOffsetJson;
            return created;
        }
    }
}
