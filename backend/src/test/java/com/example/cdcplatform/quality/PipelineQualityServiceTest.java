package com.example.cdcplatform.quality;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.cdcplatform.quality.PipelineQualityCheckRepository.NewQualityCheck;
import com.example.cdcplatform.quality.PipelineQualityCheckRepository.QualityCheckRecord;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository.NewSlaEvaluation;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository.SlaEvaluationRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PipelineQualityServiceTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final OffsetDateTime CHECKED_AT = OffsetDateTime.parse("2026-06-09T04:40:00Z");

    @Test
    void recordsPassingCompletenessCheckWithSourceMetadataDetails() throws Exception {
        PipelineQualityCheckRepository repository = mock(PipelineQualityCheckRepository.class);
        when(repository.insert(any())).thenAnswer(invocation -> recordFrom(invocation.getArgument(0)));
        PipelineQualityService service = new PipelineQualityService(repository);

        PipelineQualityCheckDto dto = service.recordQualityCheck(new PipelineQualityService.RecordQualityCheckCommand(
            "applicant-cdc-completeness",
            "applicants",
            3,
            3,
            3,
            0,
            0,
            """
                {
                  "sourceLsnFrom": 88432240,
                  "sourceLsnTo": 88432288,
                  "eventIds": ["cdc_evt_1", "cdc_evt_2", "cdc_evt_3"]
                }
                """
        ));

        assertThat(dto.checkStatus()).isEqualTo("PASSED");
        assertThat(dto.sourceTable()).isEqualTo("applicants");
        assertThat(dto.sourceRowCount()).isEqualTo(3);
        assertThat(dto.rawEventCount()).isEqualTo(3);
        assertThat(dto.canonicalEventCount()).isEqualTo(3);
        assertThat(dto.missingEventCount()).isZero();
        assertThat(dto.details().get("sourceLsnTo").asLong()).isEqualTo(88432288L);
        assertThat(dto.checkedAt()).isEqualTo(CHECKED_AT);

        ArgumentCaptor<NewQualityCheck> captor = ArgumentCaptor.forClass(NewQualityCheck.class);
        verify(repository).insert(captor.capture());
        assertThat(captor.getValue().checkStatus()).isEqualTo("PASSED");
        assertThat(OBJECT_MAPPER.readTree(captor.getValue().detailsJson()).get("eventIds")).hasSize(3);
    }

    @Test
    void marksMissingCanonicalEventsAsFailed() {
        PipelineQualityCheckRepository repository = mock(PipelineQualityCheckRepository.class);
        when(repository.insert(any())).thenAnswer(invocation -> recordFrom(invocation.getArgument(0)));
        PipelineQualityService service = new PipelineQualityService(repository);

        PipelineQualityCheckDto dto = service.recordQualityCheck(new PipelineQualityService.RecordQualityCheckCommand(
            "applicant-cdc-completeness",
            "applicants",
            3,
            3,
            2,
            0,
            1,
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432288}"
        ));

        assertThat(dto.checkStatus()).isEqualTo("FAILED");
    }

    @Test
    void marksDuplicateRawEventsAsWarningWhenCompletenessStillHolds() {
        PipelineQualityCheckRepository repository = mock(PipelineQualityCheckRepository.class);
        when(repository.insert(any())).thenAnswer(invocation -> recordFrom(invocation.getArgument(0)));
        PipelineQualityService service = new PipelineQualityService(repository);

        PipelineQualityCheckDto dto = service.recordQualityCheck(new PipelineQualityService.RecordQualityCheckCommand(
            "applicant-cdc-duplicates",
            "applicants",
            3,
            4,
            3,
            1,
            0,
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432288}"
        ));

        assertThat(dto.checkStatus()).isEqualTo("WARN");
    }

    @Test
    void evaluatesQualitySlaFromFreshnessCompletenessDuplicateRateAndLag() {
        PipelineQualityCheckRepository repository = mock(PipelineQualityCheckRepository.class);
        PipelineQualityService service = new PipelineQualityService(repository);

        PipelineQualitySlaDto dto = service.evaluateSla(new PipelineQualityService.EvaluateQualitySlaCommand(
            "applicant-cdc-sla",
            "applicants",
            100,
            102,
            98,
            2,
            2,
            90_000,
            180_000,
            0.99,
            0.01,
            60_000,
            120_000,
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_evt_1\"]}"
        ));

        assertThat(dto.slaStatus()).isEqualTo("FAILED");
        assertThat(dto.checkName()).isEqualTo("applicant-cdc-sla");
        assertThat(dto.sourceTable()).isEqualTo("applicants");
        assertThat(dto.completenessRatio()).isEqualTo(0.98);
        assertThat(dto.duplicateRatio()).isCloseTo(0.0196, within(0.0001));
        assertThat(dto.freshnessLagMillis()).isEqualTo(90_000);
        assertThat(dto.lagMillis()).isEqualTo(180_000);
        assertThat(dto.violatedSloIds()).containsExactly(
            "completeness",
            "duplicate-rate",
            "freshness",
            "lag"
        );
        assertThat(dto.warningSloIds()).isEmpty();
        assertThat(dto.details().get("sourceLsnTo").asLong()).isEqualTo(88432320L);
    }

    @Test
    void recordsSlaEvaluationWithSourceMetadataDetails() {
        PipelineQualityCheckRepository checkRepository = mock(PipelineQualityCheckRepository.class);
        PipelineQualitySlaRepository slaRepository = mock(PipelineQualitySlaRepository.class);
        when(slaRepository.insert(any())).thenAnswer(invocation -> slaRecordFrom(invocation.getArgument(0)));
        PipelineQualityService service = new PipelineQualityService(checkRepository, slaRepository);

        PipelineQualitySlaRecordDto dto = service.recordSlaEvaluation(new PipelineQualityService.EvaluateQualitySlaCommand(
            "applicant-cdc-sla",
            "applicants",
            100,
            102,
            98,
            2,
            2,
            90_000,
            180_000,
            0.99,
            0.01,
            60_000,
            120_000,
            "{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_evt_1\"]}"
        ));

        assertThat(dto.id()).isEqualTo(84L);
        assertThat(dto.slaStatus()).isEqualTo("FAILED");
        assertThat(dto.completenessRatio()).isEqualTo(0.98);
        assertThat(dto.duplicateRatio()).isCloseTo(0.0196, within(0.0001));
        assertThat(dto.violatedSloIds()).containsExactly(
            "completeness",
            "duplicate-rate",
            "freshness",
            "lag"
        );
        assertThat(dto.details().get("sourceLsnTo").asLong()).isEqualTo(88432320L);
        assertThat(dto.evaluatedAt()).isEqualTo(CHECKED_AT);

        ArgumentCaptor<NewSlaEvaluation> captor = ArgumentCaptor.forClass(NewSlaEvaluation.class);
        verify(slaRepository).insert(captor.capture());
        assertThat(captor.getValue().slaStatus()).isEqualTo("FAILED");
        assertThat(captor.getValue().violatedSloIdsJson()).contains("freshness");
        assertThat(captor.getValue().detailsJson()).contains("88432320");
    }

    @Test
    void recordsRuntimeSlaEvaluationWithUnavailableSourceLsnLagWarning() throws Exception {
        PipelineQualityCheckRepository checkRepository = mock(PipelineQualityCheckRepository.class);
        PipelineQualitySlaRepository slaRepository = mock(PipelineQualitySlaRepository.class);
        when(slaRepository.insert(any())).thenAnswer(invocation -> slaRecordFrom(invocation.getArgument(0)));
        PipelineQualityService service = new PipelineQualityService(checkRepository, slaRepository);

        PipelineQualitySlaRecordDto dto = service.recordRuntimeSlaEvaluation(
            new PipelineQualityService.RecordRuntimeSlaEvaluationCommand(
                "applicant-cdc-runtime-sla",
                "applicants",
                3,
                3,
                3,
                0,
                0,
                1.0,
                0.0,
                1_580,
                0,
                1.0,
                0.0,
                600_000,
                1_000_000,
                "WARN",
                "[]",
                "[\"source-lsn-lag-unavailable\"]",
                runtimeDetailsJson()
            )
        );

        assertThat(dto.slaStatus()).isEqualTo("WARN");
        assertThat(dto.warningSloIds()).containsExactly("source-lsn-lag-unavailable");
        assertThat(dto.details().get("scenario").asText()).isEqualTo("quality-sla-runtime");
        assertThat(dto.details().get("eventSourceOffset")).hasSize(3);
        assertThat(dto.details().get("sourceConnectorOffsetLsn").isNull()).isTrue();
        assertThat(dto.details().get("sourceLsnLag").isNull()).isTrue();
        assertThat(dto.details().get("eventIds")).hasSize(3);

        ArgumentCaptor<NewSlaEvaluation> captor = ArgumentCaptor.forClass(NewSlaEvaluation.class);
        verify(slaRepository).insert(captor.capture());
        assertThat(captor.getValue().checkName()).isEqualTo("applicant-cdc-runtime-sla");
        assertThat(captor.getValue().sourceRowCount()).isEqualTo(3);
        assertThat(captor.getValue().canonicalEventCount()).isEqualTo(3);
        assertThat(captor.getValue().slaStatus()).isEqualTo("WARN");
        assertThat(captor.getValue().violatedSloIdsJson()).isEqualTo("[]");
        assertThat(captor.getValue().warningSloIdsJson()).contains("source-lsn-lag-unavailable");
        assertThat(OBJECT_MAPPER.readTree(captor.getValue().detailsJson()).get("sourceLsnTo").asLong())
            .isEqualTo(26711584L);
    }

    private static QualityCheckRecord recordFrom(NewQualityCheck check) {
        return new QualityCheckRecord(
            42L,
            check.checkName(),
            check.sourceTable(),
            check.sourceRowCount(),
            check.rawEventCount(),
            check.canonicalEventCount(),
            check.duplicateEventCount(),
            check.missingEventCount(),
            check.checkStatus(),
            check.detailsJson(),
            CHECKED_AT
        );
    }

    private static SlaEvaluationRecord slaRecordFrom(NewSlaEvaluation evaluation) {
        return new SlaEvaluationRecord(
            84L,
            evaluation.checkName(),
            evaluation.sourceTable(),
            evaluation.sourceRowCount(),
            evaluation.rawEventCount(),
            evaluation.canonicalEventCount(),
            evaluation.duplicateEventCount(),
            evaluation.missingEventCount(),
            evaluation.completenessRatio(),
            evaluation.duplicateRatio(),
            evaluation.freshnessLagMillis(),
            evaluation.lagMillis(),
            evaluation.minCompletenessRatio(),
            evaluation.maxDuplicateRatio(),
            evaluation.maxFreshnessLagMillis(),
            evaluation.maxLagMillis(),
            evaluation.slaStatus(),
            evaluation.violatedSloIdsJson(),
            evaluation.warningSloIdsJson(),
            evaluation.detailsJson(),
            CHECKED_AT
        );
    }

    private static String runtimeDetailsJson() {
        return """
            {
              "scenario": "quality-sla-runtime",
              "evidenceLabel": "local-docker-debezium-runtime-sla",
              "sourcePrimaryKey": "d4a5f7d8-95f6-42c4-9078-5a6172732e29",
              "operations": ["c", "u", "d"],
              "sourceLsn": [26710752, 26711320, 26711584],
              "sourceLsnFrom": 26710752,
              "sourceLsnTo": 26711584,
              "eventSourceOffset": [
                {"lsn": 26710752, "sequence": "[null,\\"26710752\\"]", "txId": 761},
                {"lsn": 26711320, "sequence": "[\\"26711320\\",\\"26711320\\"]", "txId": 762},
                {"lsn": 26711584, "sequence": "[\\"26711584\\",\\"26711584\\"]", "txId": 763}
              ],
              "eventIds": [
                "cdc_70fbac3d3209c50cafdd2c3ad1ae42a6f60fa9b6",
                "cdc_2a42a8cebd4d8b0f1f930a8a23e7ee308c15cffd",
                "cdc_9814e7a323b647eddc2b5a5218843e0037ac9ac1"
              ],
              "sourceOffset": {"offsets": []},
              "sourceConnectorOffsetLsn": null,
              "sourceLsnLag": null
            }
            """;
    }
}
