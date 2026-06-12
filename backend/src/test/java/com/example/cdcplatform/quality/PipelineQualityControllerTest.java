package com.example.cdcplatform.quality;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PipelineQualityController.class)
class PipelineQualityControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private PipelineQualityService pipelineQualityService;

    @Test
    void postQualityCheckReturnsStatusAndSourceMetadataDetails() throws Exception {
        when(pipelineQualityService.recordQualityCheck(any()))
            .thenReturn(new PipelineQualityCheckDto(
                42L,
                "applicant-cdc-completeness",
                "applicants",
                3,
                3,
                3,
                0,
                0,
                "PASSED",
                objectMapper.readTree("{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432288,\"eventIds\":[\"cdc_evt_1\"]}"),
                OffsetDateTime.parse("2026-06-09T04:40:00Z")
            ));

        mockMvc.perform(post("/api/pipeline-quality/checks")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "checkName": "applicant-cdc-completeness",
                      "sourceTable": "applicants",
                      "sourceRowCount": 3,
                      "rawEventCount": 3,
                      "canonicalEventCount": 3,
                      "duplicateEventCount": 0,
                      "missingEventCount": 0,
                      "details": {
                        "sourceLsnFrom": 88432240,
                        "sourceLsnTo": 88432288,
                        "eventIds": ["cdc_evt_1"]
                      }
                    }
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.checkName").value("applicant-cdc-completeness"))
            .andExpect(jsonPath("$.sourceTable").value("applicants"))
            .andExpect(jsonPath("$.checkStatus").value("PASSED"))
            .andExpect(jsonPath("$.details.sourceLsnFrom").value(88432240))
            .andExpect(jsonPath("$.details.sourceLsnTo").value(88432288))
            .andExpect(jsonPath("$.checkedAt").value("2026-06-09T04:40:00Z"));
    }

    @Test
    void postQualitySlaEvaluationReturnsSloStatusAndRates() throws Exception {
        when(pipelineQualityService.evaluateSla(any()))
            .thenReturn(new PipelineQualitySlaDto(
                "applicant-cdc-sla",
                "applicants",
                100,
                102,
                98,
                2,
                2,
                0.98,
                0.0196,
                90_000,
                180_000,
                0.99,
                0.01,
                60_000,
                120_000,
                "FAILED",
                java.util.List.of("completeness", "duplicate-rate", "freshness", "lag"),
                java.util.List.of(),
                objectMapper.readTree("{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_evt_1\"]}")
            ));

        mockMvc.perform(post("/api/pipeline-quality/sla/evaluations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "checkName": "applicant-cdc-sla",
                      "sourceTable": "applicants",
                      "sourceRowCount": 100,
                      "rawEventCount": 102,
                      "canonicalEventCount": 98,
                      "duplicateEventCount": 2,
                      "missingEventCount": 2,
                      "freshnessLagMillis": 90000,
                      "lagMillis": 180000,
                      "minCompletenessRatio": 0.99,
                      "maxDuplicateRatio": 0.01,
                      "maxFreshnessLagMillis": 60000,
                      "maxLagMillis": 120000,
                      "details": {
                        "sourceLsnFrom": 88432240,
                        "sourceLsnTo": 88432320,
                        "eventIds": ["cdc_evt_1"]
                      }
                    }
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.checkName").value("applicant-cdc-sla"))
            .andExpect(jsonPath("$.sourceTable").value("applicants"))
            .andExpect(jsonPath("$.slaStatus").value("FAILED"))
            .andExpect(jsonPath("$.completenessRatio").value(0.98))
            .andExpect(jsonPath("$.duplicateRatio").value(0.0196))
            .andExpect(jsonPath("$.freshnessLagMillis").value(90000))
            .andExpect(jsonPath("$.lagMillis").value(180000))
            .andExpect(jsonPath("$.violatedSloIds[0]").value("completeness"))
            .andExpect(jsonPath("$.violatedSloIds[3]").value("lag"))
            .andExpect(jsonPath("$.details.sourceLsnTo").value(88432320));
    }

    @Test
    void postQualitySlaRecordReturnsPersistedEvaluation() throws Exception {
        when(pipelineQualityService.recordSlaEvaluation(any()))
            .thenReturn(new PipelineQualitySlaRecordDto(
                84L,
                "applicant-cdc-sla",
                "applicants",
                100,
                102,
                98,
                2,
                2,
                0.98,
                0.0196,
                90_000,
                180_000,
                0.99,
                0.01,
                60_000,
                120_000,
                "FAILED",
                java.util.List.of("completeness", "duplicate-rate", "freshness", "lag"),
                java.util.List.of(),
                objectMapper.readTree("{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_evt_1\"]}"),
                OffsetDateTime.parse("2026-06-09T04:40:00Z")
            ));

        mockMvc.perform(post("/api/pipeline-quality/sla/evaluations/records")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "checkName": "applicant-cdc-sla",
                      "sourceTable": "applicants",
                      "sourceRowCount": 100,
                      "rawEventCount": 102,
                      "canonicalEventCount": 98,
                      "duplicateEventCount": 2,
                      "missingEventCount": 2,
                      "freshnessLagMillis": 90000,
                      "lagMillis": 180000,
                      "minCompletenessRatio": 0.99,
                      "maxDuplicateRatio": 0.01,
                      "maxFreshnessLagMillis": 60000,
                      "maxLagMillis": 120000,
                      "details": {
                        "sourceLsnFrom": 88432240,
                        "sourceLsnTo": 88432320,
                        "eventIds": ["cdc_evt_1"]
                      }
                    }
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(84))
            .andExpect(jsonPath("$.slaStatus").value("FAILED"))
            .andExpect(jsonPath("$.violatedSloIds[2]").value("freshness"))
            .andExpect(jsonPath("$.details.sourceLsnTo").value(88432320))
            .andExpect(jsonPath("$.evaluatedAt").value("2026-06-09T04:40:00Z"));
    }

    @Test
    void postRuntimeQualitySlaRecordReturnsPersistedEvaluation() throws Exception {
        when(pipelineQualityService.recordRuntimeSlaEvaluation(any()))
            .thenReturn(new PipelineQualitySlaRecordDto(
                85L,
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
                java.util.List.of(),
                java.util.List.of("source-lsn-lag-unavailable"),
                objectMapper.readTree("""
                    {
                      "scenario": "quality-sla-runtime",
                      "evidenceLabel": "local-docker-debezium-runtime-sla",
                      "sourceLsnFrom": 26710752,
                      "sourceLsnTo": 26711584,
                      "sourceConnectorOffsetLsn": null,
                      "sourceLsnLag": null,
                      "eventSourceOffset": [
                        {"lsn": 26710752, "sequence": "[null,\\"26710752\\"]", "txId": 761}
                      ],
                      "eventIds": ["cdc_70fbac3d3209c50cafdd2c3ad1ae42a6f60fa9b6"]
                    }
                    """),
                OffsetDateTime.parse("2026-06-09T04:40:00Z")
            ));

        mockMvc.perform(post("/api/pipeline-quality/sla/runtime-evaluations/records")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "checkName": "applicant-cdc-runtime-sla",
                      "sourceTable": "applicants",
                      "sourceRowCount": 3,
                      "rawEventCount": 3,
                      "canonicalEventCount": 3,
                      "duplicateEventCount": 0,
                      "missingEventCount": 0,
                      "completenessRatio": 1.0,
                      "duplicateRatio": 0.0,
                      "freshnessLagMillis": 1580,
                      "lagMillis": 0,
                      "minCompletenessRatio": 1.0,
                      "maxDuplicateRatio": 0.0,
                      "maxFreshnessLagMillis": 600000,
                      "maxLagMillis": 1000000,
                      "slaStatus": "WARN",
                      "violatedSloIds": [],
                      "warningSloIds": ["source-lsn-lag-unavailable"],
                      "details": {
                        "scenario": "quality-sla-runtime",
                        "evidenceLabel": "local-docker-debezium-runtime-sla",
                        "sourceLsnFrom": 26710752,
                        "sourceLsnTo": 26711584,
                        "sourceConnectorOffsetLsn": null,
                        "sourceLsnLag": null,
                        "eventSourceOffset": [
                          {"lsn": 26710752, "sequence": "[null,\\"26710752\\"]", "txId": 761}
                        ],
                        "eventIds": ["cdc_70fbac3d3209c50cafdd2c3ad1ae42a6f60fa9b6"]
                      }
                    }
                    """))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(85))
            .andExpect(jsonPath("$.slaStatus").value("WARN"))
            .andExpect(jsonPath("$.warningSloIds[0]").value("source-lsn-lag-unavailable"))
            .andExpect(jsonPath("$.details.scenario").value("quality-sla-runtime"))
            .andExpect(jsonPath("$.details.sourceLsnTo").value(26711584))
            .andExpect(jsonPath("$.details.eventSourceOffset[0].lsn").value(26710752))
            .andExpect(jsonPath("$.evaluatedAt").value("2026-06-09T04:40:00Z"));
    }

    @Test
    void getLatestQualitySlaReturnsPersistedEvaluation() throws Exception {
        when(pipelineQualityService.findLatestSlaEvaluation("applicant-cdc-sla", "applicants"))
            .thenReturn(java.util.Optional.of(new PipelineQualitySlaRecordDto(
                84L,
                "applicant-cdc-sla",
                "applicants",
                100,
                102,
                98,
                2,
                2,
                0.98,
                0.0196,
                90_000,
                180_000,
                0.99,
                0.01,
                60_000,
                120_000,
                "FAILED",
                java.util.List.of("completeness", "duplicate-rate", "freshness", "lag"),
                java.util.List.of(),
                objectMapper.readTree("{\"sourceLsnFrom\":88432240,\"sourceLsnTo\":88432320,\"eventIds\":[\"cdc_evt_1\"]}"),
                OffsetDateTime.parse("2026-06-09T04:40:00Z")
            )));

        mockMvc.perform(get("/api/pipeline-quality/sla/evaluations/latest")
                .param("checkName", "applicant-cdc-sla")
                .param("sourceTable", "applicants"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(84))
            .andExpect(jsonPath("$.checkName").value("applicant-cdc-sla"))
            .andExpect(jsonPath("$.sourceTable").value("applicants"))
            .andExpect(jsonPath("$.slaStatus").value("FAILED"))
            .andExpect(jsonPath("$.details.sourceLsnFrom").value(88432240));
    }
}
