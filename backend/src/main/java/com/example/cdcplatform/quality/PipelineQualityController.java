package com.example.cdcplatform.quality;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/pipeline-quality")
@Profile("!test")
public class PipelineQualityController {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final PipelineQualityService pipelineQualityService;

    public PipelineQualityController(PipelineQualityService pipelineQualityService) {
        this.pipelineQualityService = pipelineQualityService;
    }

    @PostMapping("/checks")
    public PipelineQualityCheckDto recordQualityCheck(@RequestBody RecordQualityCheckRequest body) {
        return pipelineQualityService.recordQualityCheck(new PipelineQualityService.RecordQualityCheckCommand(
            body.checkName(),
            body.sourceTable(),
            body.sourceRowCount(),
            body.rawEventCount(),
            body.canonicalEventCount(),
            body.duplicateEventCount(),
            body.missingEventCount(),
            detailsJson(body.details())
        ));
    }

    @PostMapping("/sla/evaluations")
    public PipelineQualitySlaDto evaluateQualitySla(@RequestBody EvaluateQualitySlaRequest body) {
        return pipelineQualityService.evaluateSla(new PipelineQualityService.EvaluateQualitySlaCommand(
            body.checkName(),
            body.sourceTable(),
            body.sourceRowCount(),
            body.rawEventCount(),
            body.canonicalEventCount(),
            body.duplicateEventCount(),
            body.missingEventCount(),
            body.freshnessLagMillis(),
            body.lagMillis(),
            body.minCompletenessRatio(),
            body.maxDuplicateRatio(),
            body.maxFreshnessLagMillis(),
            body.maxLagMillis(),
            detailsJson(body.details())
        ));
    }

    @PostMapping("/sla/evaluations/records")
    public PipelineQualitySlaRecordDto recordQualitySla(@RequestBody EvaluateQualitySlaRequest body) {
        return pipelineQualityService.recordSlaEvaluation(new PipelineQualityService.EvaluateQualitySlaCommand(
            body.checkName(),
            body.sourceTable(),
            body.sourceRowCount(),
            body.rawEventCount(),
            body.canonicalEventCount(),
            body.duplicateEventCount(),
            body.missingEventCount(),
            body.freshnessLagMillis(),
            body.lagMillis(),
            body.minCompletenessRatio(),
            body.maxDuplicateRatio(),
            body.maxFreshnessLagMillis(),
            body.maxLagMillis(),
            detailsJson(body.details())
        ));
    }

    @PostMapping("/sla/runtime-evaluations/records")
    public PipelineQualitySlaRecordDto recordRuntimeQualitySla(@RequestBody RecordRuntimeSlaEvaluationRequest body) {
        return pipelineQualityService.recordRuntimeSlaEvaluation(
            new PipelineQualityService.RecordRuntimeSlaEvaluationCommand(
                body.checkName(),
                body.sourceTable(),
                body.sourceRowCount(),
                body.rawEventCount(),
                body.canonicalEventCount(),
                body.duplicateEventCount(),
                body.missingEventCount(),
                body.completenessRatio(),
                body.duplicateRatio(),
                body.freshnessLagMillis(),
                body.lagMillis(),
                body.minCompletenessRatio(),
                body.maxDuplicateRatio(),
                body.maxFreshnessLagMillis(),
                body.maxLagMillis(),
                body.slaStatus(),
                jsonArray(body.violatedSloIds()),
                jsonArray(body.warningSloIds()),
                detailsJson(body.details())
            )
        );
    }

    @GetMapping("/sla/evaluations/latest")
    public PipelineQualitySlaRecordDto getLatestQualitySla(
        @RequestParam String checkName,
        @RequestParam String sourceTable
    ) {
        return pipelineQualityService.findLatestSlaEvaluation(checkName, sourceTable)
            .orElseThrow(() -> new IllegalArgumentException("No quality SLA evaluation: " + checkName));
    }

    private static String detailsJson(JsonNode details) {
        try {
            return OBJECT_MAPPER.writeValueAsString(Objects.requireNonNullElseGet(
                details,
                OBJECT_MAPPER::createObjectNode
            ));
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("details must be JSON serializable", exc);
        }
    }

    private static String jsonArray(java.util.List<String> values) {
        try {
            return OBJECT_MAPPER.writeValueAsString(Objects.requireNonNullElseGet(
                values,
                java.util.List::of
            ));
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("SLO ids must be JSON serializable", exc);
        }
    }

    public record RecordQualityCheckRequest(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        JsonNode details
    ) {
    }

    public record EvaluateQualitySlaRequest(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        long freshnessLagMillis,
        long lagMillis,
        double minCompletenessRatio,
        double maxDuplicateRatio,
        long maxFreshnessLagMillis,
        long maxLagMillis,
        JsonNode details
    ) {
    }

    public record RecordRuntimeSlaEvaluationRequest(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        double completenessRatio,
        double duplicateRatio,
        long freshnessLagMillis,
        long lagMillis,
        double minCompletenessRatio,
        double maxDuplicateRatio,
        long maxFreshnessLagMillis,
        long maxLagMillis,
        String slaStatus,
        java.util.List<String> violatedSloIds,
        java.util.List<String> warningSloIds,
        JsonNode details
    ) {
    }
}
