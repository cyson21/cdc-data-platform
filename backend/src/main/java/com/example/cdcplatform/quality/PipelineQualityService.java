package com.example.cdcplatform.quality;

import com.example.cdcplatform.quality.PipelineQualityCheckRepository.NewQualityCheck;
import com.example.cdcplatform.quality.PipelineQualityCheckRepository.QualityCheckRecord;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository.NewSlaEvaluation;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository.SlaEvaluationRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PipelineQualityService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final PipelineQualityCheckRepository repository;
    private final PipelineQualitySlaRepository slaRepository;

    public PipelineQualityService(PipelineQualityCheckRepository repository) {
        this(repository, null);
    }

    public PipelineQualityService(PipelineQualityCheckRepository repository, PipelineQualitySlaRepository slaRepository) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.slaRepository = slaRepository;
    }

    public PipelineQualityCheckDto recordQualityCheck(RecordQualityCheckCommand command) {
        validate(command);
        String status = determineStatus(command);
        QualityCheckRecord record = repository.insert(new NewQualityCheck(
            command.checkName(),
            command.sourceTable(),
            command.sourceRowCount(),
            command.rawEventCount(),
            command.canonicalEventCount(),
            command.duplicateEventCount(),
            command.missingEventCount(),
            status,
            normalizeDetails(command.detailsJson())
        ));
        return toDto(record);
    }

    public PipelineQualitySlaDto evaluateSla(EvaluateQualitySlaCommand command) {
        validate(command);

        double completenessRatio = ratio(command.canonicalEventCount(), command.sourceRowCount(), 1.0);
        double duplicateRatio = ratio(command.duplicateEventCount(), command.rawEventCount(), 0.0);
        List<String> violatedSloIds = new ArrayList<>();
        List<String> warningSloIds = new ArrayList<>();

        if (completenessRatio < command.minCompletenessRatio() || command.missingEventCount() > 0) {
            violatedSloIds.add("completeness");
        }
        if (duplicateRatio > command.maxDuplicateRatio()) {
            violatedSloIds.add("duplicate-rate");
        } else if (command.duplicateEventCount() > 0) {
            warningSloIds.add("duplicate-rate");
        }
        if (command.freshnessLagMillis() > command.maxFreshnessLagMillis()) {
            violatedSloIds.add("freshness");
        }
        if (command.lagMillis() > command.maxLagMillis()) {
            violatedSloIds.add("lag");
        }

        String slaStatus;
        if (!violatedSloIds.isEmpty()) {
            slaStatus = "FAILED";
        } else if (!warningSloIds.isEmpty()) {
            slaStatus = "WARN";
        } else {
            slaStatus = "PASSED";
        }

        return new PipelineQualitySlaDto(
            command.checkName(),
            command.sourceTable(),
            command.sourceRowCount(),
            command.rawEventCount(),
            command.canonicalEventCount(),
            command.duplicateEventCount(),
            command.missingEventCount(),
            completenessRatio,
            duplicateRatio,
            command.freshnessLagMillis(),
            command.lagMillis(),
            command.minCompletenessRatio(),
            command.maxDuplicateRatio(),
            command.maxFreshnessLagMillis(),
            command.maxLagMillis(),
            slaStatus,
            List.copyOf(violatedSloIds),
            List.copyOf(warningSloIds),
            parseDetails(normalizeDetails(command.detailsJson()))
        );
    }

    public PipelineQualitySlaRecordDto recordSlaEvaluation(EvaluateQualitySlaCommand command) {
        PipelineQualitySlaDto evaluated = evaluateSla(command);
        SlaEvaluationRecord record = requireSlaRepository().insert(new NewSlaEvaluation(
            evaluated.checkName(),
            evaluated.sourceTable(),
            evaluated.sourceRowCount(),
            evaluated.rawEventCount(),
            evaluated.canonicalEventCount(),
            evaluated.duplicateEventCount(),
            evaluated.missingEventCount(),
            evaluated.completenessRatio(),
            evaluated.duplicateRatio(),
            evaluated.freshnessLagMillis(),
            evaluated.lagMillis(),
            evaluated.minCompletenessRatio(),
            evaluated.maxDuplicateRatio(),
            evaluated.maxFreshnessLagMillis(),
            evaluated.maxLagMillis(),
            evaluated.slaStatus(),
            toJson(evaluated.violatedSloIds()),
            toJson(evaluated.warningSloIds()),
            normalizeDetails(command.detailsJson())
        ));
        return toDto(record);
    }

    public PipelineQualitySlaRecordDto recordRuntimeSlaEvaluation(RecordRuntimeSlaEvaluationCommand command) {
        validate(command);
        SlaEvaluationRecord record = requireSlaRepository().insert(new NewSlaEvaluation(
            command.checkName(),
            command.sourceTable(),
            command.sourceRowCount(),
            command.rawEventCount(),
            command.canonicalEventCount(),
            command.duplicateEventCount(),
            command.missingEventCount(),
            command.completenessRatio(),
            command.duplicateRatio(),
            command.freshnessLagMillis(),
            command.lagMillis(),
            command.minCompletenessRatio(),
            command.maxDuplicateRatio(),
            command.maxFreshnessLagMillis(),
            command.maxLagMillis(),
            command.slaStatus(),
            normalizeStringArray(command.violatedSloIdsJson()),
            normalizeStringArray(command.warningSloIdsJson()),
            normalizeDetails(command.detailsJson())
        ));
        return toDto(record);
    }

    public Optional<PipelineQualitySlaRecordDto> findLatestSlaEvaluation(String checkName, String sourceTable) {
        requireText(checkName, "checkName");
        requireText(sourceTable, "sourceTable");
        return requireSlaRepository()
            .findLatestByCheckNameAndSourceTable(checkName, sourceTable)
            .map(PipelineQualityService::toDto);
    }

    private static String determineStatus(RecordQualityCheckCommand command) {
        if (command.missingEventCount() > 0 || command.canonicalEventCount() < command.sourceRowCount()) {
            return "FAILED";
        }
        if (command.duplicateEventCount() > 0 || command.rawEventCount() != command.canonicalEventCount()) {
            return "WARN";
        }
        return "PASSED";
    }

    private static void validate(RecordQualityCheckCommand command) {
        requireText(command.checkName(), "checkName");
        requireText(command.sourceTable(), "sourceTable");
        requireNonNegative(command.sourceRowCount(), "sourceRowCount");
        requireNonNegative(command.rawEventCount(), "rawEventCount");
        requireNonNegative(command.canonicalEventCount(), "canonicalEventCount");
        requireNonNegative(command.duplicateEventCount(), "duplicateEventCount");
        requireNonNegative(command.missingEventCount(), "missingEventCount");
    }

    private static void validate(EvaluateQualitySlaCommand command) {
        requireText(command.checkName(), "checkName");
        requireText(command.sourceTable(), "sourceTable");
        requireNonNegative(command.sourceRowCount(), "sourceRowCount");
        requireNonNegative(command.rawEventCount(), "rawEventCount");
        requireNonNegative(command.canonicalEventCount(), "canonicalEventCount");
        requireNonNegative(command.duplicateEventCount(), "duplicateEventCount");
        requireNonNegative(command.missingEventCount(), "missingEventCount");
        requireNonNegative(command.freshnessLagMillis(), "freshnessLagMillis");
        requireNonNegative(command.lagMillis(), "lagMillis");
        requireRatio(command.minCompletenessRatio(), "minCompletenessRatio");
        requireRatio(command.maxDuplicateRatio(), "maxDuplicateRatio");
        requireNonNegative(command.maxFreshnessLagMillis(), "maxFreshnessLagMillis");
        requireNonNegative(command.maxLagMillis(), "maxLagMillis");
    }

    private static void validate(RecordRuntimeSlaEvaluationCommand command) {
        requireText(command.checkName(), "checkName");
        requireText(command.sourceTable(), "sourceTable");
        requireNonNegative(command.sourceRowCount(), "sourceRowCount");
        requireNonNegative(command.rawEventCount(), "rawEventCount");
        requireNonNegative(command.canonicalEventCount(), "canonicalEventCount");
        requireNonNegative(command.duplicateEventCount(), "duplicateEventCount");
        requireNonNegative(command.missingEventCount(), "missingEventCount");
        requireRatio(command.completenessRatio(), "completenessRatio");
        requireRatio(command.duplicateRatio(), "duplicateRatio");
        requireNonNegative(command.freshnessLagMillis(), "freshnessLagMillis");
        requireNonNegative(command.lagMillis(), "lagMillis");
        requireRatio(command.minCompletenessRatio(), "minCompletenessRatio");
        requireRatio(command.maxDuplicateRatio(), "maxDuplicateRatio");
        requireNonNegative(command.maxFreshnessLagMillis(), "maxFreshnessLagMillis");
        requireNonNegative(command.maxLagMillis(), "maxLagMillis");
        requireStatus(command.slaStatus());
        normalizeStringArray(command.violatedSloIdsJson());
        normalizeStringArray(command.warningSloIdsJson());
        normalizeDetails(command.detailsJson());
    }

    private static String normalizeDetails(String detailsJson) {
        try {
            JsonNode details = OBJECT_MAPPER.readTree(
                detailsJson == null || detailsJson.isBlank() ? "{}" : detailsJson
            );
            return OBJECT_MAPPER.writeValueAsString(details);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("detailsJson must be valid JSON", exc);
        }
    }

    private static PipelineQualityCheckDto toDto(QualityCheckRecord record) {
        return new PipelineQualityCheckDto(
            record.id(),
            record.checkName(),
            record.sourceTable(),
            record.sourceRowCount(),
            record.rawEventCount(),
            record.canonicalEventCount(),
            record.duplicateEventCount(),
            record.missingEventCount(),
            record.checkStatus(),
            parseDetails(record.detailsJson()),
            record.checkedAt()
        );
    }

    private static PipelineQualitySlaRecordDto toDto(SlaEvaluationRecord record) {
        return new PipelineQualitySlaRecordDto(
            record.id(),
            record.checkName(),
            record.sourceTable(),
            record.sourceRowCount(),
            record.rawEventCount(),
            record.canonicalEventCount(),
            record.duplicateEventCount(),
            record.missingEventCount(),
            record.completenessRatio(),
            record.duplicateRatio(),
            record.freshnessLagMillis(),
            record.lagMillis(),
            record.minCompletenessRatio(),
            record.maxDuplicateRatio(),
            record.maxFreshnessLagMillis(),
            record.maxLagMillis(),
            record.slaStatus(),
            parseStringList(record.violatedSloIdsJson()),
            parseStringList(record.warningSloIdsJson()),
            parseDetails(record.detailsJson()),
            record.evaluatedAt()
        );
    }

    private static JsonNode parseDetails(String detailsJson) {
        try {
            return OBJECT_MAPPER.readTree(detailsJson);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("stored quality details JSON is invalid", exc);
        }
    }

    private static List<String> parseStringList(String json) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(json);
            if (!node.isArray()) {
                throw new IllegalArgumentException("SLO id JSON must be an array");
            }
            List<String> values = new ArrayList<>();
            node.forEach(value -> values.add(value.asText()));
            return List.copyOf(values);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("stored SLO id JSON is invalid", exc);
        }
    }

    private static String normalizeStringArray(String json) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(json == null || json.isBlank() ? "[]" : json);
            if (!node.isArray()) {
                throw new IllegalArgumentException("SLO id JSON must be an array");
            }
            for (JsonNode item : node) {
                if (!item.isTextual()) {
                    throw new IllegalArgumentException("SLO id JSON must contain strings only");
                }
            }
            return OBJECT_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("SLO id JSON is invalid", exc);
        }
    }

    private static String toJson(List<String> values) {
        try {
            return OBJECT_MAPPER.writeValueAsString(values);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("SLO ids must be JSON serializable", exc);
        }
    }

    private PipelineQualitySlaRepository requireSlaRepository() {
        if (slaRepository == null) {
            throw new IllegalStateException("PipelineQualitySlaRepository is not configured");
        }
        return slaRepository;
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
    }

    private static void requireRatio(double value, String fieldName) {
        if (Double.isNaN(value) || value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(fieldName + " must be between 0 and 1");
        }
    }

    private static void requireStatus(String value) {
        if (!"PASSED".equals(value) && !"WARN".equals(value) && !"FAILED".equals(value)) {
            throw new IllegalArgumentException("slaStatus must be PASSED, WARN, or FAILED");
        }
    }

    private static double ratio(long numerator, long denominator, double emptyValue) {
        if (denominator == 0) {
            return emptyValue;
        }
        return (double) numerator / (double) denominator;
    }

    public record RecordQualityCheckCommand(
        String checkName,
        String sourceTable,
        long sourceRowCount,
        long rawEventCount,
        long canonicalEventCount,
        long duplicateEventCount,
        long missingEventCount,
        String detailsJson
    ) {
    }

    public record EvaluateQualitySlaCommand(
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
        String detailsJson
    ) {
    }

    public record RecordRuntimeSlaEvaluationCommand(
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
        String violatedSloIdsJson,
        String warningSloIdsJson,
        String detailsJson
    ) {
    }
}
