package com.example.cdcplatform.retry;

import java.math.BigInteger;
import java.time.OffsetDateTime;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/retry-events")
@Profile("!test")
public class RetryEventController {

    private final RetryEventService retryEventService;

    public RetryEventController(RetryEventService retryEventService) {
        this.retryEventService = retryEventService;
    }

    @PostMapping
    public ResponseEntity<RetryEventService.RetryEventResult> scheduleRetry(@RequestBody ScheduleRetryRequest body) {
        return ResponseEntity.accepted().body(retryEventService.scheduleRetry(new RetryEventService.ScheduleRetryCommand(
            body.eventId(),
            body.sourceConnector(),
            body.sourceSchema(),
            body.sourceTable(),
            body.sourcePrimaryKey(),
            body.sourceLsn(),
            body.sourceOffsetJson(),
            body.rawPayloadJson(),
            body.failureReason(),
            body.retryTopic(),
            body.nextAttemptAt(),
            body.maxAttempts()
        )));
    }

    @PostMapping("/publish-ready")
    public ResponseEntity<RetryEventService.PublishReadyResult> publishReady(@RequestBody PublishReadyRequest body) {
        return ResponseEntity.accepted().body(retryEventService.publishReady(body.readyAt(), body.limit()));
    }

    public record ScheduleRetryRequest(
        String eventId,
        String sourceConnector,
        String sourceSchema,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String sourceOffsetJson,
        String rawPayloadJson,
        String failureReason,
        String retryTopic,
        OffsetDateTime nextAttemptAt,
        int maxAttempts
    ) {
    }

    public record PublishReadyRequest(OffsetDateTime readyAt, int limit) {
    }
}
