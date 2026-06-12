package com.example.cdcplatform.retry;

import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(RetryEventRepository.class)
public class RetryEventService {

    private final RetryEventRepository retryEventRepository;

    public RetryEventService(RetryEventRepository retryEventRepository) {
        this.retryEventRepository = retryEventRepository;
    }

    public RetryEventResult scheduleRetry(ScheduleRetryCommand command) {
        RetryEventRepository.InsertResult insertResult = retryEventRepository.insertIfAbsent(
            new RetryEventRepository.RetryEvent(
                command.eventId(),
                command.sourceConnector(),
                command.sourceSchema(),
                command.sourceTable(),
                command.sourcePrimaryKey(),
                command.sourceLsn(),
                command.sourceOffsetJson(),
                command.rawPayloadJson(),
                command.failureReason(),
                command.retryTopic(),
                command.nextAttemptAt(),
                command.maxAttempts()
            )
        );
        RetryEventRepository.RetryEventRecord record = insertResult.record();
        return new RetryEventResult(
            record.id(),
            record.eventId(),
            record.sourceTable(),
            record.sourcePrimaryKey(),
            record.sourceLsn(),
            record.retryTopic(),
            record.retryStatus(),
            insertResult.created()
        );
    }

    public PublishReadyResult publishReady(OffsetDateTime readyAt, int limit) {
        List<PublishedRetryEvent> publishedEvents = retryEventRepository.findReadyForPublish(readyAt, limit)
            .stream()
            .map(record -> {
                int attemptCount = retryEventRepository.markPublished(record.id());
                return new PublishedRetryEvent(
                    record.id(),
                    record.eventId(),
                    record.sourceTable(),
                    record.sourcePrimaryKey(),
                    record.sourceLsn(),
                    record.retryTopic(),
                    record.sourceOffsetJson(),
                    record.rawPayloadJson(),
                    attemptCount
                );
            })
            .toList();
        return new PublishReadyResult(publishedEvents);
    }

    public record ScheduleRetryCommand(
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

    public record RetryEventResult(
        long retryEventId,
        String eventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String retryTopic,
        String retryStatus,
        boolean created
    ) {
    }

    public record PublishReadyResult(List<PublishedRetryEvent> publishedEvents) {
    }

    public record PublishedRetryEvent(
        long retryEventId,
        String eventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String retryTopic,
        String sourceOffsetJson,
        String rawPayloadJson,
        int attemptCount
    ) {
    }
}
