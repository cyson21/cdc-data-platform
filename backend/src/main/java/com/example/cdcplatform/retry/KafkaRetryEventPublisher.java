package com.example.cdcplatform.retry;

import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean({RetryEventRepository.class, KafkaTemplate.class})
public class KafkaRetryEventPublisher {

    private final RetryEventRepository retryEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaRetryEventPublisher(
        RetryEventRepository retryEventRepository,
        KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.retryEventRepository = retryEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public KafkaPublishResult publishReady(OffsetDateTime readyAt, int limit) {
        List<PublishedKafkaRetryEvent> publishedEvents = retryEventRepository.findReadyForPublish(readyAt, limit)
            .stream()
            .map(this::sendAndMarkPublished)
            .toList();
        return new KafkaPublishResult(publishedEvents);
    }

    private PublishedKafkaRetryEvent sendAndMarkPublished(RetryEventRepository.RetryEventRecord record) {
        try {
            SendResult<String, String> sendResult = kafkaTemplate
                .send(record.retryTopic(), record.eventId(), record.rawPayloadJson())
                .get(10, TimeUnit.SECONDS);
            RecordMetadata metadata = sendResult.getRecordMetadata();
            int attemptCount = retryEventRepository.markPublished(record.id());
            return new PublishedKafkaRetryEvent(
                record.id(),
                record.eventId(),
                record.sourceTable(),
                record.sourcePrimaryKey(),
                record.sourceLsn(),
                record.retryTopic(),
                record.sourceOffsetJson(),
                record.rawPayloadJson(),
                attemptCount,
                metadata.partition(),
                metadata.offset()
            );
        } catch (Exception exc) {
            throw new IllegalStateException("Failed to publish retry event to Kafka: " + record.eventId(), exc);
        }
    }

    public record KafkaPublishResult(List<PublishedKafkaRetryEvent> publishedEvents) {
    }

    public record PublishedKafkaRetryEvent(
        long retryEventId,
        String eventId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String retryTopic,
        String sourceOffsetJson,
        String rawPayloadJson,
        int attemptCount,
        int brokerPartition,
        long brokerOffset
    ) {
    }
}
