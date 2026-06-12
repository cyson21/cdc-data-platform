package com.example.cdcplatform;

import com.example.cdcplatform.event.CanonicalEventPublisher;
import com.example.cdcplatform.event.CanonicalIngestService;
import com.example.cdcplatform.health.ConnectorHealthRepository;
import com.example.cdcplatform.health.ConnectorHealthService;
import com.example.cdcplatform.ledger.CdcEventLedgerRepository;
import com.example.cdcplatform.quality.PipelineQualityCheckRepository;
import com.example.cdcplatform.quality.PipelineQualitySlaRepository;
import com.example.cdcplatform.quality.PipelineQualityService;
import com.example.cdcplatform.recovery.RecoveryRunRepository;
import com.example.cdcplatform.recovery.RecoveryRunService;
import com.example.cdcplatform.replay.DlqEventRepository;
import com.example.cdcplatform.replay.ReplayRequestRepository;
import com.example.cdcplatform.replay.ReplayRequestService;
import com.example.cdcplatform.retry.RetryEventRepository;
import com.example.cdcplatform.retry.RetryEventService;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class ControlPlaneConfiguration {

    @Bean
    CdcEventLedgerRepository cdcEventLedgerRepository(DataSource dataSource) {
        return new CdcEventLedgerRepository(dataSource);
    }

    @Bean
    CanonicalEventPublisher canonicalEventPublisher(CdcEventLedgerRepository repository) {
        return new CanonicalEventPublisher(repository);
    }

    @Bean
    CanonicalIngestService canonicalIngestService(CanonicalEventPublisher publisher) {
        return new CanonicalIngestService(publisher);
    }

    @Bean
    ConnectorHealthRepository connectorHealthRepository(DataSource dataSource) {
        return new ConnectorHealthRepository(dataSource);
    }

    @Bean
    ConnectorHealthService connectorHealthService(ConnectorHealthRepository repository) {
        return new ConnectorHealthService(repository);
    }

    @Bean
    PipelineQualityCheckRepository pipelineQualityCheckRepository(DataSource dataSource) {
        return new PipelineQualityCheckRepository(dataSource);
    }

    @Bean
    PipelineQualitySlaRepository pipelineQualitySlaRepository(DataSource dataSource) {
        return new PipelineQualitySlaRepository(dataSource);
    }

    @Bean
    PipelineQualityService pipelineQualityService(
        PipelineQualityCheckRepository repository,
        PipelineQualitySlaRepository slaRepository
    ) {
        return new PipelineQualityService(repository, slaRepository);
    }

    @Bean
    RetryEventRepository retryEventRepository(DataSource dataSource) {
        return new RetryEventRepository(dataSource);
    }

    @Bean
    RetryEventService retryEventService(RetryEventRepository repository) {
        return new RetryEventService(repository);
    }

    @Bean
    DlqEventRepository dlqEventRepository(DataSource dataSource) {
        return new DlqEventRepository(dataSource);
    }

    @Bean
    ReplayRequestRepository replayRequestRepository(DataSource dataSource) {
        return new ReplayRequestRepository(dataSource);
    }

    @Bean
    ReplayRequestService replayRequestService(
        DlqEventRepository dlqEventRepository,
        ReplayRequestRepository replayRequestRepository
    ) {
        return new ReplayRequestService(dlqEventRepository, replayRequestRepository);
    }

    @Bean
    RecoveryRunRepository recoveryRunRepository(DataSource dataSource) {
        return new RecoveryRunRepository(dataSource);
    }

    @Bean
    RecoveryRunService recoveryRunService(RecoveryRunRepository repository) {
        return new RecoveryRunService(repository);
    }
}
