package com.example.cdcplatform.replay;

import java.math.BigInteger;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean({DlqEventRepository.class, ReplayRequestRepository.class})
public class ReplayRequestService {

    private static final String REQUESTED = "REQUESTED";

    private final DlqEventRepository dlqEventRepository;
    private final ReplayRequestRepository replayRequestRepository;

    public ReplayRequestService(
        DlqEventRepository dlqEventRepository,
        ReplayRequestRepository replayRequestRepository
    ) {
        this.dlqEventRepository = dlqEventRepository;
        this.replayRequestRepository = replayRequestRepository;
    }

    public ReplayRequestResult requestReplay(long dlqEventId, String requestedBy) {
        DlqEventRepository.DlqEvent dlqEvent = dlqEventRepository.findById(dlqEventId)
            .orElseThrow(() -> new IllegalArgumentException("DLQ event not found: " + dlqEventId));
        String requestId = "replay_" + UUID.randomUUID();

        replayRequestRepository.insert(new ReplayRequestRepository.ReplayRequest(
            requestId,
            requestedBy,
            dlqEventId,
            dlqEvent.sourceTable(),
            dlqEvent.sourcePrimaryKey(),
            dlqEvent.sourceLsn(),
            REQUESTED
        ));
        dlqEventRepository.markReplayRequested(dlqEventId);

        return new ReplayRequestResult(
            requestId,
            dlqEvent.sourceTable(),
            dlqEvent.sourcePrimaryKey(),
            dlqEvent.sourceLsn(),
            REQUESTED
        );
    }

    public record ReplayRequestResult(
        String requestId,
        String sourceTable,
        String sourcePrimaryKey,
        BigInteger sourceLsn,
        String replayStatus
    ) {
    }
}
