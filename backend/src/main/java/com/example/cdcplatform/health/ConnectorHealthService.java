package com.example.cdcplatform.health;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnBean(ConnectorHealthRepository.class)
public class ConnectorHealthService {

    private final ConnectorHealthRepository repository;
    private final ObjectMapper objectMapper;

    public ConnectorHealthService(ConnectorHealthRepository repository) {
        this.repository = repository;
        this.objectMapper = new ObjectMapper();
    }

    public Optional<ConnectorHealthSnapshotDto> getLatestConnectorHealth(String connectorName) {
        return repository.findLatestByConnectorName(connectorName).map(snapshot -> {
            return new ConnectorHealthSnapshotDto(
                snapshot.connectorName(),
                snapshot.connectorStatus(),
                snapshot.taskStatus(),
                snapshot.lagMillis(),
                parseOffsetSummary(snapshot.offsetSummaryJson()),
                snapshot.capturedAt()
            );
        });
    }

    private JsonNode parseOffsetSummary(String offsetSummaryJson) {
        try {
            return objectMapper.readTree(offsetSummaryJson);
        } catch (JsonProcessingException exc) {
            throw new IllegalArgumentException("Invalid offset summary JSON", exc);
        }
    }
}
