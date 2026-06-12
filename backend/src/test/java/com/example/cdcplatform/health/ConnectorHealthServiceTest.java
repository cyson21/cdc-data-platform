package com.example.cdcplatform.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.cdcplatform.health.ConnectorHealthRepository.ConnectorHealthSnapshot;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConnectorHealthServiceTest {

    @Test
    void returnsDtoWithParsedOffsetSummary() {
        ConnectorHealthRepository repository = mock(ConnectorHealthRepository.class);
        ConnectorHealthService service = new ConnectorHealthService(repository);

        when(repository.findLatestByConnectorName("ats-source"))
            .thenReturn(Optional.of(sampleSnapshot()));

        ConnectorHealthSnapshotDto dto = service.getLatestConnectorHealth("ats-source").orElseThrow();

        assertThat(dto.connectorName()).isEqualTo("ats-source");
        assertThat(dto.connectorStatus()).isEqualTo("PAUSED");
        assertThat(dto.taskStatus()).isEqualTo("RUNNING");
        assertThat(dto.lagMillis()).isEqualTo(240L);
        assertThat(dto.offsetSummary().get("committed").asLong()).isEqualTo(84L);
        assertThat(dto.capturedAt()).isEqualTo(OffsetDateTime.parse("2026-06-08T10:00:00Z"));
    }

    @Test
    void failsWhenOffsetSummaryIsNotValidJson() {
        ConnectorHealthRepository repository = mock(ConnectorHealthRepository.class);
        ConnectorHealthService service = new ConnectorHealthService(repository);

        when(repository.findLatestByConnectorName("broken-source"))
            .thenReturn(Optional.of(new ConnectorHealthSnapshot(
                "broken-source",
                "FAILED",
                "ERROR",
                7L,
                "NOT_JSON",
                OffsetDateTime.parse("2026-06-08T10:00:00Z")
            )));

        assertThrows(IllegalArgumentException.class, () -> service.getLatestConnectorHealth("broken-source").orElseThrow());
    }

    private ConnectorHealthSnapshot sampleSnapshot() {
        return new ConnectorHealthSnapshot(
            "ats-source",
            "PAUSED",
            "RUNNING",
            240L,
            "{\"committed\":84}",
            OffsetDateTime.parse("2026-06-08T10:00:00Z")
        );
    }
}
