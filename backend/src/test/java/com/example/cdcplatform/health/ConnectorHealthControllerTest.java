package com.example.cdcplatform.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ConnectorHealthControllerTest {

    @Test
    void returnsLatestSnapshotFromService() {
        ConnectorHealthService service = mock(ConnectorHealthService.class);
        ConnectorHealthController controller = new ConnectorHealthController(service);
        ConnectorHealthSnapshotDto dto = new ConnectorHealthSnapshotDto(
            "ats-source",
            "RUNNING",
            "RUNNING",
            120L,
            null,
            OffsetDateTime.parse("2026-06-08T11:00:00Z")
        );

        when(service.getLatestConnectorHealth("ats-source")).thenReturn(Optional.of(dto));

        ConnectorHealthSnapshotDto actual = controller.getLatestConnectorHealth("ats-source");

        assertThat(actual).isSameAs(dto);
        verify(service).getLatestConnectorHealth("ats-source");
    }

    @Test
    void throwsWhenNoSnapshotForConnector() {
        ConnectorHealthService service = mock(ConnectorHealthService.class);
        ConnectorHealthController controller = new ConnectorHealthController(service);

        when(service.getLatestConnectorHealth("missing-source")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> controller.getLatestConnectorHealth("missing-source"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("missing-source");
    }
}
