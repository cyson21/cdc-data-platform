package com.example.cdcplatform.health;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ConnectorHealthController.class)
class ConnectorHealthHttpControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private ConnectorHealthService connectorHealthService;

    @Test
    void getConnectorHealthReturnsLatestSnapshot() throws Exception {
        when(connectorHealthService.getLatestConnectorHealth("ats-source"))
            .thenReturn(Optional.of(new ConnectorHealthSnapshotDto(
                "ats-source",
                "RUNNING",
                "RUNNING",
                120L,
                objectMapper.readTree("{\"committed\":42}"),
                OffsetDateTime.parse("2026-06-08T11:00:00Z")
            )));

        mockMvc.perform(get("/api/connectors/ats-source/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.connectorName").value("ats-source"))
            .andExpect(jsonPath("$.connectorStatus").value("RUNNING"))
            .andExpect(jsonPath("$.taskStatus").value("RUNNING"))
            .andExpect(jsonPath("$.lagMillis").value(120))
            .andExpect(jsonPath("$.offsetSummary.committed").value(42))
            .andExpect(jsonPath("$.capturedAt").value("2026-06-08T11:00:00Z"));
    }
}
