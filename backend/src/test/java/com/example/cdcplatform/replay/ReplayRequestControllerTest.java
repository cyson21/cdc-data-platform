package com.example.cdcplatform.replay;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ReplayRequestController.class)
class ReplayRequestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ReplayRequestService replayRequestService;

    @Test
    void postReplayRequestReturnsAcceptedReplayResult() throws Exception {
        when(replayRequestService.requestReplay(eq(42L), eq("local-operator")))
            .thenReturn(new ReplayRequestService.ReplayRequestResult(
                "replay_123",
                "applicants",
                "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                new BigInteger("88432240"),
                "REQUESTED"
            ));

        mockMvc.perform(post("/api/replay-requests")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "dlqEventId": 42,
                      "requestedBy": "local-operator"
                    }
                    """))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.requestId").value("replay_123"))
            .andExpect(jsonPath("$.sourceTable").value("applicants"))
            .andExpect(jsonPath("$.sourcePrimaryKey").value("19c14e85-9840-7a5a-bb16-3c7f70547d9f"))
            .andExpect(jsonPath("$.sourceLsn").value(88432240))
            .andExpect(jsonPath("$.replayStatus").value("REQUESTED"));
    }
}
