package com.example.cdcplatform.retry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(RetryEventController.class)
class RetryEventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RetryEventService retryEventService;

    @Test
    void postRetryEventSchedulesSourceMetadataRetry() throws Exception {
        when(retryEventService.scheduleRetry(any(RetryEventService.ScheduleRetryCommand.class)))
            .thenReturn(new RetryEventService.RetryEventResult(
                7L,
                "candidate-cdc-evt-1",
                "applicants",
                "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                new BigInteger("88432240"),
                "cdc.retry.applicants",
                "SCHEDULED",
                true
            ));

        mockMvc.perform(post("/api/retry-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "eventId": "candidate-cdc-evt-1",
                      "sourceConnector": "ats-source",
                      "sourceSchema": "public",
                      "sourceTable": "applicants",
                      "sourcePrimaryKey": "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                      "sourceLsn": 88432240,
                      "sourceOffsetJson": "{\\"lsn\\":88432240}",
                      "rawPayloadJson": "{\\"op\\":\\"u\\"}",
                      "failureReason": "canonical publisher unavailable",
                      "retryTopic": "cdc.retry.applicants",
                      "nextAttemptAt": "2026-06-09T10:00:00Z",
                      "maxAttempts": 3
                    }
                    """))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.retryEventId").value(7))
            .andExpect(jsonPath("$.eventId").value("candidate-cdc-evt-1"))
            .andExpect(jsonPath("$.sourceLsn").value(88432240))
            .andExpect(jsonPath("$.retryStatus").value("SCHEDULED"))
            .andExpect(jsonPath("$.created").value(true));
    }

    @Test
    void postPublishReadyReturnsRetryTopicAndPayloadMetadata() throws Exception {
        when(retryEventService.publishReady(eq(OffsetDateTime.parse("2026-06-09T11:00:00Z")), eq(10)))
            .thenReturn(new RetryEventService.PublishReadyResult(List.of(
                new RetryEventService.PublishedRetryEvent(
                    7L,
                    "candidate-cdc-evt-1",
                    "applicants",
                    "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                    new BigInteger("88432240"),
                    "cdc.retry.applicants",
                    "{\"lsn\":88432240}",
                    "{\"op\":\"u\"}",
                    1
                )
            )));

        mockMvc.perform(post("/api/retry-events/publish-ready")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "readyAt": "2026-06-09T11:00:00Z",
                      "limit": 10
                    }
                    """))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.publishedEvents[0].retryEventId").value(7))
            .andExpect(jsonPath("$.publishedEvents[0].retryTopic").value("cdc.retry.applicants"))
            .andExpect(jsonPath("$.publishedEvents[0].sourceLsn").value(88432240))
            .andExpect(jsonPath("$.publishedEvents[0].attemptCount").value(1));
    }
}
