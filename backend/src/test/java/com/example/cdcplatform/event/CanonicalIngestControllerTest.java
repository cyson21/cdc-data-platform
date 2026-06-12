package com.example.cdcplatform.event;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(CanonicalIngestController.class)
class CanonicalIngestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CanonicalIngestService canonicalIngestService;

    @Test
    void postCanonicalIngestReturnsSourceMetadataEvidence() throws Exception {
        when(canonicalIngestService.ingestRawEnvelope(anyString()))
            .thenReturn(new CanonicalIngestService.CanonicalIngestResult(
                "cdc_1234567890abcdef",
                "applicants",
                "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
                new BigInteger("88432240"),
                "{\"lsn\":88432240,\"txId\":773}",
                "u",
                true,
                1,
                0
            ));

        mockMvc.perform(post("/api/canonical-events/ingest")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "source": {
                        "name": "cdc.raw",
                        "schema": "public",
                        "table": "applicants",
                        "lsn": 88432240
                      },
                      "after": {
                        "id": "19c14e85-9840-7a5a-bb16-3c7f70547d9f"
                      },
                      "op": "u"
                    }
                    """))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.eventId").value("cdc_1234567890abcdef"))
            .andExpect(jsonPath("$.sourceTable").value("applicants"))
            .andExpect(jsonPath("$.sourcePrimaryKey").value("19c14e85-9840-7a5a-bb16-3c7f70547d9f"))
            .andExpect(jsonPath("$.sourceLsn").value(88432240))
            .andExpect(jsonPath("$.created").value(true))
            .andExpect(jsonPath("$.canonicalEventCount").value(1))
            .andExpect(jsonPath("$.duplicateEventCount").value(0));
    }
}
