package com.example.cdcplatform.event;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/canonical-events")
@Profile("!test")
public class CanonicalIngestController {

    private final CanonicalIngestService canonicalIngestService;

    public CanonicalIngestController(CanonicalIngestService canonicalIngestService) {
        this.canonicalIngestService = canonicalIngestService;
    }

    @PostMapping("/ingest")
    public ResponseEntity<CanonicalIngestService.CanonicalIngestResult> ingestRawEnvelope(@RequestBody String body) {
        return ResponseEntity.accepted().body(canonicalIngestService.ingestRawEnvelope(body));
    }
}
