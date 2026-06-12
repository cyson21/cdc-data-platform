package com.example.cdcplatform.replay;

import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/replay-requests")
@Profile("!test")
public class ReplayRequestController {

    private final ReplayRequestService replayRequestService;

    public ReplayRequestController(ReplayRequestService replayRequestService) {
        this.replayRequestService = replayRequestService;
    }

    @PostMapping
    public ResponseEntity<ReplayRequestService.ReplayRequestResult> requestReplay(@RequestBody ReplayRequestBody body) {
        return ResponseEntity.accepted().body(replayRequestService.requestReplay(body.dlqEventId(), body.requestedBy()));
    }

    public record ReplayRequestBody(long dlqEventId, String requestedBy) {
    }
}
