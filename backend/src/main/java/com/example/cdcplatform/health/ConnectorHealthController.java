package com.example.cdcplatform.health;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/connectors")
@Profile("!test")
public class ConnectorHealthController {

    private final ConnectorHealthService connectorHealthService;

    public ConnectorHealthController(ConnectorHealthService connectorHealthService) {
        this.connectorHealthService = connectorHealthService;
    }

    @GetMapping("/{connectorName}/health")
    public ConnectorHealthSnapshotDto getLatestConnectorHealth(@PathVariable String connectorName) {
        return connectorHealthService.getLatestConnectorHealth(connectorName)
            .orElseThrow(() -> new IllegalArgumentException("No connector health snapshot: " + connectorName));
    }
}
