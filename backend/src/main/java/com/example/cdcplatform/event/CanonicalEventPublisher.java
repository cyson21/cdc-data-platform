package com.example.cdcplatform.event;

import com.example.cdcplatform.ledger.CdcEventLedgerRepository;

public class CanonicalEventPublisher {

    private final CdcEventLedgerRepository ledgerRepository;

    public CanonicalEventPublisher(CdcEventLedgerRepository ledgerRepository) {
        this.ledgerRepository = ledgerRepository;
    }

    public boolean recordCanonicalEvent(CanonicalCdcEvent event, String sourceOffsetJson) {
        return ledgerRepository.insertIfAbsent(new CdcEventLedgerRepository.LedgerEvent(
            event.eventId(),
            event.sourceConnector(),
            event.sourceSchema(),
            event.sourceTable(),
            event.sourcePrimaryKey(),
            event.sourceLsn(),
            sourceOffsetJson,
            event.operation(),
            "PROCESSED"
        ));
    }
}
