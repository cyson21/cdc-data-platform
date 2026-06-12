package com.example.cdcplatform.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class CdcEventIdTest {

    @Test
    void eventIdUsesSourceMetadataNotWallClockTime() {
        String first = CdcEventId.fromSourceMetadata(
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432240"),
            "u"
        ).value();

        String second = CdcEventId.fromSourceMetadata(
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432240"),
            "u"
        ).value();

        assertThat(second).isEqualTo(first);
    }

    @Test
    void eventIdChangesWhenSourceLsnChanges() {
        String first = CdcEventId.fromSourceMetadata(
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432240"),
            "u"
        ).value();

        String second = CdcEventId.fromSourceMetadata(
            "ats-source",
            "public",
            "applicants",
            "19c14e85-9840-7a5a-bb16-3c7f70547d9f",
            new BigInteger("88432241"),
            "u"
        ).value();

        assertThat(second).isNotEqualTo(first);
    }
}
