/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BrokerUrlsTest {

    @Test
    void singleUrlIsReturnedAsIs() {
        assertEquals(List.of("amqp://broker-1.example.com:61112?jms.prefetchPolicy.all=100"),
                BrokerUrls.parse("amqp://broker-1.example.com:61112?jms.prefetchPolicy.all=100"));
    }

    @Test
    void listIsSplitOnTopLevelCommasAndTrimmed() {
        assertEquals(List.of("amqp://a:61112", "amqp://b:61112", "amqp://c:61112"),
                BrokerUrls.parse("amqp://a:61112, amqp://b:61112 ,amqp://c:61112"));
    }

    @Test
    void commasInsideFailoverUrisAreNotSeparators() {
        assertEquals(List.of(
                        "failover:(amqp://a:61112,amqp://b:61112)?failover.maxReconnectAttempts=3",
                        "amqp://c:61112"),
                BrokerUrls.parse(
                        "failover:(amqp://a:61112,amqp://b:61112)?failover.maxReconnectAttempts=3,amqp://c:61112"));
    }

    @Test
    void emptyEntriesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> BrokerUrls.parse("amqp://a:61112,,amqp://b:61112"));
        assertThrows(IllegalArgumentException.class, () -> BrokerUrls.parse("amqp://a:61112,"));
        assertThrows(IllegalArgumentException.class, () -> BrokerUrls.parse("  "));
    }

    @Test
    void unbalancedParenthesesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> BrokerUrls.parse("failover:(amqp://a,amqp://b"));
        assertThrows(IllegalArgumentException.class, () -> BrokerUrls.parse("amqp://a),amqp://b"));
    }
}
