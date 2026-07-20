/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AckRegistryTest {

    private final AckRegistry registry = new AckRegistry();
    private final JmsClient clientA = newClient("amqp://a:61112");
    private final JmsClient clientB = newClient("amqp://b:61112");

    private static JmsClient newClient(String url) {
        Map<String, String> props = new HashMap<>();
        props.put(AmqSourceConnectorConfig.URL_CONFIG, url);
        props.put(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG, "queue");
        props.put(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "topic");
        return new JmsClient(new AmqSourceConnectorConfig(props), url);
    }

    @Test
    void confirmedMessagesMoveFromInFlightToPendingAcknowledgement() {
        FakeMessage first = new FakeTextMessage("a");
        FakeMessage second = new FakeTextMessage("b");
        long firstId = registry.nextAcknowledgeId();
        registry.track(firstId, first, clientA, 1);
        long secondId = registry.nextAcknowledgeId();
        registry.track(secondId, second, clientA, 1);
        assertEquals(2, registry.inFlightCount());
        assertEquals(0, registry.pendingAcknowledgementCount());

        assertTrue(registry.complete(firstId));
        assertEquals(1, registry.inFlightCount());
        assertEquals(1, registry.pendingAcknowledgementCount());
        assertSame(first, registry.nextPendingAcknowledgement().message());
        assertNull(registry.nextPendingAcknowledgement());
    }

    @Test
    void completingAnUntrackedMessageIsReportedAsStale() {
        assertFalse(registry.complete(42L));
        long id = registry.nextAcknowledgeId();
        registry.track(id, new FakeTextMessage("a"), clientA, 1);
        assertTrue(registry.complete(id));
        assertFalse(registry.complete(id), "double completion must not enqueue a second acknowledgement");
        assertEquals(1, registry.pendingAcknowledgementCount());
    }

    @Test
    void clearOnlyDropsTheInFlightMessagesOfTheGivenClient() {
        long onA = registry.nextAcknowledgeId();
        registry.track(onA, new FakeTextMessage("a"), clientA, 1);
        long onB = registry.nextAcknowledgeId();
        registry.track(onB, new FakeTextMessage("b"), clientB, 1);

        assertEquals(1, registry.clear(clientA));
        assertEquals(1, registry.inFlightCount());
        assertFalse(registry.complete(onA), "messages dropped by a reset must not be acknowledged");
        assertTrue(registry.complete(onB), "the other broker's messages must stay tracked");
    }

    @Test
    void messagesFromAnOldConnectionEpochAreStale() {
        long id = registry.nextAcknowledgeId();
        registry.track(id, new FakeTextMessage("a"), clientA, 1);
        assertTrue(registry.complete(id));
        AckRegistry.Tracked tracked = registry.nextPendingAcknowledgement();
        assertFalse(tracked.isStale(), "a tracked message on the live connection epoch is acknowledgeable");

        clientA.close(); // connection reset bumps the epoch
        assertTrue(tracked.isStale(), "after a reset the acknowledgement must be dropped, not sent");
    }

    @Test
    void byteAccountingFollowsTheMessageLifecycle() {
        long onA = registry.nextAcknowledgeId();
        registry.track(onA, new FakeTextMessage("a"), clientA, 100);
        long onB = registry.nextAcknowledgeId();
        registry.track(onB, new FakeTextMessage("b"), clientB, 40);
        assertEquals(140, registry.inFlightBytes());

        // Confirmation releases the bytes even though the acknowledgement is still pending.
        assertTrue(registry.complete(onA));
        assertEquals(40, registry.inFlightBytes());

        // A connection reset releases the bytes of the dropped messages exactly once.
        assertEquals(1, registry.clear(clientB));
        assertEquals(0, registry.inFlightBytes());
        assertFalse(registry.complete(onB), "a cleared message must not be completed");
        assertEquals(0, registry.inFlightBytes(), "a late confirmation must not subtract twice");
    }

    @Test
    void messagesFromADirtyConnectionAreStale() {
        long id = registry.nextAcknowledgeId();
        registry.track(id, new FakeTextMessage("a"), clientA, 1);
        registry.complete(id);
        AckRegistry.Tracked tracked = registry.nextPendingAcknowledgement();
        clientA.markDirty();
        assertTrue(tracked.isStale());
    }
}
