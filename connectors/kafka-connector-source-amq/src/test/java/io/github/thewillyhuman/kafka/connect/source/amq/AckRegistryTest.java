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
        registry.track(firstId, first, clientA);
        long secondId = registry.nextAcknowledgeId();
        registry.track(secondId, second, clientA);
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
        registry.track(id, new FakeTextMessage("a"), clientA);
        assertTrue(registry.complete(id));
        assertFalse(registry.complete(id), "double completion must not enqueue a second acknowledgement");
        assertEquals(1, registry.pendingAcknowledgementCount());
    }

    @Test
    void clearOnlyDropsTheInFlightMessagesOfTheGivenClient() {
        long onA = registry.nextAcknowledgeId();
        registry.track(onA, new FakeTextMessage("a"), clientA);
        long onB = registry.nextAcknowledgeId();
        registry.track(onB, new FakeTextMessage("b"), clientB);

        assertEquals(1, registry.clear(clientA));
        assertEquals(1, registry.inFlightCount());
        assertFalse(registry.complete(onA), "messages dropped by a reset must not be acknowledged");
        assertTrue(registry.complete(onB), "the other broker's messages must stay tracked");
    }

    @Test
    void messagesFromAnOldConnectionEpochAreStale() {
        long id = registry.nextAcknowledgeId();
        registry.track(id, new FakeTextMessage("a"), clientA);
        assertTrue(registry.complete(id));
        AckRegistry.Tracked tracked = registry.nextPendingAcknowledgement();
        assertFalse(tracked.isStale(), "a tracked message on the live connection epoch is acknowledgeable");

        clientA.close(); // connection reset bumps the epoch
        assertTrue(tracked.isStale(), "after a reset the acknowledgement must be dropped, not sent");
    }

    @Test
    void messagesFromADirtyConnectionAreStale() {
        long id = registry.nextAcknowledgeId();
        registry.track(id, new FakeTextMessage("a"), clientA);
        registry.complete(id);
        AckRegistry.Tracked tracked = registry.nextPendingAcknowledgement();
        clientA.markDirty();
        assertTrue(tracked.isStale());
    }
}
