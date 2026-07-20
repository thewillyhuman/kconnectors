/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the lifecycle of every consumed JMS message until it is safe to acknowledge it:
 *
 * <ol>
 *   <li><b>in flight</b> — handed to Kafka Connect as a source record, not yet confirmed;</li>
 *   <li><b>pending acknowledgement</b> — Kafka confirmed the record ({@code commitRecord}),
 *       the JMS acknowledgement still has to be sent;</li>
 *   <li>gone — acknowledged on the broker.</li>
 * </ol>
 *
 * <p>{@code commitRecord} is invoked from Kafka producer callback threads while all JMS calls
 * must stay on the task thread, so confirmed messages are parked in a queue that the task
 * thread drains on its next poll.
 *
 * <p>A task may consume from several brokers, so every entry remembers the client (and the
 * client's connection epoch) it was received on: when one broker connection is reset, only
 * its messages are dropped for redelivery, and a confirmation racing with the reset is
 * detected by the epoch check instead of being acknowledged through a dead session.
 */
final class AckRegistry {

    /** A message together with the connection it was received on. */
    record Tracked(Message message, JmsClient client, long epoch, int payloadBytes) {

        /** True when the connection the message arrived on is no longer the live one. */
        boolean isStale() {
            return epoch != client.epoch() || client.isDirty();
        }
    }

    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong inFlightBytes = new AtomicLong();
    private final ConcurrentHashMap<Long, Tracked> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Tracked> pendingAcknowledgements = new ConcurrentLinkedQueue<>();

    /** Returns a new acknowledge id to embed in the source record offset. */
    long nextAcknowledgeId() {
        return sequence.incrementAndGet();
    }

    /** Starts tracking a message that is about to be handed to Kafka Connect. */
    void track(long acknowledgeId, Message message, JmsClient client, int payloadBytes) {
        inFlight.put(acknowledgeId, new Tracked(message, client, client.epoch(), payloadBytes));
        inFlightBytes.addAndGet(payloadBytes);
    }

    /**
     * Marks a message as confirmed by Kafka, moving it to the pending-acknowledgement queue.
     *
     * @return false if the message is no longer tracked (e.g. dropped by a connection reset)
     */
    boolean complete(long acknowledgeId) {
        Tracked tracked = inFlight.remove(acknowledgeId);
        if (tracked == null) {
            return false;
        }
        inFlightBytes.addAndGet(-tracked.payloadBytes());
        pendingAcknowledgements.add(tracked);
        return true;
    }

    /** Next confirmed message to acknowledge on the broker, or null if there is none. */
    Tracked nextPendingAcknowledgement() {
        return pendingAcknowledgements.poll();
    }

    int inFlightCount() {
        return inFlight.size();
    }

    /** Cumulative payload bytes of the in-flight messages (max.unacked.bytes accounting). */
    long inFlightBytes() {
        return inFlightBytes.get();
    }

    int pendingAcknowledgementCount() {
        return pendingAcknowledgements.size();
    }

    /**
     * Drops the in-flight messages of one client after its connection was reset: their
     * session is gone and the broker will redeliver them. Entries already pending
     * acknowledgement (or completed concurrently with this call) are caught later by the
     * {@link Tracked#isStale()} epoch check when the acknowledgement is attempted.
     *
     * @return the number of in-flight messages dropped
     */
    int clear(JmsClient client) {
        int dropped = 0;
        // Conditional remove: a concurrent complete() may win the race for an entry, and
        // its bytes must be subtracted exactly once.
        for (Map.Entry<Long, Tracked> entry : inFlight.entrySet()) {
            if (entry.getValue().client() == client && inFlight.remove(entry.getKey(), entry.getValue())) {
                inFlightBytes.addAndGet(-entry.getValue().payloadBytes());
                dropped++;
            }
        }
        return dropped;
    }
}
