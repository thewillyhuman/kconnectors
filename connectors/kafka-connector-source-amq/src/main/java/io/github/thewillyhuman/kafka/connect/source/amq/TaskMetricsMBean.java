/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

/**
 * JMX interface of the per-task metrics, registered under
 * {@code io.github.thewillyhuman.kafka.connect.source.amq:type=source-task,connector=<name>,task=<id>}.
 */
public interface TaskMetricsMBean {

    /** Messages received from the broker, including redeliveries. */
    long getMessagesReceived();

    /** Messages acknowledged on the broker after Kafka confirmed them. */
    long getMessagesAcknowledged();

    /** Messages received with the JMS redelivered flag set. */
    long getRedeliveredMessagesReceived();

    /** Messages dropped because they could not be converted (conversion.error.policy=discard). */
    long getMessagesDiscarded();

    /** Messages that failed conversion, regardless of the policy applied. */
    long getConversionErrors();

    /** Acknowledgements that failed and triggered a connection reset. */
    long getAcknowledgeFailures();

    /** Kafka confirmations for messages no longer tracked (dropped by a connection reset). */
    long getStaleAcknowledgements();

    /** Times the broker connection was reset after a failure. */
    long getConnectionResets();

    /** Messages handed to Kafka Connect and not yet confirmed by Kafka. */
    int getInFlightMessages();

    /** Messages confirmed by Kafka whose broker acknowledgement is not yet sent. */
    int getPendingAcknowledgements();

    /** Number of brokers this task is configured to consume from. */
    int getBrokersConfigured();

    /** Number of brokers this task currently holds a healthy connection to. */
    int getBrokersConnected();

    /** Whether the task is connected to every configured broker. */
    boolean isConnected();

    /** Epoch millis of the last message received, 0 if none yet. */
    long getLastMessageEpochMillis();

    /** Epoch millis of the last broker acknowledgement, 0 if none yet. */
    long getLastAcknowledgeEpochMillis();
}
