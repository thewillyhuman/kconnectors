/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * Per-task counters and gauges, exposed over JMX. Metric failures are never allowed to
 * affect the data path: registration problems are logged and ignored.
 */
final class TaskMetrics implements TaskMetricsMBean {

    private static final Logger log = LoggerFactory.getLogger(TaskMetrics.class);
    private static final String JMX_DOMAIN = "io.github.thewillyhuman.kafka.connect.source.amq";

    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesAcknowledged = new AtomicLong();
    private final AtomicLong redeliveredMessagesReceived = new AtomicLong();
    private final AtomicLong messagesDiscarded = new AtomicLong();
    private final AtomicLong conversionErrors = new AtomicLong();
    private final AtomicLong acknowledgeFailures = new AtomicLong();
    private final AtomicLong staleAcknowledgements = new AtomicLong();
    private final AtomicLong connectionResets = new AtomicLong();
    private volatile long lastMessageEpochMillis;
    private volatile long lastAcknowledgeEpochMillis;

    private final IntSupplier inFlightMessages;
    private final IntSupplier pendingAcknowledgements;
    private final IntSupplier brokersConfigured;
    private final IntSupplier brokersConnected;

    private volatile ObjectName objectName;

    TaskMetrics(IntSupplier inFlightMessages, IntSupplier pendingAcknowledgements,
                IntSupplier brokersConfigured, IntSupplier brokersConnected) {
        this.inFlightMessages = inFlightMessages;
        this.pendingAcknowledgements = pendingAcknowledgements;
        this.brokersConfigured = brokersConfigured;
        this.brokersConnected = brokersConnected;
    }

    void register(String connectorName, String taskId) {
        try {
            ObjectName name = new ObjectName(JMX_DOMAIN + ":type=source-task,connector="
                    + ObjectName.quote(connectorName) + ",task=" + ObjectName.quote(taskId));
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
            }
            server.registerMBean(this, name);
            objectName = name;
        } catch (Exception e) {
            log.warn("Could not register task metrics MBean; continuing without JMX metrics", e);
        }
    }

    void unregister() {
        ObjectName name = objectName;
        if (name == null) {
            return;
        }
        objectName = null;
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        } catch (Exception e) {
            log.debug("Could not unregister task metrics MBean {}: {}", name, e.toString());
        }
    }

    void messageReceived(boolean redelivered) {
        messagesReceived.incrementAndGet();
        if (redelivered) {
            redeliveredMessagesReceived.incrementAndGet();
        }
        lastMessageEpochMillis = System.currentTimeMillis();
    }

    void messageAcknowledged() {
        messagesAcknowledged.incrementAndGet();
        lastAcknowledgeEpochMillis = System.currentTimeMillis();
    }

    void messageDiscarded() {
        messagesDiscarded.incrementAndGet();
    }

    void conversionError() {
        conversionErrors.incrementAndGet();
    }

    void acknowledgeFailure() {
        acknowledgeFailures.incrementAndGet();
    }

    void staleAcknowledgement() {
        staleAcknowledgements.incrementAndGet();
    }

    void connectionReset() {
        connectionResets.incrementAndGet();
    }

    @Override
    public long getMessagesReceived() {
        return messagesReceived.get();
    }

    @Override
    public long getMessagesAcknowledged() {
        return messagesAcknowledged.get();
    }

    @Override
    public long getRedeliveredMessagesReceived() {
        return redeliveredMessagesReceived.get();
    }

    @Override
    public long getMessagesDiscarded() {
        return messagesDiscarded.get();
    }

    @Override
    public long getConversionErrors() {
        return conversionErrors.get();
    }

    @Override
    public long getAcknowledgeFailures() {
        return acknowledgeFailures.get();
    }

    @Override
    public long getStaleAcknowledgements() {
        return staleAcknowledgements.get();
    }

    @Override
    public long getConnectionResets() {
        return connectionResets.get();
    }

    @Override
    public int getInFlightMessages() {
        return inFlightMessages.getAsInt();
    }

    @Override
    public int getPendingAcknowledgements() {
        return pendingAcknowledgements.getAsInt();
    }

    @Override
    public int getBrokersConfigured() {
        return brokersConfigured.getAsInt();
    }

    @Override
    public int getBrokersConnected() {
        return brokersConnected.getAsInt();
    }

    @Override
    public boolean isConnected() {
        int configured = brokersConfigured.getAsInt();
        return configured > 0 && brokersConnected.getAsInt() == configured;
    }

    @Override
    public long getLastMessageEpochMillis() {
        return lastMessageEpochMillis;
    }

    @Override
    public long getLastAcknowledgeEpochMillis() {
        return lastAcknowledgeEpochMillis;
    }
}
