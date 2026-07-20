/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thin wrapper around the Qpid JMS connection/session/consumer used by a task.
 *
 * <p>Sessions are created in {@code INDIVIDUAL_ACKNOWLEDGE} mode (a Qpid JMS extension over
 * AMQP 1.0 dispositions): a message stays on the broker until {@link Message#acknowledge()}
 * is called on that exact message, which the task only does after Kafka has confirmed the
 * corresponding record. Unacknowledged messages are redelivered by the broker.
 *
 * <p>Threading: all JMS operations happen on the Connect task thread, except {@link #close()}
 * which may be called from the framework's stop path ({@code Connection.close()} is one of
 * the few JMS operations that is required to be thread-safe) and {@link #onException} which is
 * invoked from a Qpid internal thread and only flips an atomic flag.
 */
final class JmsClient implements ExceptionListener {

    private static final Logger log = LoggerFactory.getLogger(JmsClient.class);

    /**
     * Qpid JMS proprietary session mode (accepted by {@code Connection.createSession(int)}):
     * acknowledging a message settles only that message, not the whole session. The behaviour
     * is pinned by the redelivery integration test.
     */
    static final int INDIVIDUAL_ACKNOWLEDGE = 101;

    private final AmqSourceConnectorConfig config;
    private final String url;
    private final String label;
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final AtomicLong epoch = new AtomicLong();

    private volatile Connection connection;
    private volatile MessageConsumer consumer;
    private volatile boolean connected;

    JmsClient(AmqSourceConnectorConfig config, String url) {
        this.config = config;
        this.url = url;
        this.label = sanitizeUrl(url);
    }

    /** Sanitized broker URL, safe for logs, headers and metrics. */
    String label() {
        return label;
    }

    /**
     * Connection generation, bumped on every close. Messages received on an older epoch
     * belong to a dead session and must never be acknowledged through the new one.
     */
    long epoch() {
        return epoch.get();
    }

    synchronized void connect() throws JMSException {
        close();
        JmsConnectionFactory factory = new JmsConnectionFactory(url);
        Connection newConnection = null;
        try {
            newConnection = config.username() != null
                    ? factory.createConnection(config.username(), config.password())
                    : factory.createConnection();
            if (config.clientId() != null) {
                newConnection.setClientID(config.clientId());
            }
            newConnection.setExceptionListener(this);
            Session session = newConnection.createSession(INDIVIDUAL_ACKNOWLEDGE);
            Destination destination = config.destinationType() == AmqSourceConnectorConfig.DestinationType.QUEUE
                    ? session.createQueue(config.destinationName())
                    : session.createTopic(config.destinationName());
            MessageConsumer newConsumer = config.subscriptionName() != null
                    ? session.createDurableConsumer((Topic) destination, config.subscriptionName(),
                            config.messageSelector(), false)
                    : session.createConsumer(destination, config.messageSelector());
            newConnection.start();
            connection = newConnection;
            consumer = newConsumer;
            dirty.set(false);
            connected = true;
        } catch (JMSException | RuntimeException e) {
            closeQuietly(newConnection);
            connected = false;
            throw e;
        }
    }

    Message receive(long timeoutMs) throws JMSException {
        return activeConsumer().receive(timeoutMs);
    }

    Message receiveNoWait() throws JMSException {
        return activeConsumer().receiveNoWait();
    }

    void acknowledge(Message message) throws JMSException {
        message.acknowledge();
    }

    boolean isConnected() {
        return connected && !dirty.get();
    }

    boolean isDirty() {
        return dirty.get();
    }

    /** Flags the connection as broken so the task resets it on its next poll. */
    void markDirty() {
        dirty.set(true);
    }

    @Override
    public void onException(JMSException exception) {
        log.error("Asynchronous JMS connection failure on {}: {}", label, exception.toString());
        markDirty();
    }

    synchronized void close() {
        connected = false;
        epoch.incrementAndGet();
        Connection current = connection;
        connection = null;
        consumer = null;
        closeQuietly(current);
    }

    private MessageConsumer activeConsumer() throws JMSException {
        MessageConsumer current = consumer;
        if (current == null) {
            throw new JMSException("Not connected to the broker");
        }
        return current;
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            log.debug("Ignoring failure while closing JMS connection: {}", e.toString());
        }
    }

    /** Strips credentials possibly embedded in a broker URI so it can be logged safely. */
    static String sanitizeUrl(String url) {
        return url == null ? null : url.replaceAll("://[^/@\\s]+@", "://***@");
    }
}
