/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Source task with at-least-once delivery semantics, consuming from one or more brokers.
 *
 * <p>Lifecycle of a message:
 * <ol>
 *   <li>{@link #poll()} receives it from a broker on a session in individual-acknowledge
 *       mode — nothing is acknowledged yet — and returns it as a {@link SourceRecord} whose
 *       source offset carries an acknowledge id;</li>
 *   <li>Kafka Connect applies the transformation chain and produces the record to Kafka;</li>
 *   <li>once the Kafka producer confirms the write, the framework calls
 *       {@link #commitRecord}, which marks the message as ready to acknowledge;</li>
 *   <li>the next {@link #poll()} sends the acknowledgement to the broker from the task
 *       thread (JMS sessions are single-threaded).</li>
 * </ol>
 *
 * <p>If the task crashes, is rebalanced, or a connection drops at any point before the
 * acknowledgement, the broker redelivers the messages: nothing is lost, duplicates are
 * possible. Consumption is paused while {@code max.unacked.messages} messages are awaiting
 * confirmation, so a slow or unavailable Kafka propagates backpressure to the brokers
 * instead of accumulating unbounded state.
 *
 * <p>When the connector is configured with several brokers, each task maintains an
 * independent connection per assigned broker (own reconnect backoff, own redelivery scope),
 * so one unreachable broker never blocks consumption from the others.
 */
public class AmqSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(AmqSourceTask.class);

    static final String OFFSET_ACKNOWLEDGE_ID_KEY = "acknowledge.id";
    static final String PARTITION_DESTINATION_KEY = "destination";
    static final String PARTITION_BROKER_KEY = "broker";

    private static final long DISCONNECTED_POLL_PAUSE_MS = 500;
    private static final long BACKPRESSURE_PAUSE_MS = 100;
    private static final long MIN_BLOCKING_RECEIVE_MS = 50;

    /** One broker connection plus its reconnection state. */
    private static final class ManagedClient {
        final JmsClient client;
        long backoffMs;
        long nextConnectAttemptAt;

        ManagedClient(JmsClient client, long initialBackoffMs) {
            this.client = client;
            this.backoffMs = initialBackoffMs;
        }
    }

    private AmqSourceConnectorConfig config;
    private List<ManagedClient> clients;
    private AckRegistry ackRegistry;
    private MessageRecordBuilder recordBuilder;
    private TaskMetrics metrics;

    private volatile boolean running;
    private int sweepOffset;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new AmqSourceConnectorConfig(props);
        String connectorName = props.getOrDefault("name", "amq-source");
        String taskId = props.getOrDefault(AmqSourceConnector.TASK_ID_CONFIG, "0");

        // The connector assigns each task its share of the brokers; a task started outside a
        // connector (tests, standalone experiments) falls back to the full list.
        String assignedBrokers = props.get(AmqSourceConnector.TASK_BROKERS_CONFIG);
        List<String> brokerUrls = assignedBrokers != null
                ? BrokerUrls.parse(assignedBrokers)
                : config.brokerUrls();

        ackRegistry = new AckRegistry();
        recordBuilder = new MessageRecordBuilder(config);
        clients = new ArrayList<>(brokerUrls.size());
        for (String url : brokerUrls) {
            clients.add(new ManagedClient(new JmsClient(config, url), config.connectionRetryBackoffMs()));
        }
        metrics = new TaskMetrics(ackRegistry::inFlightCount, ackRegistry::inFlightBytes,
                ackRegistry::pendingAcknowledgementCount, clients::size, this::connectedClientCount);
        metrics.register(connectorName, taskId);
        running = true;

        log.info("Starting AMQ source task {}/{} version {}: brokers={}, destination={} ({}), "
                        + "kafka topic={}, max unacked messages={}",
                connectorName, taskId, version(), JmsClient.sanitizeUrl(String.join(", ", brokerUrls)),
                config.destinationName(), config.destinationType(), config.kafkaTopic(),
                config.maxUnackedMessages());

        for (ManagedClient managed : clients) {
            try {
                managed.client.connect();
                log.info("Connected to {}", managed.client.label());
            } catch (JMSException e) {
                managed.nextConnectAttemptAt = System.currentTimeMillis() + managed.backoffMs;
                log.warn("Initial connection to {} failed, the task will keep retrying: {}",
                        managed.client.label(), e.toString());
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        drainPendingAcknowledgements();
        if (!running) {
            return null;
        }
        List<JmsClient> connected = connectedClients();
        if (connected.isEmpty()) {
            Thread.sleep(DISCONNECTED_POLL_PAUSE_MS);
            return null;
        }

        int capacity = config.maxUnackedMessages() - ackRegistry.inFlightCount();
        if (capacity <= 0 || byteLimitReached()) {
            log.debug("Backpressure: {} messages / {} payload bytes awaiting Kafka confirmation "
                            + "(max {} messages / {} bytes), pausing consumption",
                    ackRegistry.inFlightCount(), ackRegistry.inFlightBytes(),
                    config.maxUnackedMessages(), config.maxUnackedBytes());
            Thread.sleep(BACKPRESSURE_PAUSE_MS);
            drainPendingAcknowledgements();
            return null;
        }

        int batchLimit = Math.min(config.batchMaxSize(), capacity);
        List<SourceRecord> records = new ArrayList<>();
        int start = sweepOffset++;

        // First a non-blocking sweep over all brokers (drains what the clients prefetched),
        // then, if nothing was buffered, wait for a message splitting the poll timeout across
        // brokers. The sweep starting point rotates so no broker is systematically favoured.
        for (int i = 0; i < connected.size() && records.size() < batchLimit && !byteLimitReached()
                && running; i++) {
            receiveFrom(connected.get(Math.floorMod(start + i, connected.size())), records, batchLimit, 0);
        }
        if (records.isEmpty() && running) {
            long perBrokerWaitMs = Math.max(config.pollTimeoutMs() / connected.size(), MIN_BLOCKING_RECEIVE_MS);
            for (int i = 0; i < connected.size() && records.isEmpty() && running; i++) {
                receiveFrom(connected.get(Math.floorMod(start + i, connected.size())), records, batchLimit,
                        perBrokerWaitMs);
            }
        }

        if (!records.isEmpty() && log.isDebugEnabled()) {
            log.debug("Returning batch of {} records ({} messages now in flight)",
                    records.size(), ackRegistry.inFlightCount());
        }
        return records.isEmpty() ? null : records;
    }

    /** Receives from one broker until the batch is full or it has nothing more buffered. */
    private void receiveFrom(JmsClient client, List<SourceRecord> records, int batchLimit, long waitMs) {
        try {
            Message message = waitMs > 0 ? client.receive(waitMs) : client.receiveNoWait();
            while (message != null && running) {
                handleMessage(client, message, records);
                if (records.size() >= batchLimit || byteLimitReached()) {
                    return;
                }
                message = client.receiveNoWait();
            }
        } catch (JMSException e) {
            log.warn("JMS failure while receiving from {}, resetting that connection: {}",
                    client.label(), e.toString());
            client.markDirty();
        }
    }

    /**
     * True when the unconfirmed payload bytes are at or above {@code max.unacked.bytes}.
     * Checked after admitting each message (never before the first), so a single message
     * larger than the limit still flows instead of deadlocking the task.
     */
    private boolean byteLimitReached() {
        long limit = config.maxUnackedBytes();
        return limit > 0 && ackRegistry.inFlightBytes() >= limit;
    }

    private void handleMessage(JmsClient client, Message message, List<SourceRecord> records) throws JMSException {
        boolean redelivered = message.getJMSRedelivered();
        metrics.messageReceived(redelivered);
        if (redelivered) {
            log.debug("Received redelivered message {} from {}", safeMessageId(message), client.label());
        }
        long acknowledgeId = ackRegistry.nextAcknowledgeId();
        SourceRecord record;
        try {
            record = recordBuilder.toSourceRecord(message, acknowledgeId, client.label());
        } catch (ConversionException | JMSException e) {
            metrics.conversionError();
            if (config.conversionErrorPolicy() == AmqSourceConnectorConfig.ConversionErrorPolicy.DISCARD) {
                log.error("Discarding message {} from '{}' on {} that could not be converted: {}",
                        safeMessageId(message), config.destinationName(), client.label(), e.toString());
                client.acknowledge(message);
                metrics.messageDiscarded();
                return;
            }
            throw new ConnectException("Could not convert JMS message " + safeMessageId(message)
                    + " from '" + config.destinationName() + "' on " + client.label()
                    + "; failing the task so the message is not lost (conversion.error.policy=fail)", e);
        }
        ackRegistry.track(acknowledgeId, message, client, payloadSizeBytes(record.value()));
        records.add(record);
    }

    /**
     * Approximate payload size used for the {@code max.unacked.bytes} accounting: exact for
     * byte-array values, character count for strings (close enough for a heap bound).
     */
    private static int payloadSizeBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes.length;
        }
        if (value instanceof String string) {
            return string.length();
        }
        return 0;
    }

    /**
     * Called by the framework once Kafka has confirmed (or the transformation chain has
     * intentionally filtered out) the record. Runs on a producer callback thread, so the JMS
     * acknowledgement itself is deferred to the task thread.
     */
    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        Map<String, ?> offset = record.sourceOffset();
        Object acknowledgeId = offset == null ? null : offset.get(OFFSET_ACKNOWLEDGE_ID_KEY);
        if (acknowledgeId instanceof Long id && !ackRegistry.complete(id)) {
            metrics.staleAcknowledgement();
            log.debug("Kafka confirmed a message that is no longer tracked (acknowledge id {}); "
                    + "it was dropped by a connection reset and will be redelivered", id);
        }
    }

    @Override
    public void commit() {
        // Invoked periodically by the framework (offset flush): a good heartbeat for operators.
        if (metrics != null) {
            log.info("Status: received={}, acknowledged={}, inFlight={}, inFlightBytes={}, "
                            + "pendingAcks={}, redelivered={}, discarded={}, staleAcks={}, "
                            + "connectionResets={}, connectedBrokers={}/{}",
                    metrics.getMessagesReceived(), metrics.getMessagesAcknowledged(),
                    metrics.getInFlightMessages(), metrics.getInFlightBytes(),
                    metrics.getPendingAcknowledgements(),
                    metrics.getRedeliveredMessagesReceived(), metrics.getMessagesDiscarded(),
                    metrics.getStaleAcknowledgements(), metrics.getConnectionResets(),
                    metrics.getBrokersConnected(), metrics.getBrokersConfigured());
        }
    }

    @Override
    public void stop() {
        int unacknowledged = ackRegistry == null
                ? 0
                : ackRegistry.inFlightCount() + ackRegistry.pendingAcknowledgementCount();
        log.info("Stopping AMQ source task; {} unacknowledged messages will be redelivered by the brokers",
                unacknowledged);
        running = false;
        if (clients != null) {
            for (ManagedClient managed : clients) {
                managed.client.close(); // Thread-safe; also unblocks a poll waiting in receive().
            }
        }
        if (metrics != null) {
            metrics.unregister();
        }
    }

    /** Sends pending acknowledgements to the brokers; must run on the task thread. */
    private void drainPendingAcknowledgements() {
        AckRegistry.Tracked tracked;
        while ((tracked = ackRegistry.nextPendingAcknowledgement()) != null) {
            if (tracked.isStale() || !tracked.client().isConnected()) {
                // The connection the message arrived on is gone: the broker owns it again and
                // will redeliver it. Acknowledging through the new session would be wrong.
                metrics.staleAcknowledgement();
                continue;
            }
            try {
                tracked.message().acknowledge();
                metrics.messageAcknowledged();
            } catch (Exception e) {
                metrics.acknowledgeFailure();
                tracked.client().markDirty();
                log.warn("Failed to acknowledge message {} on {}; that connection will be reset and "
                                + "its unacknowledged messages redelivered: {}",
                        safeMessageId(tracked.message()), tracked.client().label(), e.toString());
            }
        }
    }

    /**
     * Repairs broken connections (with per-broker exponential backoff) and returns the
     * clients that are currently usable. Never sleeps: brokers being down must not delay
     * consumption from the healthy ones.
     */
    private List<JmsClient> connectedClients() {
        long now = System.currentTimeMillis();
        List<JmsClient> connected = new ArrayList<>(clients.size());
        for (ManagedClient managed : clients) {
            JmsClient client = managed.client;
            if (client.isDirty()) {
                metrics.connectionReset();
                client.close();
                int dropped = ackRegistry.clear(client);
                if (dropped > 0) {
                    log.warn("Connection reset on {} dropped {} locally tracked messages; the broker "
                            + "will redeliver them (at-least-once)", client.label(), dropped);
                }
            }
            if (client.isConnected()) {
                connected.add(client);
                continue;
            }
            if (now < managed.nextConnectAttemptAt) {
                continue;
            }
            try {
                client.connect();
                managed.backoffMs = config.connectionRetryBackoffMs();
                log.info("Connected to {}, consuming {} '{}'", client.label(),
                        config.destinationType(), config.destinationName());
                connected.add(client);
            } catch (JMSException e) {
                client.close();
                managed.nextConnectAttemptAt = System.currentTimeMillis() + managed.backoffMs;
                log.warn("Connection attempt to {} failed, retrying in {} ms: {}",
                        client.label(), managed.backoffMs, e.toString());
                managed.backoffMs = Math.min(managed.backoffMs * 2, config.connectionRetryBackoffMaxMs());
            }
        }
        return connected;
    }

    private int connectedClientCount() {
        List<ManagedClient> current = clients;
        if (current == null) {
            return 0;
        }
        int count = 0;
        for (ManagedClient managed : current) {
            if (managed.client.isConnected()) {
                count++;
            }
        }
        return count;
    }

    private static String safeMessageId(Message message) {
        try {
            String id = message.getJMSMessageID();
            return id == null ? "<no id>" : id;
        } catch (JMSException e) {
            return "<unavailable>";
        }
    }
}
