/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end tests of the delivery semantics against real AMQP 1.0 brokers
 * (embedded ActiveMQ Artemis instances).
 */
@Timeout(60)
class AmqSourceTaskIntegrationTest {

    private static EmbeddedActiveMQ primaryBroker;
    private static EmbeddedActiveMQ secondaryBroker;
    private static int primaryPort;
    private static int secondaryPort;

    @BeforeAll
    static void startBrokers() throws Exception {
        primaryPort = freePort();
        secondaryPort = freePort();
        primaryBroker = startBroker("amq-source-itest-primary", primaryPort);
        secondaryBroker = startBroker("amq-source-itest-secondary", secondaryPort);
    }

    @AfterAll
    static void stopBrokers() throws Exception {
        if (primaryBroker != null) {
            primaryBroker.stop();
        }
        if (secondaryBroker != null) {
            secondaryBroker.stop();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static EmbeddedActiveMQ startBroker(String name, int port) throws Exception {
        Configuration configuration = new ConfigurationImpl()
                .setName(name)
                .setPersistenceEnabled(false)
                .setSecurityEnabled(false)
                .addAcceptorConfiguration("amqp", "tcp://127.0.0.1:" + port + "?protocols=AMQP");
        EmbeddedActiveMQ broker = new EmbeddedActiveMQ();
        broker.setConfiguration(configuration);
        broker.start();
        return broker;
    }

    @Test
    void messagesAreOnlyAcknowledgedAfterKafkaConfirmsThem() throws Exception {
        String queue = "itest.ack";
        sendTextMessages(primaryPort, queue, "payload-", 5);

        AmqSourceTask task = startTask(queue, Map.of());
        try {
            List<SourceRecord> records = pollUntil(task, 5);
            Set<String> payloads = new HashSet<>();
            for (SourceRecord record : records) {
                payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
            }
            assertEquals(5, payloads.size(), "all payloads must be distinct");

            // Nothing confirmed by Kafka yet: every message must still be owned by the broker.
            assertEquals(5, messageCount(primaryBroker, queue));

            for (SourceRecord record : records) {
                task.commitRecord(record, null);
            }
            // Acknowledgements are sent from the task thread on the next polls.
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void uncommittedMessagesAreRedeliveredAfterARestart() throws Exception {
        String queue = "itest.redelivery";
        sendTextMessages(primaryPort, queue, "payload-", 3);

        AmqSourceTask first = startTask(queue, Map.of());
        String committedPayload;
        try {
            List<SourceRecord> records = pollUntil(first, 3);
            committedPayload = new String((byte[]) records.get(0).value(), StandardCharsets.UTF_8);
            first.commitRecord(records.get(0), null);
            awaitMessageCount(first, primaryBroker, queue, 2);
        } finally {
            first.stop();
        }

        // A new task (crash/rebalance scenario) must receive exactly the two uncommitted messages.
        AmqSourceTask second = startTask(queue, Map.of());
        try {
            List<SourceRecord> redelivered = pollUntil(second, 2);
            Set<String> payloads = new HashSet<>();
            for (SourceRecord record : redelivered) {
                payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                // The redelivered flag is broker/shutdown dependent (a clean stop releases the
                // messages as "never delivered"), so only its presence is asserted here.
                Header header = record.headers().lastWithName("jms.redelivered");
                assertTrue(header != null && header.value() instanceof Boolean,
                        "records must carry the jms.redelivered header");
            }
            assertTrue(payloads.size() == 2 && !payloads.contains(committedPayload),
                    "the committed message must not be redelivered, got: " + payloads);

            for (SourceRecord record : redelivered) {
                second.commitRecord(record, null);
            }
            awaitMessageCount(second, primaryBroker, queue, 0);
        } finally {
            second.stop();
        }
    }

    @Test
    void consumptionPausesWhenTooManyMessagesAwaitConfirmation() throws Exception {
        String queue = "itest.backpressure";
        sendTextMessages(primaryPort, queue, "payload-", 5);

        AmqSourceTask task = startTask(queue, Map.of(
                AmqSourceConnectorConfig.MAX_UNACKED_MESSAGES_CONFIG, "2",
                AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "100"));
        try {
            List<SourceRecord> firstBatch = pollUntil(task, 2);
            assertNull(task.poll(), "consumption must pause at max.unacked.messages");
            assertEquals(5, messageCount(primaryBroker, queue));

            // Once Kafka confirms records, consumption resumes; committing while polling lets
            // the remaining messages flow through the 2-message window.
            Set<String> payloads = new HashSet<>();
            for (SourceRecord record : firstBatch) {
                payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                task.commitRecord(record, null);
            }
            long deadline = System.currentTimeMillis() + 20_000;
            while (payloads.size() < 5 && System.currentTimeMillis() < deadline) {
                List<SourceRecord> batch = task.poll();
                if (batch == null) {
                    continue;
                }
                for (SourceRecord record : batch) {
                    payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                    task.commitRecord(record, null);
                }
            }
            assertEquals(5, payloads.size(), "all messages must eventually be consumed exactly once");
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void consumptionPausesWhenUnackedPayloadBytesExceedTheLimit() throws Exception {
        String queue = "itest.bytebackpressure";
        // ~1 KB payloads with a 1.5 KB budget: the second message crosses the limit.
        String payloadPrefix = "x".repeat(1000) + "-";
        sendTextMessages(primaryPort, queue, payloadPrefix, 5);

        AmqSourceTask task = startTask(queue, Map.of(
                AmqSourceConnectorConfig.MAX_UNACKED_BYTES_CONFIG, "1500",
                AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "100"));
        try {
            List<SourceRecord> firstBatch = pollUntil(task, 2);
            assertNull(task.poll(), "consumption must pause at max.unacked.bytes");
            assertEquals(5, messageCount(primaryBroker, queue));

            // Confirmations release the byte budget and the rest flows through the window.
            Set<String> payloads = new HashSet<>();
            for (SourceRecord record : firstBatch) {
                payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                task.commitRecord(record, null);
            }
            long deadline = System.currentTimeMillis() + 20_000;
            while (payloads.size() < 5 && System.currentTimeMillis() < deadline) {
                List<SourceRecord> batch = task.poll();
                if (batch == null) {
                    continue;
                }
                for (SourceRecord record : batch) {
                    payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                    task.commitRecord(record, null);
                }
            }
            assertEquals(5, payloads.size(), "all messages must eventually be consumed");
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void aMessageLargerThanTheByteLimitStillFlows() throws Exception {
        String queue = "itest.oversized";
        // Every payload is far larger than the byte limit: each poll must still admit
        // exactly one message instead of deadlocking.
        String payloadPrefix = "y".repeat(2000) + "-";
        sendTextMessages(primaryPort, queue, payloadPrefix, 3);

        AmqSourceTask task = startTask(queue, Map.of(
                AmqSourceConnectorConfig.MAX_UNACKED_BYTES_CONFIG, "100",
                AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "100"));
        try {
            Set<String> payloads = new HashSet<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (payloads.size() < 3 && System.currentTimeMillis() < deadline) {
                List<SourceRecord> batch = task.poll();
                if (batch == null) {
                    continue;
                }
                assertEquals(1, batch.size(), "an oversized message must be admitted alone");
                for (SourceRecord record : batch) {
                    payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                    task.commitRecord(record, null);
                }
            }
            assertEquals(3, payloads.size(), "oversized messages must keep flowing one at a time");
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void oneTaskConsumesFromSeveralBrokersConcurrently() throws Exception {
        String queue = "itest.multibroker";
        sendTextMessages(primaryPort, queue, "primary-", 3);
        sendTextMessages(secondaryPort, queue, "secondary-", 3);

        AmqSourceTask task = startTask(queue, Map.of(AmqSourceConnectorConfig.URL_CONFIG,
                "amqp://127.0.0.1:" + primaryPort + ",amqp://127.0.0.1:" + secondaryPort));
        try {
            List<SourceRecord> records = pollUntil(task, 6);
            Set<String> payloads = new HashSet<>();
            Set<Object> brokers = new HashSet<>();
            for (SourceRecord record : records) {
                payloads.add(new String((byte[]) record.value(), StandardCharsets.UTF_8));
                brokers.add(record.sourcePartition().get(AmqSourceTask.PARTITION_BROKER_KEY));
                Header header = record.headers().lastWithName("jms.broker");
                assertTrue(header != null && header.value() != null,
                        "records must carry the jms.broker provenance header");
                task.commitRecord(record, null);
            }
            assertEquals(6, payloads.size(), "messages from both brokers must arrive: " + payloads);
            assertEquals(2, brokers.size(), "source partitions must distinguish the two brokers");

            // Acknowledgements must land on the broker each message came from.
            awaitMessageCount(task, primaryBroker, queue, 0);
            awaitMessageCount(task, secondaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void anUnreachableBrokerDoesNotBlockTheOthers() throws Exception {
        String queue = "itest.partialoutage";
        int deadPort = freePort();
        sendTextMessages(primaryPort, queue, "payload-", 3);

        AmqSourceTask task = startTask(queue, Map.of(AmqSourceConnectorConfig.URL_CONFIG,
                "amqp://127.0.0.1:" + primaryPort + ",amqp://127.0.0.1:" + deadPort));
        try {
            List<SourceRecord> records = pollUntil(task, 3);
            for (SourceRecord record : records) {
                task.commitRecord(record, null);
            }
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void taskFailsWhenAllBrokersAreDownForTooLong() throws Exception {
        int deadPort = freePort();
        AmqSourceTask task = startTask("itest.totaloutage", Map.of(
                AmqSourceConnectorConfig.URL_CONFIG, "amqp://127.0.0.1:" + deadPort,
                AmqSourceConnectorConfig.CONNECTION_MAX_DOWNTIME_MS_CONFIG, "300"));
        try {
            ConnectException failure = assertThrows(ConnectException.class, () -> {
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline) {
                    task.poll();
                }
            }, "the task must fail once every broker has been down longer than the limit");
            assertTrue(failure.getMessage().contains("127.0.0.1:" + deadPort),
                    "the failure must name the unreachable brokers: " + failure.getMessage());
            assertTrue(failure.getMessage().contains(
                            AmqSourceConnectorConfig.CONNECTION_MAX_DOWNTIME_MS_CONFIG),
                    "the failure must name the limit that fired: " + failure.getMessage());
        } finally {
            task.stop();
        }
    }

    @Test
    void aPartialOutageNeverFailsTheTask() throws Exception {
        String queue = "itest.partialdowntime";
        int deadPort = freePort();
        sendTextMessages(primaryPort, queue, "payload-", 2);

        // Downtime limit far below the polling time: the healthy broker must keep
        // resetting the downtime clock, so the task never fails.
        AmqSourceTask task = startTask(queue, Map.of(
                AmqSourceConnectorConfig.URL_CONFIG,
                "amqp://127.0.0.1:" + primaryPort + ",amqp://127.0.0.1:" + deadPort,
                AmqSourceConnectorConfig.CONNECTION_MAX_DOWNTIME_MS_CONFIG, "200",
                AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "100"));
        try {
            List<SourceRecord> records = pollUntil(task, 2);
            for (SourceRecord record : records) {
                task.commitRecord(record, null);
            }
            long deadline = System.currentTimeMillis() + 1_000;
            while (System.currentTimeMillis() < deadline) {
                task.poll(); // must never throw while one broker is healthy
            }
            awaitMessageCount(task, primaryBroker, queue, 0);
        } finally {
            task.stop();
        }
    }

    @Test
    void anOutageShorterThanTheLimitDoesNotFailTheTask() throws Exception {
        String queue = "itest.shortoutage";
        int port = freePort();
        EmbeddedActiveMQ broker = startBroker("itest-restart", port);
        try {
            sendTextMessages(port, queue, "before-", 1);
            AmqSourceTask task = startTask(queue, Map.of(
                    AmqSourceConnectorConfig.URL_CONFIG, "amqp://127.0.0.1:" + port,
                    AmqSourceConnectorConfig.CONNECTION_MAX_DOWNTIME_MS_CONFIG, "20000",
                    AmqSourceConnectorConfig.CONNECTION_RETRY_BACKOFF_MS_CONFIG, "100",
                    AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "100"));
            try {
                for (SourceRecord record : pollUntil(task, 1)) {
                    task.commitRecord(record, null);
                }
                awaitMessageCount(task, broker, queue, 0);

                // Outage far shorter than the limit: polling must keep returning null.
                broker.stop();
                long outageEnd = System.currentTimeMillis() + 1_500;
                while (System.currentTimeMillis() < outageEnd) {
                    assertNull(task.poll(), "no records can arrive while the broker is down");
                }

                // The broker returns before the limit: the task must reconnect and resume.
                broker = startBroker("itest-restart", port);
                sendTextMessages(port, queue, "after-", 1);
                List<SourceRecord> recovered = pollUntil(task, 1);
                assertEquals("after-0",
                        new String((byte[]) recovered.get(0).value(), StandardCharsets.UTF_8));
                task.commitRecord(recovered.get(0), null);
                awaitMessageCount(task, broker, queue, 0);
            } finally {
                task.stop();
            }
        } finally {
            broker.stop();
        }
    }

    private static AmqSourceTask startTask(String queue, Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("name", "itest-" + queue);
        props.put(AmqSourceConnector.TASK_ID_CONFIG, "0");
        props.put(AmqSourceConnectorConfig.URL_CONFIG, "amqp://127.0.0.1:" + primaryPort);
        props.put(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG, queue);
        props.put(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "itest-topic");
        props.put(AmqSourceConnectorConfig.POLL_TIMEOUT_MS_CONFIG, "250");
        props.putAll(overrides);
        AmqSourceTask task = new AmqSourceTask();
        task.start(props);
        return task;
    }

    private static void sendTextMessages(int port, String queue, String payloadPrefix, int count)
            throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:" + port);
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(queue));
            for (int i = 0; i < count; i++) {
                TextMessage message = session.createTextMessage(payloadPrefix + i);
                message.setStringProperty("origin", "itest");
                producer.send(message);
            }
        }
    }

    private static List<SourceRecord> pollUntil(AmqSourceTask task, int count) throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 20_000;
        while (records.size() < count && System.currentTimeMillis() < deadline) {
            List<SourceRecord> batch = task.poll();
            if (batch != null) {
                records.addAll(batch);
            }
        }
        assertEquals(count, records.size(), "expected number of records within the timeout");
        return records;
    }

    /** Polls the task (which drains acknowledgements) until the broker reports the expected count. */
    private static void awaitMessageCount(AmqSourceTask task, EmbeddedActiveMQ broker, String queue,
                                          long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            task.poll();
            if (messageCount(broker, queue) == expected) {
                return;
            }
        }
        fail("queue '" + queue + "' still holds " + messageCount(broker, queue)
                + " messages, expected " + expected);
    }

    private static long messageCount(EmbeddedActiveMQ broker, String queue) {
        Queue serverQueue = broker.getActiveMQServer().locateQueue(SimpleString.of(queue));
        return serverQueue == null ? -1 : serverQueue.getMessageCount();
    }
}
