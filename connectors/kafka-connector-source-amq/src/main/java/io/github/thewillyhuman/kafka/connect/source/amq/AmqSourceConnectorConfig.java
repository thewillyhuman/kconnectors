/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for {@link AmqSourceConnector} and {@link AmqSourceTask}.
 */
public final class AmqSourceConnectorConfig extends AbstractConfig {

    public enum DestinationType { QUEUE, TOPIC }

    public enum ValueFormat { BYTES, STRING }

    public enum KeySource { NONE, MESSAGE_ID, CORRELATION_ID }

    public enum ConversionErrorPolicy { FAIL, DISCARD }

    private static final String GROUP_BROKER = "Broker";
    private static final String GROUP_DESTINATION = "Destination";
    private static final String GROUP_KAFKA = "Kafka";
    private static final String GROUP_DELIVERY = "Delivery";
    private static final String GROUP_RECORDS = "Records";

    public static final String URL_CONFIG = "amq.url";
    private static final String URL_DOC =
            "Comma-separated list of AMQP 1.0 broker URIs, e.g. "
            + "amqp://broker-1.example.com:61112?jms.prefetchPolicy.all=100,amqp://broker-2.example.com:61112. "
            + "The connector consumes from ALL listed brokers concurrently, distributing them "
            + "across tasks, so a single connector covers a queue that lives on several brokers. "
            + "Each entry supports all Qpid JMS URI options, amqps:// for TLS, and may itself be "
            + "a failover:(amqp://a,amqp://b) URI for HA pairs (one connection per entry).";

    public static final String USERNAME_CONFIG = "amq.username";
    private static final String USERNAME_DOC = "Username used to authenticate against the broker (SASL).";

    public static final String PASSWORD_CONFIG = "amq.password";
    private static final String PASSWORD_DOC =
            "Password used to authenticate against the broker. Never logged; use a Kafka Connect "
            + "config provider (e.g. ${file:...} or ${secrets:...}) to avoid storing it in clear text.";

    public static final String CLIENT_ID_CONFIG = "amq.client.id";
    private static final String CLIENT_ID_DOC =
            "JMS client id set on the connection. Required when using a durable topic subscription.";

    public static final String DESTINATION_NAME_CONFIG = "amq.destination.name";
    private static final String DESTINATION_NAME_DOC =
            "Name of the queue or topic to consume from, e.g. Consumer.sample.events.";

    public static final String DESTINATION_TYPE_CONFIG = "amq.destination.type";
    private static final String DESTINATION_TYPE_DOC =
            "Type of the destination: 'queue' or 'topic'. Queues (including ActiveMQ virtual-topic "
            + "consumer queues) are recommended: they buffer messages while the connector is down.";

    public static final String MESSAGE_SELECTOR_CONFIG = "amq.message.selector";
    private static final String MESSAGE_SELECTOR_DOC = "Optional JMS message selector applied to the consumer.";

    public static final String SUBSCRIPTION_NAME_CONFIG = "amq.subscription.name";
    private static final String SUBSCRIPTION_NAME_DOC =
            "Optional durable subscription name, only valid for topics and requires " + CLIENT_ID_CONFIG + ". "
            + "Durable subscriptions are limited to a single task.";

    public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
    private static final String KAFKA_TOPIC_DOC = "Kafka topic the messages are produced to.";

    public static final String BATCH_MAX_SIZE_CONFIG = "batch.max.size";
    private static final String BATCH_MAX_SIZE_DOC = "Maximum number of records returned by a single poll of the task.";

    public static final String POLL_TIMEOUT_MS_CONFIG = "poll.timeout.ms";
    private static final String POLL_TIMEOUT_MS_DOC =
            "How long a poll waits for the first message before returning an empty batch.";

    public static final String MAX_UNACKED_MESSAGES_CONFIG = "max.unacked.messages";
    private static final String MAX_UNACKED_MESSAGES_DOC =
            "Maximum number of messages consumed from the broker but not yet confirmed by Kafka. "
            + "When the limit is reached the task stops consuming until Kafka catches up "
            + "(backpressure). This bounds both memory usage and the number of possible duplicates "
            + "after a crash.";

    public static final String MAX_UNACKED_BYTES_CONFIG = "max.unacked.bytes";
    private static final String MAX_UNACKED_BYTES_DOC =
            "Maximum cumulative payload size, in bytes, of the messages consumed from the broker "
            + "but not yet confirmed by Kafka. Complements " + MAX_UNACKED_MESSAGES_CONFIG + ": the "
            + "count bounds the duplicate window while this bounds heap usage when messages are "
            + "large. At least one message is always admitted, so a single message larger than the "
            + "limit still flows. 0 disables the byte limit.";

    public static final String VALUE_FORMAT_CONFIG = "record.value.format";
    private static final String VALUE_FORMAT_DOC =
            "Format of the record value handed to the transformation chain: 'bytes' (default; both "
            + "JMS TextMessage and BytesMessage payloads become byte arrays, matching pipelines that "
            + "start with a BytesToMap transform) or 'string' (payloads become UTF-8 strings).";

    public static final String KEY_SOURCE_CONFIG = "record.key.source";
    private static final String KEY_SOURCE_DOC =
            "What to use as the Kafka record key: 'none' (default), 'message-id' or 'correlation-id'.";

    public static final String HEADERS_ENABLED_CONFIG = "record.headers.enabled";
    private static final String HEADERS_ENABLED_DOC =
            "Whether to copy JMS metadata (message id, timestamp, redelivered flag, ...) and JMS "
            + "properties into Kafka record headers, prefixed with 'jms.'.";

    public static final String CONVERSION_ERROR_POLICY_CONFIG = "conversion.error.policy";
    private static final String CONVERSION_ERROR_POLICY_DOC =
            "What to do when a JMS message cannot be converted to a source record: 'fail' (default; "
            + "the task fails and the message stays on the broker) or 'discard' (the message is "
            + "logged, counted in the metrics, acknowledged and dropped).";

    public static final String CONNECTION_RETRY_BACKOFF_MS_CONFIG = "connection.retry.backoff.ms";
    private static final String CONNECTION_RETRY_BACKOFF_MS_DOC =
            "Initial delay between reconnection attempts to the broker. Doubles on every failed "
            + "attempt up to " + "connection.retry.backoff.max.ms" + ".";

    public static final String CONNECTION_RETRY_BACKOFF_MAX_MS_CONFIG = "connection.retry.backoff.max.ms";
    private static final String CONNECTION_RETRY_BACKOFF_MAX_MS_DOC =
            "Maximum delay between reconnection attempts to the broker.";

    public static final String CONNECTION_MAX_DOWNTIME_MS_CONFIG = "connection.max.downtime.ms";
    private static final String CONNECTION_MAX_DOWNTIME_MS_DOC =
            "How long the task tolerates having no reachable broker at all before it fails, so a "
            + "total outage becomes visible to the control plane (task state FAILED) instead of the "
            + "task looking healthy while consuming nothing. While at least one assigned broker is "
            + "reachable the task never fails, and any successful connection resets the clock. "
            + "Failing loses no messages: everything unacknowledged is redelivered by the brokers. "
            + "0 disables the limit (the task retries forever, silently).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Broker
            .define(URL_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    Importance.HIGH, URL_DOC, GROUP_BROKER, 0, Width.LONG, "Broker URL")
            .define(USERNAME_CONFIG, Type.STRING, null,
                    Importance.MEDIUM, USERNAME_DOC, GROUP_BROKER, 1, Width.MEDIUM, "Username")
            .define(PASSWORD_CONFIG, Type.PASSWORD, null,
                    Importance.MEDIUM, PASSWORD_DOC, GROUP_BROKER, 2, Width.MEDIUM, "Password")
            .define(CLIENT_ID_CONFIG, Type.STRING, null,
                    Importance.LOW, CLIENT_ID_DOC, GROUP_BROKER, 3, Width.MEDIUM, "Client id")
            .define(CONNECTION_RETRY_BACKOFF_MS_CONFIG, Type.LONG, 1000L, Range.atLeast(100L),
                    Importance.LOW, CONNECTION_RETRY_BACKOFF_MS_DOC, GROUP_BROKER, 4, Width.SHORT,
                    "Retry backoff (ms)")
            .define(CONNECTION_RETRY_BACKOFF_MAX_MS_CONFIG, Type.LONG, 60_000L, Range.atLeast(1000L),
                    Importance.LOW, CONNECTION_RETRY_BACKOFF_MAX_MS_DOC, GROUP_BROKER, 5, Width.SHORT,
                    "Max retry backoff (ms)")
            .define(CONNECTION_MAX_DOWNTIME_MS_CONFIG, Type.LONG, 900_000L, Range.atLeast(0L),
                    Importance.MEDIUM, CONNECTION_MAX_DOWNTIME_MS_DOC, GROUP_BROKER, 6, Width.SHORT,
                    "Max total downtime (ms)")
            // Destination
            .define(DESTINATION_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    Importance.HIGH, DESTINATION_NAME_DOC, GROUP_DESTINATION, 0, Width.LONG, "Destination name")
            .define(DESTINATION_TYPE_CONFIG, Type.STRING, "queue",
                    ConfigDef.CaseInsensitiveValidString.in("queue", "topic"),
                    Importance.HIGH, DESTINATION_TYPE_DOC, GROUP_DESTINATION, 1, Width.SHORT, "Destination type")
            .define(MESSAGE_SELECTOR_CONFIG, Type.STRING, null,
                    Importance.LOW, MESSAGE_SELECTOR_DOC, GROUP_DESTINATION, 2, Width.LONG, "Message selector")
            .define(SUBSCRIPTION_NAME_CONFIG, Type.STRING, null,
                    Importance.LOW, SUBSCRIPTION_NAME_DOC, GROUP_DESTINATION, 3, Width.MEDIUM, "Subscription name")
            // Kafka
            .define(KAFKA_TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    Importance.HIGH, KAFKA_TOPIC_DOC, GROUP_KAFKA, 0, Width.LONG, "Kafka topic")
            // Delivery
            .define(MAX_UNACKED_MESSAGES_CONFIG, Type.INT, 2048, Range.atLeast(1),
                    Importance.MEDIUM, MAX_UNACKED_MESSAGES_DOC, GROUP_DELIVERY, 0, Width.SHORT,
                    "Max unacknowledged messages")
            .define(MAX_UNACKED_BYTES_CONFIG, Type.LONG, 128L * 1024 * 1024, Range.atLeast(0L),
                    Importance.MEDIUM, MAX_UNACKED_BYTES_DOC, GROUP_DELIVERY, 1, Width.SHORT,
                    "Max unacknowledged bytes")
            .define(BATCH_MAX_SIZE_CONFIG, Type.INT, 1024, Range.between(1, 100_000),
                    Importance.MEDIUM, BATCH_MAX_SIZE_DOC, GROUP_DELIVERY, 2, Width.SHORT, "Max batch size")
            .define(POLL_TIMEOUT_MS_CONFIG, Type.LONG, 1000L, Range.between(10L, 60_000L),
                    Importance.LOW, POLL_TIMEOUT_MS_DOC, GROUP_DELIVERY, 3, Width.SHORT, "Poll timeout (ms)")
            .define(CONVERSION_ERROR_POLICY_CONFIG, Type.STRING, "fail",
                    ConfigDef.CaseInsensitiveValidString.in("fail", "discard"),
                    Importance.MEDIUM, CONVERSION_ERROR_POLICY_DOC, GROUP_DELIVERY, 4, Width.SHORT,
                    "Conversion error policy")
            // Records
            .define(VALUE_FORMAT_CONFIG, Type.STRING, "bytes",
                    ConfigDef.CaseInsensitiveValidString.in("bytes", "string"),
                    Importance.MEDIUM, VALUE_FORMAT_DOC, GROUP_RECORDS, 0, Width.SHORT, "Value format")
            .define(KEY_SOURCE_CONFIG, Type.STRING, "none",
                    ConfigDef.CaseInsensitiveValidString.in("none", "message-id", "correlation-id"),
                    Importance.LOW, KEY_SOURCE_DOC, GROUP_RECORDS, 1, Width.SHORT, "Key source")
            .define(HEADERS_ENABLED_CONFIG, Type.BOOLEAN, true,
                    Importance.LOW, HEADERS_ENABLED_DOC, GROUP_RECORDS, 2, Width.SHORT, "JMS headers enabled");

    private final List<String> brokerUrls;

    public AmqSourceConnectorConfig(Map<String, String> originals) {
        // Task configs carry the full connector config (transforms.*, converters, ...); do not
        // log those as unknown.
        super(CONFIG_DEF, originals, false);
        try {
            this.brokerUrls = BrokerUrls.parse(url());
        } catch (IllegalArgumentException e) {
            throw new ConfigException(URL_CONFIG, url(), e.getMessage());
        }
        validateDurableSubscription();
    }

    private void validateDurableSubscription() {
        if (subscriptionName() == null) {
            return;
        }
        if (destinationType() != DestinationType.TOPIC) {
            throw new ConfigException(SUBSCRIPTION_NAME_CONFIG,
                    subscriptionName(), "Durable subscriptions are only valid when "
                    + DESTINATION_TYPE_CONFIG + " is 'topic'");
        }
        if (clientId() == null) {
            throw new ConfigException(SUBSCRIPTION_NAME_CONFIG,
                    subscriptionName(), "Durable subscriptions require " + CLIENT_ID_CONFIG + " to be set");
        }
    }

    public String url() {
        return getString(URL_CONFIG);
    }

    /** All brokers to consume from, one connection per entry. */
    public List<String> brokerUrls() {
        return List.copyOf(brokerUrls);
    }

    public String username() {
        return getString(USERNAME_CONFIG);
    }

    public String password() {
        Password password = getPassword(PASSWORD_CONFIG);
        return password == null ? null : password.value();
    }

    public String clientId() {
        return getString(CLIENT_ID_CONFIG);
    }

    public String destinationName() {
        return getString(DESTINATION_NAME_CONFIG);
    }

    public DestinationType destinationType() {
        return DestinationType.valueOf(getString(DESTINATION_TYPE_CONFIG).toUpperCase(Locale.ROOT));
    }

    public String messageSelector() {
        return getString(MESSAGE_SELECTOR_CONFIG);
    }

    public String subscriptionName() {
        return getString(SUBSCRIPTION_NAME_CONFIG);
    }

    public String kafkaTopic() {
        return getString(KAFKA_TOPIC_CONFIG);
    }

    public int batchMaxSize() {
        return getInt(BATCH_MAX_SIZE_CONFIG);
    }

    public long pollTimeoutMs() {
        return getLong(POLL_TIMEOUT_MS_CONFIG);
    }

    public int maxUnackedMessages() {
        return getInt(MAX_UNACKED_MESSAGES_CONFIG);
    }

    /** Byte limit for unconfirmed message payloads; 0 means no byte limit. */
    public long maxUnackedBytes() {
        return getLong(MAX_UNACKED_BYTES_CONFIG);
    }

    public ValueFormat valueFormat() {
        return ValueFormat.valueOf(getString(VALUE_FORMAT_CONFIG).toUpperCase(Locale.ROOT));
    }

    public KeySource keySource() {
        return KeySource.valueOf(getString(KEY_SOURCE_CONFIG).toUpperCase(Locale.ROOT).replace('-', '_'));
    }

    public boolean headersEnabled() {
        return getBoolean(HEADERS_ENABLED_CONFIG);
    }

    public ConversionErrorPolicy conversionErrorPolicy() {
        return ConversionErrorPolicy.valueOf(getString(CONVERSION_ERROR_POLICY_CONFIG).toUpperCase(Locale.ROOT));
    }

    public long connectionRetryBackoffMs() {
        return getLong(CONNECTION_RETRY_BACKOFF_MS_CONFIG);
    }

    public long connectionRetryBackoffMaxMs() {
        return getLong(CONNECTION_RETRY_BACKOFF_MAX_MS_CONFIG);
    }

    /** Max tolerated total-outage duration before the task fails; 0 means never fail. */
    public long connectionMaxDowntimeMs() {
        return getLong(CONNECTION_MAX_DOWNTIME_MS_CONFIG);
    }
}
