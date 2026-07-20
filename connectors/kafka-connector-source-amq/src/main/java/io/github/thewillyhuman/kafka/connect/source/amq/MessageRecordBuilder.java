/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Converts JMS messages into Kafka Connect source records.
 *
 * <p>The record value is the raw message payload (bytes by default) so that existing SMT
 * chains starting with a bytes-to-map style transform keep working unchanged. JMS metadata
 * and properties are exposed as record headers prefixed with {@code jms.}.
 */
final class MessageRecordBuilder {

    private static final String HEADER_PREFIX = "jms.";
    private static final String PROPERTY_HEADER_PREFIX = HEADER_PREFIX + "property.";

    private final String kafkaTopic;
    private final AmqSourceConnectorConfig.ValueFormat valueFormat;
    private final AmqSourceConnectorConfig.KeySource keySource;
    private final boolean headersEnabled;
    private final String destinationLabel;
    // Only touched from the task thread; one entry per broker the task consumes from.
    private final Map<String, Map<String, String>> sourcePartitionsByBroker = new HashMap<>();

    MessageRecordBuilder(AmqSourceConnectorConfig config) {
        this.kafkaTopic = config.kafkaTopic();
        this.valueFormat = config.valueFormat();
        this.keySource = config.keySource();
        this.headersEnabled = config.headersEnabled();
        this.destinationLabel =
                config.destinationType().name().toLowerCase(Locale.ROOT) + ":" + config.destinationName();
    }

    SourceRecord toSourceRecord(Message message, long acknowledgeId, String broker)
            throws JMSException, ConversionException {
        Object value = extractValue(message);
        Schema valueSchema = valueFormat == AmqSourceConnectorConfig.ValueFormat.BYTES
                ? Schema.OPTIONAL_BYTES_SCHEMA
                : Schema.OPTIONAL_STRING_SCHEMA;
        String key = extractKey(message);
        Schema keySchema = key == null ? null : Schema.OPTIONAL_STRING_SCHEMA;
        long jmsTimestamp = message.getJMSTimestamp();
        Long recordTimestamp = jmsTimestamp > 0 ? jmsTimestamp : null;
        ConnectHeaders headers = headersEnabled ? buildHeaders(message, broker) : null;
        Map<String, String> sourcePartition = sourcePartitionsByBroker.computeIfAbsent(broker,
                b -> Map.of(AmqSourceTask.PARTITION_DESTINATION_KEY, destinationLabel,
                        AmqSourceTask.PARTITION_BROKER_KEY, b));
        Map<String, Long> sourceOffset = Map.of(AmqSourceTask.OFFSET_ACKNOWLEDGE_ID_KEY, acknowledgeId);
        return new SourceRecord(sourcePartition, sourceOffset, kafkaTopic, null,
                keySchema, key, valueSchema, value, recordTimestamp, headers);
    }

    private Object extractValue(Message message) throws JMSException, ConversionException {
        if (message instanceof TextMessage textMessage) {
            String text = textMessage.getText();
            if (text == null) {
                return null;
            }
            return valueFormat == AmqSourceConnectorConfig.ValueFormat.STRING
                    ? text
                    : text.getBytes(StandardCharsets.UTF_8);
        }
        if (message instanceof BytesMessage bytesMessage) {
            long length = bytesMessage.getBodyLength();
            if (length > Integer.MAX_VALUE) {
                throw new ConversionException("Message body of " + length + " bytes is too large");
            }
            byte[] payload = new byte[(int) length];
            bytesMessage.readBytes(payload);
            return valueFormat == AmqSourceConnectorConfig.ValueFormat.STRING
                    ? new String(payload, StandardCharsets.UTF_8)
                    : payload;
        }
        throw new ConversionException("Unsupported JMS message type " + message.getClass().getName()
                + "; only TextMessage and BytesMessage are supported");
    }

    private String extractKey(Message message) throws JMSException {
        return switch (keySource) {
            case NONE -> null;
            case MESSAGE_ID -> message.getJMSMessageID();
            case CORRELATION_ID -> message.getJMSCorrelationID();
        };
    }

    private ConnectHeaders buildHeaders(Message message, String broker) throws JMSException {
        ConnectHeaders headers = new ConnectHeaders();
        addIfPresent(headers, "broker", broker);
        addIfPresent(headers, "message.id", message.getJMSMessageID());
        addIfPresent(headers, "correlation.id", message.getJMSCorrelationID());
        addIfPresent(headers, "type", message.getJMSType());
        addIfPresent(headers, "destination", destinationName(message.getJMSDestination()));
        if (message.getJMSTimestamp() > 0) {
            headers.addLong(HEADER_PREFIX + "timestamp", message.getJMSTimestamp());
        }
        if (message.getJMSExpiration() > 0) {
            headers.addLong(HEADER_PREFIX + "expiration", message.getJMSExpiration());
        }
        headers.addBoolean(HEADER_PREFIX + "redelivered", message.getJMSRedelivered());
        headers.addInt(HEADER_PREFIX + "priority", message.getJMSPriority());
        Enumeration<?> propertyNames = message.getPropertyNames();
        while (propertyNames != null && propertyNames.hasMoreElements()) {
            String name = String.valueOf(propertyNames.nextElement());
            addProperty(headers, name, message.getObjectProperty(name));
        }
        return headers;
    }

    private static void addIfPresent(ConnectHeaders headers, String name, String value) {
        if (value != null) {
            headers.addString(HEADER_PREFIX + name, value);
        }
    }

    private static String destinationName(Destination destination) throws JMSException {
        if (destination instanceof Queue queue) {
            return queue.getQueueName();
        }
        if (destination instanceof Topic topic) {
            return topic.getTopicName();
        }
        return null;
    }

    private static void addProperty(ConnectHeaders headers, String name, Object value) {
        if (value == null) {
            return;
        }
        String key = PROPERTY_HEADER_PREFIX + name;
        if (value instanceof String string) {
            headers.addString(key, string);
        } else if (value instanceof Boolean bool) {
            headers.addBoolean(key, bool);
        } else if (value instanceof Byte byteValue) {
            headers.addByte(key, byteValue);
        } else if (value instanceof Short shortValue) {
            headers.addShort(key, shortValue);
        } else if (value instanceof Integer intValue) {
            headers.addInt(key, intValue);
        } else if (value instanceof Long longValue) {
            headers.addLong(key, longValue);
        } else if (value instanceof Float floatValue) {
            headers.addFloat(key, floatValue);
        } else if (value instanceof Double doubleValue) {
            headers.addDouble(key, doubleValue);
        } else {
            headers.addString(key, value.toString());
        }
    }
}
