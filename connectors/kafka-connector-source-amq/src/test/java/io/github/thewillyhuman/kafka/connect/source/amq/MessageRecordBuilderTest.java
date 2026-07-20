/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageRecordBuilderTest {

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(AmqSourceConnectorConfig.URL_CONFIG, "amqp://localhost:61616");
        props.put(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG, "Consumer.sample.events");
        props.put(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "sample-events");
        return props;
    }

    private static final String BROKER = "amqp://broker-1.example.com:61112";

    private static MessageRecordBuilder builder(Map<String, String> props) {
        return new MessageRecordBuilder(new AmqSourceConnectorConfig(props));
    }

    @Test
    void textMessageBecomesBytesRecordWithJmsHeaders() throws Exception {
        FakeTextMessage message = new FakeTextMessage("{\"metric\": 1}");
        message.setJMSMessageID("ID:1234");
        message.setJMSTimestamp(1720000000000L);
        message.setJMSRedelivered(true);
        message.setStringProperty("origin", "etf");
        message.setIntProperty("attempt", 3);

        SourceRecord record = builder(props()).toSourceRecord(message, 7L, BROKER);

        assertEquals("sample-events", record.topic());
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, record.valueSchema());
        assertArrayEquals("{\"metric\": 1}".getBytes(StandardCharsets.UTF_8), (byte[]) record.value());
        assertNull(record.key());
        assertEquals(1720000000000L, record.timestamp());
        assertEquals(7L, record.sourceOffset().get(AmqSourceTask.OFFSET_ACKNOWLEDGE_ID_KEY));
        assertEquals("queue:Consumer.sample.events",
                record.sourcePartition().get(AmqSourceTask.PARTITION_DESTINATION_KEY));
        assertEquals(BROKER, record.sourcePartition().get(AmqSourceTask.PARTITION_BROKER_KEY));

        assertEquals(BROKER, header(record, "jms.broker"));
        assertEquals("ID:1234", header(record, "jms.message.id"));
        assertEquals(true, header(record, "jms.redelivered"));
        assertEquals(1720000000000L, header(record, "jms.timestamp"));
        assertEquals("etf", header(record, "jms.property.origin"));
        assertEquals(3, header(record, "jms.property.attempt"));
    }

    @Test
    void bytesMessagePayloadIsCopiedVerbatim() throws Exception {
        byte[] payload = {1, 2, 3, 4, 5};
        SourceRecord record = builder(props()).toSourceRecord(new FakeBytesMessage(payload), 1L, BROKER);
        assertArrayEquals(payload, (byte[]) record.value());
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, record.valueSchema());
    }

    @Test
    void stringValueFormatProducesStrings() throws Exception {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.VALUE_FORMAT_CONFIG, "string");
        SourceRecord record = builder(props)
                .toSourceRecord(new FakeBytesMessage("hello".getBytes(StandardCharsets.UTF_8)), 1L, BROKER);
        assertEquals("hello", record.value());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, record.valueSchema());
    }

    @Test
    void messageIdKeySourceSetsTheRecordKey() throws Exception {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.KEY_SOURCE_CONFIG, "message-id");
        FakeTextMessage message = new FakeTextMessage("x");
        message.setJMSMessageID("ID:42");
        SourceRecord record = builder(props).toSourceRecord(message, 1L, BROKER);
        assertEquals("ID:42", record.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, record.keySchema());
    }

    @Test
    void headersCanBeDisabled() throws Exception {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.HEADERS_ENABLED_CONFIG, "false");
        FakeTextMessage message = new FakeTextMessage("x");
        message.setStringProperty("origin", "etf");
        SourceRecord record = builder(props).toSourceRecord(message, 1L, BROKER);
        assertTrue(record.headers().isEmpty());
    }

    @Test
    void nullTextBodyBecomesNullValue() throws Exception {
        SourceRecord record = builder(props()).toSourceRecord(new FakeTextMessage(null), 1L, BROKER);
        assertNull(record.value());
    }

    @Test
    void unsupportedMessageTypesAreRejected() {
        assertThrows(ConversionException.class,
                () -> builder(props()).toSourceRecord(new FakeMessage(), 1L, BROKER));
    }

    @Test
    void missingTimestampLeavesRecordTimestampUnset() throws Exception {
        SourceRecord record = builder(props()).toSourceRecord(new FakeTextMessage("x"), 1L, BROKER);
        assertNull(record.timestamp());
        assertFalse(record.headers().allWithName("jms.timestamp").hasNext());
    }

    @Test
    void sourcePartitionsDistinguishBrokers() throws Exception {
        MessageRecordBuilder builder = builder(props());
        SourceRecord first = builder.toSourceRecord(new FakeTextMessage("x"), 1L, "amqp://a:61112");
        SourceRecord second = builder.toSourceRecord(new FakeTextMessage("y"), 2L, "amqp://b:61112");
        assertEquals("amqp://a:61112", first.sourcePartition().get(AmqSourceTask.PARTITION_BROKER_KEY));
        assertEquals("amqp://b:61112", second.sourcePartition().get(AmqSourceTask.PARTITION_BROKER_KEY));
    }

    private static Object header(SourceRecord record, String name) {
        Header header = record.headers().lastWithName(name);
        return header == null ? null : header.value();
    }
}
