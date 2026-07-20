/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AmqSourceConnectorConfigTest {

    private static Map<String, String> minimalProps() {
        Map<String, String> props = new HashMap<>();
        props.put(AmqSourceConnectorConfig.URL_CONFIG, "amqp://localhost:61616");
        props.put(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG, "Consumer.sample.events");
        props.put(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "sample-events");
        return props;
    }

    @Test
    void minimalConfigUsesSafeDefaults() {
        AmqSourceConnectorConfig config = new AmqSourceConnectorConfig(minimalProps());
        assertEquals(AmqSourceConnectorConfig.DestinationType.QUEUE, config.destinationType());
        assertEquals(AmqSourceConnectorConfig.ValueFormat.BYTES, config.valueFormat());
        assertEquals(AmqSourceConnectorConfig.KeySource.NONE, config.keySource());
        assertEquals(AmqSourceConnectorConfig.ConversionErrorPolicy.FAIL, config.conversionErrorPolicy());
        assertEquals(2048, config.maxUnackedMessages());
        assertEquals(128L * 1024 * 1024, config.maxUnackedBytes());
        assertEquals(1024, config.batchMaxSize());
        assertTrue(config.headersEnabled());
        assertNull(config.username());
        assertNull(config.password());
        assertNull(config.messageSelector());
    }

    @Test
    void requiredSettingsAreEnforced() {
        Map<String, String> props = minimalProps();
        props.remove(AmqSourceConnectorConfig.URL_CONFIG);
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(props));

        Map<String, String> noDestination = minimalProps();
        noDestination.remove(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG);
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(noDestination));

        Map<String, String> noTopic = minimalProps();
        noTopic.remove(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG);
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(noTopic));
    }

    @Test
    void enumSettingsAreCaseInsensitive() {
        Map<String, String> props = minimalProps();
        props.put(AmqSourceConnectorConfig.DESTINATION_TYPE_CONFIG, "Topic");
        props.put(AmqSourceConnectorConfig.VALUE_FORMAT_CONFIG, "STRING");
        props.put(AmqSourceConnectorConfig.KEY_SOURCE_CONFIG, "Message-Id");
        props.put(AmqSourceConnectorConfig.CONVERSION_ERROR_POLICY_CONFIG, "Discard");
        AmqSourceConnectorConfig config = new AmqSourceConnectorConfig(props);
        assertEquals(AmqSourceConnectorConfig.DestinationType.TOPIC, config.destinationType());
        assertEquals(AmqSourceConnectorConfig.ValueFormat.STRING, config.valueFormat());
        assertEquals(AmqSourceConnectorConfig.KeySource.MESSAGE_ID, config.keySource());
        assertEquals(AmqSourceConnectorConfig.ConversionErrorPolicy.DISCARD, config.conversionErrorPolicy());
    }

    @Test
    void invalidEnumValueIsRejected() {
        Map<String, String> props = minimalProps();
        props.put(AmqSourceConnectorConfig.DESTINATION_TYPE_CONFIG, "exchange");
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(props));
    }

    @Test
    void durableSubscriptionRequiresTopicAndClientId() {
        Map<String, String> onQueue = minimalProps();
        onQueue.put(AmqSourceConnectorConfig.SUBSCRIPTION_NAME_CONFIG, "sample-sub");
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(onQueue));

        Map<String, String> noClientId = minimalProps();
        noClientId.put(AmqSourceConnectorConfig.DESTINATION_TYPE_CONFIG, "topic");
        noClientId.put(AmqSourceConnectorConfig.SUBSCRIPTION_NAME_CONFIG, "sample-sub");
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(noClientId));

        Map<String, String> valid = minimalProps();
        valid.put(AmqSourceConnectorConfig.DESTINATION_TYPE_CONFIG, "topic");
        valid.put(AmqSourceConnectorConfig.SUBSCRIPTION_NAME_CONFIG, "sample-sub");
        valid.put(AmqSourceConnectorConfig.CLIENT_ID_CONFIG, "sample-connector");
        assertEquals("sample-sub", new AmqSourceConnectorConfig(valid).subscriptionName());
    }

    @Test
    void maxUnackedBytesCanBeDisabledButNotNegative() {
        Map<String, String> disabled = minimalProps();
        disabled.put(AmqSourceConnectorConfig.MAX_UNACKED_BYTES_CONFIG, "0");
        assertEquals(0, new AmqSourceConnectorConfig(disabled).maxUnackedBytes());

        Map<String, String> negative = minimalProps();
        negative.put(AmqSourceConnectorConfig.MAX_UNACKED_BYTES_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> new AmqSourceConnectorConfig(negative));
    }

    @Test
    void passwordIsNotExposedByToString() {
        Map<String, String> props = minimalProps();
        props.put(AmqSourceConnectorConfig.PASSWORD_CONFIG, "super-secret");
        AmqSourceConnectorConfig config = new AmqSourceConnectorConfig(props);
        assertEquals("super-secret", config.password());
        assertTrue(config.getPassword(AmqSourceConnectorConfig.PASSWORD_CONFIG).toString().contains("hidden"));
    }
}
