/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AmqSourceConnectorTest {

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "sample-events");
        props.put(AmqSourceConnectorConfig.URL_CONFIG, "amqp://localhost:61616");
        props.put(AmqSourceConnectorConfig.DESTINATION_NAME_CONFIG, "Consumer.sample.events");
        props.put(AmqSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "sample-events");
        return props;
    }

    @Test
    void tasksInheritTheConnectorConfigAndGetATaskId() {
        AmqSourceConnector connector = new AmqSourceConnector();
        connector.start(props());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(3);
        assertEquals(3, taskConfigs.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), taskConfigs.get(i).get(AmqSourceConnector.TASK_ID_CONFIG));
            assertEquals("amqp://localhost:61616", taskConfigs.get(i).get(AmqSourceConnectorConfig.URL_CONFIG));
            assertEquals("amqp://localhost:61616", taskConfigs.get(i).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
            assertEquals("sample-events", taskConfigs.get(i).get("name"));
        }
        connector.stop();
    }

    @Test
    void brokersAreDistributedAcrossTasks() {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.URL_CONFIG, "amqp://a:1,amqp://b:2,amqp://c:3");
        AmqSourceConnector connector = new AmqSourceConnector();
        connector.start(props);

        // Fewer tasks than brokers: tasks split the broker list.
        List<Map<String, String>> two = connector.taskConfigs(2);
        assertEquals(2, two.size());
        assertEquals("amqp://a:1,amqp://c:3", two.get(0).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        assertEquals("amqp://b:2", two.get(1).get(AmqSourceConnector.TASK_BROKERS_CONFIG));

        // More tasks than brokers: one broker per task, surplus tasks are competing consumers.
        List<Map<String, String>> five = connector.taskConfigs(5);
        assertEquals(5, five.size());
        assertEquals("amqp://a:1", five.get(0).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        assertEquals("amqp://b:2", five.get(1).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        assertEquals("amqp://c:3", five.get(2).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        assertEquals("amqp://a:1", five.get(3).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        assertEquals("amqp://b:2", five.get(4).get(AmqSourceConnector.TASK_BROKERS_CONFIG));
        connector.stop();
    }

    @Test
    void failoverUrisSurviveTheTaskAssignmentRoundTrip() {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.URL_CONFIG,
                "failover:(amqp://a:1,amqp://b:2),amqp://c:3");
        AmqSourceConnector connector = new AmqSourceConnector();
        connector.start(props);
        List<Map<String, String>> tasks = connector.taskConfigs(1);
        assertEquals(List.of("failover:(amqp://a:1,amqp://b:2)", "amqp://c:3"),
                BrokerUrls.parse(tasks.get(0).get(AmqSourceConnector.TASK_BROKERS_CONFIG)));
        connector.stop();
    }

    @Test
    void durableSubscriptionsAreLimitedToASingleTask() {
        Map<String, String> props = props();
        props.put(AmqSourceConnectorConfig.DESTINATION_TYPE_CONFIG, "topic");
        props.put(AmqSourceConnectorConfig.SUBSCRIPTION_NAME_CONFIG, "sample-sub");
        props.put(AmqSourceConnectorConfig.CLIENT_ID_CONFIG, "sample-connector");
        AmqSourceConnector connector = new AmqSourceConnector();
        connector.start(props);
        assertEquals(1, connector.taskConfigs(4).size());
        connector.stop();
    }

    @Test
    void connectorIsDiscoverableViaServiceLoader() {
        // KIP-898: workers with plugin.discovery=service_load only see plugins declared in
        // a META-INF/services manifest; without it the connector silently disappears.
        boolean discovered = ServiceLoader.load(SourceConnector.class).stream()
                .anyMatch(provider -> provider.type() == AmqSourceConnector.class);
        assertTrue(discovered,
                "META-INF/services/org.apache.kafka.connect.source.SourceConnector must declare the connector");
    }

    @Test
    void versionIsExposed() {
        AmqSourceConnector connector = new AmqSourceConnector();
        assertNotNull(connector.version());
        assertNotEquals("unknown", connector.version(), "the build should inject the project version");
        assertEquals(AmqSourceTask.class, connector.taskClass());
    }
}
