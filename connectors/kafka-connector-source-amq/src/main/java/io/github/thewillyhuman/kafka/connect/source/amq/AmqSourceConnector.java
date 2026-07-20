/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source connector that consumes messages from ActiveMQ over AMQP 1.0 and produces them to
 * Kafka with at-least-once semantics: a message is acknowledged on the broker only after
 * Kafka has confirmed the corresponding record.
 *
 * <p>For queues, multiple tasks act as competing consumers on the same destination, so
 * {@code tasksMax} scales consumption horizontally.
 */
public class AmqSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(AmqSourceConnector.class);

    /** Injected into every task config so tasks can identify themselves in logs and metrics. */
    public static final String TASK_ID_CONFIG = "task.id";

    /** Injected into every task config: the subset of brokers the task consumes from. */
    public static final String TASK_BROKERS_CONFIG = "task.brokers";

    private Map<String, String> originalProps;
    private AmqSourceConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.originalProps = Map.copyOf(props);
        this.config = new AmqSourceConnectorConfig(props);
        log.info("Starting AMQ source connector {} version {}: broker={}, destination={} ({}), kafka topic={}",
                props.getOrDefault("name", ""), version(), JmsClient.sanitizeUrl(config.url()),
                config.destinationName(), config.destinationType(), config.kafkaTopic());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AmqSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        int tasks = maxTasks;
        if (config.subscriptionName() != null && maxTasks > 1) {
            log.warn("Durable subscription '{}' only supports a single consumer; running 1 task instead of {}",
                    config.subscriptionName(), maxTasks);
            tasks = 1;
        }
        List<List<String>> assignments = assignBrokers(config.brokerUrls(), tasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>(assignments.size());
        for (int i = 0; i < assignments.size(); i++) {
            Map<String, String> taskProps = new HashMap<>(originalProps);
            taskProps.put(TASK_ID_CONFIG, Integer.toString(i));
            taskProps.put(TASK_BROKERS_CONFIG, String.join(",", assignments.get(i)));
            taskConfigs.add(taskProps);
            log.info("Task {} will consume '{}' from broker(s) {}", i, config.destinationName(),
                    JmsClient.sanitizeUrl(String.join(", ", assignments.get(i))));
        }
        return taskConfigs;
    }

    /**
     * Distributes the brokers across tasks so a single connector covers a destination that
     * lives on several brokers. With at least as many tasks as brokers, every task gets one
     * broker and surplus tasks become additional competing consumers (queues only benefit);
     * with fewer tasks, each task consumes from several brokers concurrently.
     */
    static List<List<String>> assignBrokers(List<String> brokers, int tasks) {
        List<List<String>> assignments = new ArrayList<>();
        if (tasks >= brokers.size()) {
            for (int i = 0; i < tasks; i++) {
                assignments.add(List.of(brokers.get(i % brokers.size())));
            }
        } else {
            for (int i = 0; i < tasks; i++) {
                assignments.add(new ArrayList<>());
            }
            for (int j = 0; j < brokers.size(); j++) {
                assignments.get(j % tasks).add(brokers.get(j));
            }
        }
        return assignments;
    }

    @Override
    public void stop() {
        log.info("Stopping AMQ source connector");
    }

    @Override
    public ConfigDef config() {
        return AmqSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        // JMS has no replayable offsets, so the connector is at-least-once by design.
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    @Override
    public String version() {
        return Version.get();
    }
}
