/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Exposes the connector version, injected by the build into a properties resource.
 */
final class Version {

    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static final String VERSION = load();

    private Version() {
    }

    static String get() {
        return VERSION;
    }

    private static String load() {
        try (InputStream stream = Version.class.getResourceAsStream("/kafka-connector-source-amq.properties")) {
            Properties properties = new Properties();
            if (stream != null) {
                properties.load(stream);
            }
            return properties.getProperty("version", "unknown").trim();
        } catch (Exception e) {
            log.warn("Could not read connector version", e);
            return "unknown";
        }
    }
}
