/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

/**
 * A JMS message could not be converted to a Kafka Connect source record. Handled according
 * to the {@code conversion.error.policy} configuration.
 */
class ConversionException extends Exception {

    private static final long serialVersionUID = 1L;

    ConversionException(String message) {
        super(message);
    }
}
