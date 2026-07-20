/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal in-memory {@link Message} for unit tests, avoiding a mocking framework.
 */
class FakeMessage implements Message {

    private final Map<String, Object> properties = new LinkedHashMap<>();

    private String messageId;
    private long timestamp;
    private String correlationId;
    private Destination replyTo;
    private Destination destination;
    private int deliveryMode = DEFAULT_DELIVERY_MODE;
    private boolean redelivered;
    private String type;
    private long expiration;
    private long deliveryTime;
    private int priority = DEFAULT_PRIORITY;

    private int acknowledgeCount;
    private JMSException acknowledgeFailure;

    int acknowledgeCount() {
        return acknowledgeCount;
    }

    void failAcknowledgeWith(JMSException failure) {
        this.acknowledgeFailure = failure;
    }

    @Override
    public String getJMSMessageID() {
        return messageId;
    }

    @Override
    public void setJMSMessageID(String id) {
        this.messageId = id;
    }

    @Override
    public long getJMSTimestamp() {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSCorrelationID(String correlationID) {
        this.correlationId = correlationID;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationId;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public void setJMSType(String type) {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) {
        this.expiration = expiration;
    }

    @Override
    public long getJMSDeliveryTime() {
        return deliveryTime;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) {
        this.deliveryTime = deliveryTime;
    }

    @Override
    public int getJMSPriority() {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public void clearProperties() {
        properties.clear();
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return (Boolean) properties.get(name);
    }

    @Override
    public byte getByteProperty(String name) {
        return (Byte) properties.get(name);
    }

    @Override
    public short getShortProperty(String name) {
        return (Short) properties.get(name);
    }

    @Override
    public int getIntProperty(String name) {
        return (Integer) properties.get(name);
    }

    @Override
    public long getLongProperty(String name) {
        return (Long) properties.get(name);
    }

    @Override
    public float getFloatProperty(String name) {
        return (Float) properties.get(name);
    }

    @Override
    public double getDoubleProperty(String name) {
        return (Double) properties.get(name);
    }

    @Override
    public String getStringProperty(String name) {
        return (String) properties.get(name);
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.get(name);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Enumeration getPropertyNames() {
        return Collections.enumeration(properties.keySet());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) {
        properties.put(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) {
        properties.put(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) {
        properties.put(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) {
        properties.put(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) {
        properties.put(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) {
        properties.put(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) {
        properties.put(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) {
        properties.put(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) {
        properties.put(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeFailure != null) {
            throw acknowledgeFailure;
        }
        acknowledgeCount++;
    }

    @Override
    public void clearBody() {
    }

    @Override
    public <T> T getBody(Class<T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean isBodyAssignableTo(Class c) {
        return false;
    }
}
