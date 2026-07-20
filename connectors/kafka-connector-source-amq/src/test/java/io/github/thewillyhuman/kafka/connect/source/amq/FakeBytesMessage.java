/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import jakarta.jms.BytesMessage;

class FakeBytesMessage extends FakeMessage implements BytesMessage {

    private final byte[] body;

    FakeBytesMessage(byte[] body) {
        this.body = body;
    }

    @Override
    public long getBodyLength() {
        return body.length;
    }

    @Override
    public int readBytes(byte[] value) {
        int length = Math.min(value.length, body.length);
        System.arraycopy(body, 0, value, 0, length);
        return length;
    }

    @Override
    public int readBytes(byte[] value, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeShort(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChar(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeFloat(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeObject(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
    }
}
