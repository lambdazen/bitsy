package com.lambdazen.bitsy;

import org.apache.commons.lang.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;

import java.io.IOException;

public class UUIDGraphBinarySerializer implements CustomTypeSerializer<UUID>
{

    private final byte[] typeInfoBuffer = new byte[] { 0, 0, 0, 0 };

    @Override
    public String getTypeName() {
        return "bitsy.UUID";
    }

    @Override
    public DataType getDataType() {
        return DataType.CUSTOM;
    }

    @Override
    public UUID read(Buffer buffer, GraphBinaryReader context) throws IOException {
        // {custom type info}, {value_flag} and {value}
        // No custom_type_info
        if (buffer.readInt() != 0) {
            throw new SerializationException("{custom_type_info} should not be provided for this custom type");
        }

        return readValue(buffer, context, true);
    }

    @Override
    public UUID readValue(Buffer buffer, GraphBinaryReader context, boolean nullable) throws IOException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        // Read the byte length of the value bytes
        final int valueLength = buffer.readInt();

        if (valueLength <= 0) {
            throw new SerializationException(String.format("Unexpected value length: %d", valueLength));
        }

        if (valueLength > buffer.readableBytes()) {
            throw new SerializationException(
                    String.format("Not enough readable bytes: %d (expected %d)", valueLength, buffer.readableBytes()));
        }

        long msb = context.readValue(buffer, Long.class, false);
        long lsb = context.readValue(buffer, Long.class, false);
        return new UUID(msb, lsb);
    }

    @Override
    public void write(UUID value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        // Write {custom type info}, {value_flag} and {value}
        buffer.writeBytes(typeInfoBuffer);

        writeValue(value, buffer, context, true);
    }

    @Override
    public void writeValue(UUID value, Buffer buffer, GraphBinaryWriter context, boolean nullable) throws IOException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            context.writeValueFlagNone(buffer);
        }

        final Long msb = value.getMostSignificantBits();
        final Long lsb = value.getLeastSignificantBits();

        // value_length = name_byte_length + long + long
        buffer.writeInt(4 + 8 + 8);

        context.writeValue(msb, buffer, false);
        context.writeValue(lsb, buffer, false);
    }

}