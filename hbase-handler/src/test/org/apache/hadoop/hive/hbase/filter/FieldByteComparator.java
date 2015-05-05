package org.apache.hadoop.hive.hbase.filter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;


public class FieldByteComparator extends ByteArrayComparable {

    private int fieldLength;
    private int offsetForField;

    public FieldByteComparator(byte[] value, int offsetForField, int fieldLength) {
        super(value);
        this.offsetForField = offsetForField;
        this.fieldLength = fieldLength;
    }

    /**
     * Special compareTo method for subclasses, to avoid
     * copying byte[] unnecessarily.
     *
     * @param value  byte[] to compare
     * @param offset offset into value
     * @param length number of bytes to compare
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(byte[] row, int offset, int length) {

        int dataDimOffset = offset + this.offsetForField;

        assert dataDimOffset < offset + length;
        byte[] value = getValue();
        return Bytes.compareTo(value, 0, value.length, row, dataDimOffset, fieldLength);
    }

    @Override
    public byte[] toByteArray(){
        ByteString.Output valueOut = ByteString.newOutput(getValue().length);
        valueOut.write(getValue(), 0, getValue().length);

        HiveHBaseFilterProtos.FieldByteComparator.Builder builder =
                HiveHBaseFilterProtos.FieldByteComparator.newBuilder()
                        .setFieldLength(fieldLength)
                        .setOffsetForField(offsetForField)
                        .setValue(valueOut.toByteString());

        return builder.build().toByteArray();
    }

    public static FieldByteComparator parseFrom(final byte [] pbBytes)
            throws DeserializationException {
        HiveHBaseFilterProtos.FieldByteComparator proto;
        try {
            proto = HiveHBaseFilterProtos.FieldByteComparator.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new FieldByteComparator(proto.getValue().toByteArray(),
                proto.getOffsetForField(),
                proto.getFieldLength()
        );
    }

}
