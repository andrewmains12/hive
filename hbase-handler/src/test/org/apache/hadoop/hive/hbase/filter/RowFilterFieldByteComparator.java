package org.apache.hadoop.hive.hbase.filter;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.RowFilter;

/**
 * Simple RowFilter subclass that implements JsonPrintable for query plan printing
 */
public class RowFilterFieldByteComparator extends RowFilter {

    public RowFilterFieldByteComparator(CompareOp rowCompareOp, FieldByteComparator rowComparator) {
        super(rowCompareOp, rowComparator);
        comparator = rowComparator;
    }

    public static RowFilterFieldByteComparator parseFrom(final byte[] pbBytes) throws DeserializationException {
        RowFilter tempFilter = RowFilter.parseFrom(pbBytes);
        return new RowFilterFieldByteComparator(tempFilter.getOperator(), (FieldByteComparator) tempFilter.getComparator());
    }
}

