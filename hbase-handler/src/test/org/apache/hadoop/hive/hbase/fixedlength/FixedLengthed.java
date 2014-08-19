package org.apache.hadoop.hive.hbase.fixedlength;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;

import java.util.ArrayList;
import java.util.List;

public class FixedLengthed implements LazyObjectBase {

  public int getFixedLength() {
    return fixedLength;
  }

  public List<Object> getFields() {
    return fields;
  }

  private final int fixedLength;
  private final List<Object> fields = new ArrayList<Object>();

  public FixedLengthed(int fixedLength) {
    this.fixedLength = fixedLength;
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    fields.clear();
    byte[] data = bytes.getData();
    int rowStart = start;
    int rowStop = rowStart + fixedLength;
    for (; rowStart < length; rowStart = rowStop + 1, rowStop = rowStart + fixedLength) {
      fields.add(new String(data, rowStart, rowStop - rowStart).trim());
    }
  }

  @Override
  public Object getObject() {
    return this;
  }
}
