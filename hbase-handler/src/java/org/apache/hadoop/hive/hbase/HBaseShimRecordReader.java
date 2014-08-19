package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * mapred.RecordReader wrapper around mapreduce.RecordReader
 */
public class HBaseShimRecordReader implements RecordReader<ImmutableBytesWritable, ResultWritable> {

  private final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result> recordReader;

  public HBaseShimRecordReader(org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result> recordReader) {
    this.recordReader = recordReader;
  }

  @Override
  public void close() throws IOException {
    recordReader.close();
  }

  @Override
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  @Override
  public ResultWritable createValue() {
    return new ResultWritable(new Result());
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    float progress = 0.0F;

    try {
      progress = recordReader.getProgress();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return progress;
  }

  @Override
  public boolean next(ImmutableBytesWritable rowKey, ResultWritable value) throws IOException {

    boolean next = false;

    try {
      next = recordReader.nextKeyValue();

      if (next) {
        rowKey.set(recordReader.getCurrentValue().getRow());
        value.setResult(recordReader.getCurrentValue());
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    return next;
  }
}
