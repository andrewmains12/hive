package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * mapred port of {@link org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase}
 */
public class HiveMultiTableInputFormatBase extends MultiTableInputFormatBase implements InputFormat<ImmutableBytesWritable, ResultWritable> {

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int splitNum) throws IOException {
    //obtain delegation tokens for the job
    TableMapReduceUtil.initCredentials(jobConf);
    Job job = new Job(jobConf);
    org.apache.hadoop.mapreduce.JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);


    List<org.apache.hadoop.mapreduce.InputSplit> splits = super.getSplits(jobContext);

    //Convert mapreduce splits to mapred splits
    InputSplit[] rtn = new InputSplit[splits.size()];

    for (int i = 0; i < splits.size(); i++) {
      rtn[i] = new HBaseSplit((TableSplit) splits.get(i), FileInputFormat.getInputPaths(jobConf)[0]);
    }

    return rtn;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, ResultWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    HBaseSplit hbaseSplit = (HBaseSplit) inputSplit;
    TableSplit tableSplit = hbaseSplit.getSplit();
    Job job = new Job(jobConf);
    HadoopShims hadoopShims = ShimLoader.getHadoopShims();
    TaskAttemptContext tac = hadoopShims.newTaskAttemptContext(
        job.getConfiguration(), reporter);

    final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result> recordReader;
    try {
      recordReader = createRecordReader(tableSplit, tac);
      recordReader.initialize(tableSplit, tac);
    } catch (InterruptedException e) {
      throw new IOException("Failed to initialized RecordReader", e);
    }

    return new HBaseShimRecordReader(recordReader);
  }

}
