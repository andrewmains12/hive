package org.apache.hadoop.hive.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.mapred.JobConf;

/**
 * Simple extension of {@link TestHBaseKeyFactory2} with exception of using filters instead of start
 * and stop keys
 * */
public class TestHBaseKeyFactory3 extends TestHBaseKeyFactory2 {

  @Override
  public HBaseDecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    TestHBasePredicateDecomposer decomposedPredicate = new TestHBasePredicateDecomposer(keyMapping);
    return decomposedPredicate.decomposePredicate(keyMapping.columnName, predicate);
  }
}

class TestHBasePredicateDecomposer extends TestHBaseKeyFactory2.FixedLengthPredicateDecomposer {


  TestHBasePredicateDecomposer(ColumnMapping keyMapping) {
    super(keyMapping);
  }

  @Override
  public List<HBaseScanRange> getScanRanges(List<IndexSearchCondition> searchConditions)
      throws Exception {

    List<HBaseScanRange> rtn = Lists.newArrayList();

    // Translate start stop bounds on the scan range into filter objects
    // Note: the previous implementation of this method was incorrect:
    // it simply took the last condition in searchConditions as the value for the key, which
    // made it fail on conditions such as: key.col1 = 128 AND key.col2 = 1128 (since the filter would look for
    // "1128\x00\x00..." , but the key would be: "128\x00\x00..." .
    // This failure was masked by the fact that HiveHBaseTableInputFormat previously didn't
    // pass the filter objects along to the record reader, causing the erroneous filters to be dropped.
    for (HBaseScanRange scanRange : super.getScanRanges(searchConditions)) {
      Filter filter;

      if (Arrays.equals(scanRange.getStartRow(), scanRange.getStopRow())) {
        filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(scanRange.getStartRow()));
      } else {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filter = filterList;
        if (scanRange.getStartRow() != null) {
          filterList.addFilter(new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(scanRange.getStartRow())));
        }

        if (scanRange.getStopRow() != null) {
          filterList.addFilter(new RowFilter(CompareOp.LESS, new BinaryComparator(scanRange.getStopRow())));
        }
      }
      HBaseScanRange scanRangeWithFilters = new HBaseScanRange();

      scanRangeWithFilters.addFilter(filter);
      rtn.add(scanRangeWithFilters);
    }

    return rtn;
  }
}