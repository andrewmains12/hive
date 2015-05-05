/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.hbase.filter.FieldByteComparator;
import org.apache.hadoop.hive.hbase.filter.RowFilterFieldByteComparator;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.mapred.JobConf;

/**
 * Simple extension of {@link SampleHBaseKeyFactory2} with exception of using filters instead of start
 * and stop keys
 * */
public class SampleHBaseKeyFactory3 extends SampleHBaseKeyFactory2 {

  @Override
  public HBaseDecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    SampleHBasePredicateDecomposer decomposedPredicate = new SampleHBasePredicateDecomposer(keyMapping);
    return decomposedPredicate.decomposePredicate(keyMapping.columnName, predicate);
  }
}

class SampleHBasePredicateDecomposer extends AbstractHBaseKeyPredicateDecomposer {

  private static final int FIXED_LENGTH = 10;

  private ColumnMapping keyMapping;

  SampleHBasePredicateDecomposer(ColumnMapping keyMapping) {
    this.keyMapping = keyMapping;
  }

  @Override
  public HBaseScanRange getScanRange(List<IndexSearchCondition> searchConditions)
      throws Exception {
    Map<String, List<IndexSearchCondition>> fieldConds =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : searchConditions) {
      String fieldName = condition.getFields()[0];
      List<IndexSearchCondition> fieldCond = fieldConds.get(fieldName);
      if (fieldCond == null) {
        fieldConds.put(fieldName, fieldCond = new ArrayList<IndexSearchCondition>());
      }
      fieldCond.add(condition);
    }
    Filter filter = null;
    HBaseScanRange range = new HBaseScanRange();

    StructTypeInfo type = (StructTypeInfo) keyMapping.columnType;
    int offset = - FIXED_LENGTH;
    for (String name : type.getAllStructFieldNames()) {
      offset += FIXED_LENGTH;
      List<IndexSearchCondition> fieldCond = fieldConds.get(name);
      if (fieldCond == null || fieldCond.size() > 2) {
        continue;
      }
      filter = searchConditionsToFilter(offset, fieldCond);
      if (filter != null) {
        range.addFilter(filter);
      }
    }
    return range;
  }

  public Filter searchConditionsToFilter(int offset, List<IndexSearchCondition> fieldCond)
      throws IOException {
    Filter filter = null;
    for (IndexSearchCondition condition : fieldCond) {
      if (condition.getConstantDesc().getValue() == null) {
        continue;
      }
      String comparisonOp = condition.getComparisonOp();
      String constantVal = String.valueOf(condition.getConstantDesc().getValue());

      byte[] valueAsBytes = SampleHBaseKeyFactory2.toBinary(constantVal, FIXED_LENGTH, false, false);
      FieldByteComparator rowComparator =
          new FieldByteComparator(valueAsBytes, offset, FIXED_LENGTH);
      if (comparisonOp.endsWith("UDFOPEqual")) {
        filter = new RowFilterFieldByteComparator(CompareOp.EQUAL, rowComparator);
      } else if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {
        filter = new RowFilterFieldByteComparator(CompareOp.GREATER_OR_EQUAL, rowComparator);
      } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {
        filter = new RowFilterFieldByteComparator(CompareOp.GREATER, rowComparator);
      } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {
        filter = new RowFilterFieldByteComparator(CompareOp.LESS_OR_EQUAL, rowComparator);
      } else if (comparisonOp.endsWith("UDFOPLessThan")) {
        filter = new RowFilterFieldByteComparator(CompareOp.LESS, rowComparator);
      } else {
        throw new IOException(comparisonOp + " is not a supported comparison operator");
      }
    }
    return filter;
  }
}