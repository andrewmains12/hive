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
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat extends HiveMultiTableInputFormatBase
    implements InputFormat<ImmutableBytesWritable, ResultWritable> {

  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    // Take filter pushdown into account while calculating splits; this
    // allows us to prune off regions immediately.  Note that although
    // the Javadoc for the superclass getSplits says that it returns one
    // split per region, the implementation actually takes the scan
    // definition into account and excludes regions which don't satisfy
    // the start/stop row conditions (HBASE-1829).
    List<Scan> scans = createFilterScans(jobConf);
    // REVIEW:  are we supposed to be applying the getReadColumnIDs
    // same as in getRecordReader?

    for (Scan scan : scans) {
      pushScanColumns(scan, jobConf);
      configureScan(scan, jobConf);
    }

    setScans(scans);
    return super.getSplits(jobConf, numSplits);
  }

  private void configureScan(
    Scan scan,
    JobConf jobConf) throws IOException {

    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);

    if (hbaseTableName == null) {
      throw new IOException("HBase table must be specified in the JobConf");
    } else {
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(hbaseTableName));
    }

    String scanCache = jobConf.get(HBaseSerDe.HBASE_SCAN_CACHE);
    if (scanCache != null) {
      scan.setCaching(Integer.valueOf(scanCache));
    }
    String scanCacheBlocks = jobConf.get(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS);
    if (scanCacheBlocks != null) {
      scan.setCacheBlocks(Boolean.valueOf(scanCacheBlocks));
    }
    String scanBatch = jobConf.get(HBaseSerDe.HBASE_SCAN_BATCH);
    if (scanBatch != null) {
      scan.setBatch(Integer.valueOf(scanBatch));
    }
  }

  /**
   * Push the configured columns to be read into scan, reading all necessary values from jobConf and delegating
   * to {@link #pushScanColumns(org.apache.hadoop.hbase.client.Scan, ColumnMappings, java.util.List, boolean)}
   * @param scan
   * @param jobConf
   * @throws IOException
   */
  private void pushScanColumns(Scan scan, JobConf jobConf) throws IOException {
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    boolean doColumnRegexMatching = jobConf.getBoolean(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, true);
    ColumnMappings columnMappings;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping, doColumnRegexMatching);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    boolean readAllColumns = ColumnProjectionUtils.isReadAllColumns(jobConf);

    pushScanColumns(scan, columnMappings, readColIDs, readAllColumns);
  }

  private void pushScanColumns(Scan scan, ColumnMappings columnMappings, List<Integer> readColIDs, boolean readAllColumns) throws IOException {


    if (columnMappings.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean empty = true;

    // The list of families that have been added to the scan
    List<String> addedFamilies = new ArrayList<String>();

    if (!readAllColumns) {
      ColumnMapping[] columnsMapping = columnMappings.getColumnsMapping();
      for (int i : readColIDs) {
        ColumnMapping colMap = columnsMapping[i];
        if (colMap.hbaseRowKey) {
          continue;
        }

        if (colMap.qualifierName == null) {
          scan.addFamily(colMap.familyNameBytes);
          addedFamilies.add(colMap.familyName);
        } else {
          if(!addedFamilies.contains(colMap.familyName)){
            // add only if the corresponding family has not already been added
            scan.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
          }
        }

        empty = false;
      }
    }

    // The HBase table's row key maps to a Hive table column. In the corner case when only the
    // row key column is selected in Hive, the HBase Scan will be empty i.e. no column family/
    // column qualifier will have been added to the scan. We arbitrarily add at least one column
    // to the HBase scan so that we can retrieve all of the row keys and return them as the Hive
    // tables column projection.
    if (empty) {
      for (ColumnMapping colMap: columnMappings) {
        if (colMap.hbaseRowKey) {
          continue;
        }

        if (colMap.qualifierName == null) {
          scan.addFamily(colMap.familyNameBytes);
        } else {
          scan.addColumn(colMap.familyNameBytes, colMap.qualifierNameBytes);
        }

        if (!readAllColumns) {
          break;
        }
      }
    }
  }


  /**
   * Converts a filter (which has been pushed down from Hive's optimizer)
   * into corresponding restrictions on the HBase scan.
   *
   * If a list of HBaseScanRanges has been pushed as TableScanDesc.FILTER_OBJECT_CONF_STR, use that; otherwise use TableScanDesc.FILTER_EXPR_CONF_STR. If nothing has been
   * pushed, return a single element list containing an unbounded scan.
   * @return converted scan
   */
  private List<Scan> createFilterScans(JobConf jobConf) throws IOException {
    String filterObjectSerialized = jobConf.get(TableScanDesc.FILTER_OBJECT_CONF_STR);
    String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);

    // If nothing was pushed, exit early--some other things might not be present as well.
    if (filterObjectSerialized == null && filterExprSerialized == null) {
      return ImmutableList.of(new Scan());
    }

    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    boolean doColumnRegexMatching = jobConf.getBoolean(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, true);

    if (hbaseColumnsMapping == null) {
      throw new IOException("hbase.columns.mapping required for HBase Table.");
    }


    String listColumnsString = jobConf.get(serdeConstants.LIST_COLUMNS);
    String listColumnTypeString = jobConf.get(serdeConstants.LIST_COLUMN_TYPES);

    List<String> columns = Arrays.asList(listColumnsString.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(listColumnTypeString);

    String defaultStorageType = jobConf.get(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE, "string");

    ColumnMappings columnMappings;
    try {
      columnMappings = getColumnMappings(hbaseColumnsMapping, doColumnRegexMatching, columns, columnTypes, defaultStorageType);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    // TODO: do we always have a keyMapping?
    ColumnMapping keyMapping = columnMappings.getKeyMapping();


    List<HBaseScanRange> ranges = null;
    if (filterObjectSerialized != null) {
      ranges = (List<HBaseScanRange>) Utilities.deserializeObject(filterObjectSerialized,
          ArrayList.class);
    }

    ExprNodeGenericFuncDesc filterExpr = filterExprSerialized != null ?
        Utilities.deserializeExpression(filterExprSerialized) : null;

    boolean isKeyBinary = getStorageFormatOfKey(keyMapping.mappingSpec,
        defaultStorageType);

    return createFilterScans(ranges, filterExpr, keyMapping, isKeyBinary, jobConf);
  }

  private ColumnMappings getColumnMappings(String hbaseColumnMappings,
                                           boolean doColumnRegexMatching,
                                           List<String> columnsList,
                                           List<TypeInfo> columnTypeList,
                                           String defaultStorageType
                                           ) throws SerDeException {
    ColumnMappings columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnMappings, doColumnRegexMatching);

    // Only non virtual columns are mapped, and all of the virtual columns ought to be at the end. Therefore,
    // take only as many columns as there are in the mapping.
    List<String> nonVirtualColumns = columnsList.subList(0, columnMappings.size());
    List<TypeInfo> nonVirtualColumnTypes = columnTypeList.subList(0, columnMappings.size());
    columnMappings.setHiveColumnDescription(HBaseSerDe.class.getName(), nonVirtualColumns, nonVirtualColumnTypes);
    columnMappings.parseColumnStorageTypes(defaultStorageType);

    return columnMappings;
  }

  /**
   * Perform the actual conversion from pushed objects (deserialized from the JobConf)
   * to a List<Scan>
   */
  private List<Scan> createFilterScans(List<HBaseScanRange> ranges, ExprNodeGenericFuncDesc filterExpr, ColumnMapping keyMapping, boolean isKeyBinary, JobConf jobConf) throws IOException {

    // TODO: assert iKey is HBaseSerDe#HBASE_KEY_COL

    if (ranges != null) {
      List<Scan> rtn = Lists.newArrayListWithCapacity(ranges.size());

      for (HBaseScanRange range : ranges) {
        Scan scan = new Scan();
        try {
          range.setup(scan, jobConf);
        } catch (Exception e) {
          throw new IOException(e);
        }
        rtn.add(scan);
      }
      return rtn;
    } else if (filterExpr != null) {
      return ImmutableList.of(createScanFromFilterExpr(filterExpr, keyMapping, isKeyBinary));
    } else {
      // nothing pushed; just return an unbounded scan.
      return ImmutableList.of(new Scan());
    }
  }

  /**
   * Create a scan with the filters represented by filterExpr.
   * @param filterExpr
   * @param isKeyBinary
   * @return
   * @throws IOException
   */
  private Scan createScanFromFilterExpr(ExprNodeGenericFuncDesc filterExpr, ColumnMapping keyMapping, boolean isKeyBinary) throws IOException {
    Scan scan = new Scan();
    IndexPredicateAnalyzer analyzer = newIndexPredicateAnalyzer(keyMapping.getColumnName(), keyMapping.getColumnType(), isKeyBinary);

    List<IndexSearchCondition> searchConditions =
      new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate =
      analyzer.analyzePredicate(filterExpr, searchConditions);

    // There should be no residual since we already negotiated that earlier in
    // HBaseStorageHandler.decomposePredicate. However, with hive.optimize.index.filter
    // OpProcFactory#pushFilterToStorageHandler pushes the original filter back down again.
    // Since pushed-down filters are not ommitted at the higher levels (and thus the
    // contract of negotiation is ignored anyway), just ignore the residuals.
    // Re-assess this when negotiation is honored and the duplicate evaluation is removed.
    // THIS IGNORES RESIDUAL PARSING FROM HBaseStorageHandler#decomposePredicate
    if (residualPredicate != null) {
      LOG.debug("Ignoring residual predicate " + residualPredicate.getExprString());
    }

    // Convert the search condition into a restriction on the HBase scan
    byte [] startRow = HConstants.EMPTY_START_ROW, stopRow = HConstants.EMPTY_END_ROW;
    for (IndexSearchCondition sc : searchConditions){

      ExprNodeConstantEvaluator eval = new ExprNodeConstantEvaluator(sc.getConstantDesc());
      PrimitiveObjectInspector objInspector;
      Object writable;

      try {
        objInspector = (PrimitiveObjectInspector)eval.initialize(null);
        writable = eval.evaluate(null);
      } catch (ClassCastException cce) {
        throw new IOException("Currently only primitve types are supported. Found: " +
            sc.getConstantDesc().getTypeString());
      } catch (HiveException e) {
        throw new IOException(e);
      }

      byte [] constantVal = getConstantVal(writable, objInspector, isKeyBinary);
      String comparisonOp = sc.getComparisonOp();

      if("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual".equals(comparisonOp)){
        startRow = constantVal;
        stopRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan".equals(comparisonOp)){
        stopRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan"
          .equals(comparisonOp)) {
        startRow = constantVal;
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan"
          .equals(comparisonOp)){
        startRow = getNextBA(constantVal);
      } else if ("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan"
          .equals(comparisonOp)){
        stopRow = getNextBA(constantVal);
      } else {
        throw new IOException(comparisonOp + " is not a supported comparison operator");
      }
    }
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);

    if (LOG.isDebugEnabled()) {
      LOG.debug(Bytes.toStringBinary(startRow) + " ~ " + Bytes.toStringBinary(stopRow));
    }
    return scan;
  }

  private byte[] getConstantVal(Object writable, PrimitiveObjectInspector poi,
        boolean isKeyBinary) throws IOException{

        if (!isKeyBinary){
          // Key is stored in text format. Get bytes representation of constant also of
          // text format.
          byte[] startRow;
          ByteStream.Output serializeStream = new ByteStream.Output();
          LazyUtils.writePrimitiveUTF8(serializeStream, writable, poi, false, (byte) 0, null);
          startRow = new byte[serializeStream.getLength()];
          System.arraycopy(serializeStream.getData(), 0, startRow, 0, serializeStream.getLength());
          return startRow;
        }

        PrimitiveCategory pc = poi.getPrimitiveCategory();
        switch (poi.getPrimitiveCategory()) {
        case INT:
            return Bytes.toBytes(((IntWritable)writable).get());
        case BOOLEAN:
            return Bytes.toBytes(((BooleanWritable)writable).get());
        case LONG:
            return Bytes.toBytes(((LongWritable)writable).get());
        case FLOAT:
            return Bytes.toBytes(((FloatWritable)writable).get());
        case DOUBLE:
            return Bytes.toBytes(((DoubleWritable)writable).get());
        case SHORT:
            return Bytes.toBytes(((ShortWritable)writable).get());
        case STRING:
            return Bytes.toBytes(((Text)writable).toString());
        case BYTE:
            return Bytes.toBytes(((ByteWritable)writable).get());

        default:
          throw new IOException("Type not supported " + pc);
        }
      }


  private byte[] getNextBA(byte[] current){
    // startRow is inclusive while stopRow is exclusive,
    // this util method returns very next bytearray which will occur after the current one
    // by padding current one with a trailing 0 byte.
    byte[] next = new byte[current.length + 1];
    System.arraycopy(current, 0, next, 0, current.length);
    return next;
  }

  static IndexPredicateAnalyzer newIndexPredicateAnalyzer(
      String keyColumnName, TypeInfo keyColType, boolean isKeyBinary) {
    return newIndexPredicateAnalyzer(keyColumnName, keyColType.getTypeName(), isKeyBinary);
  }

  /**
   * Instantiates a new predicate analyzer suitable for
   * determining how to push a filter down into the HBase scan,
   * based on the rules for what kinds of pushdown we currently support.
   *
   * @param keyColumnName name of the Hive column mapped to the HBase row key
   *
   * @return preconfigured predicate analyzer
   */
  static IndexPredicateAnalyzer newIndexPredicateAnalyzer(
    String keyColumnName, String keyColType, boolean isKeyBinary) {

    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // We can always do equality predicate. Just need to make sure we get appropriate
    // BA representation of constant of filter condition.
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
    // We can do other comparisons only if storage format in hbase is either binary
    // or we are dealing with string types since there lexographic ordering will suffice.
    if(isKeyBinary || (keyColType.equalsIgnoreCase("string"))){
      analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic." +
        "GenericUDFOPEqualOrGreaterThan");
      analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");
      analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
      analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
    }

    // and only on the key column
    analyzer.clearAllowedColumnNames();
    analyzer.allowColumnName(keyColumnName);

    return analyzer;
  }

  private boolean getStorageFormatOfKey(String spec, String defaultFormat) throws IOException{

    String[] mapInfo = spec.split("#");
    boolean tblLevelDefault = "binary".equalsIgnoreCase(defaultFormat) ? true : false;

    switch (mapInfo.length) {
    case 1:
      return tblLevelDefault;

    case 2:
      String storageType = mapInfo[1];
      if(storageType.equals("-")) {
        return tblLevelDefault;
      } else if ("string".startsWith(storageType)){
        return false;
      } else if ("binary".startsWith(storageType)){
        return true;
      }

    default:
      throw new IOException("Malformed string: " + spec);
    }
  }
}
