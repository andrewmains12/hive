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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util code common between HiveHBaseTableInputFormat and HiveHBaseTableSnapshotInputFormat.
 */
class HiveHBaseInputFormatUtil {

  /**
   * Parse {@code jobConf} to create the target {@link HTable} instance.
   */
  public static HTable getTable(JobConf jobConf) throws IOException {
    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    return new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName));
  }

  /**
   * Parse {@code jobConf} to create a {@link Scan} instance.
   */
  public static Scan getScan(JobConf jobConf) throws IOException {
    Scan scan = new Scan();
    configureScan(jobConf, scan);
    return scan;
  }

  public static void configureScan(JobConf jobConf, Scan scan) throws IOException {
    pushScanColumns(jobConf, scan);

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

  public static void pushScanColumns(JobConf jobConf, Scan scan) throws IOException {
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    boolean doColumnRegexMatching = jobConf.getBoolean(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, true);
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    ColumnMappings columnMappings;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping, doColumnRegexMatching);
    } catch (SerDeException e) {
      throw new IOException(e);
    }

    if (columnMappings.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean readAllColumns = ColumnProjectionUtils.isReadAllColumns(jobConf);

    pushScanColumns(scan, columnMappings, readAllColumns, readColIDs);
  }

  public static void pushScanColumns(Scan scan, ColumnMappings columnMappings, boolean readAllColumns, List<Integer> readColIDs) {
    boolean empty = true;

    // The list of families that have been added to the scan
    List<String> addedFamilies = new ArrayList<String>();

    if (!readAllColumns) {
      ColumnMapping[] columnsMapping = columnMappings.getColumnsMapping();
      for (int i : readColIDs) {
        ColumnMapping colMap = columnsMapping[i];
        if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
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
        if (colMap.hbaseRowKey || colMap.hbaseTimestamp) {
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

  public static boolean getStorageFormatOfKey(String spec, String defaultFormat) throws IOException{

    String[] mapInfo = spec.split("#");
    boolean tblLevelDefault = "binary".equalsIgnoreCase(defaultFormat);

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

  public static Map<String, List<IndexSearchCondition>> decompose(
      List<IndexSearchCondition> searchConditions) {
    Map<String, List<IndexSearchCondition>> result =
        new HashMap<String, List<IndexSearchCondition>>();
    for (IndexSearchCondition condition : searchConditions) {
      List<IndexSearchCondition> conditions = result.get(condition.getColumnDesc().getColumn());
      if (conditions == null) {
        conditions = new ArrayList<IndexSearchCondition>();
        result.put(condition.getColumnDesc().getColumn(), conditions);
      }
      conditions.add(condition);
    }
    return result;
  }
}
