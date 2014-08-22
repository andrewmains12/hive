package org.apache.hadoop.hive.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;

import javax.management.RuntimeErrorException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Key factory supporting fixed length composite row keys, where the first field in the row key is a "bucket" value intended
 * to distribute data. Predicate pushdown in this class works by pushing down predicates on the rest of the fields
 * to all buckets if a predicate on the bucket isn't provided.
 */
public class TestMultiScanHBaseKeyFactory extends TestHBaseKeyFactory2 {

  public static final String HIVE_HBASE_MULTISCAN_NUM_BUCKETS = "hive.hbase.multiscan.num_buckets";
  private int numBuckets;

  public static final List<Class<? extends GenericUDFBaseCompare>> ALLOWED_COMPARISONS = ImmutableList.of(
      GenericUDFOPGreaterThan.class,
      GenericUDFOPEqualOrGreaterThan.class,
      GenericUDFOPEqual.class,
      GenericUDFOPLessThan.class,
      GenericUDFOPEqualOrLessThan.class
  );

  @Override
  public void init(HBaseSerDeParameters hbaseParam, Properties properties) throws SerDeException {
    super.init(hbaseParam, properties);

    super.init(hbaseParam, properties);
    String numBucketsProp = properties.getProperty(HIVE_HBASE_MULTISCAN_NUM_BUCKETS);

    try {
      this.numBuckets = Integer.parseInt(numBucketsProp);
    } catch (NullPointerException e) {
      throw new SerDeException(
          "Expected table property " + HIVE_HBASE_MULTISCAN_NUM_BUCKETS + " to be a valid integer; was: " + numBucketsProp, e);
    } catch (NumberFormatException e) {
      throw new SerDeException(
          "Expected table property " + HIVE_HBASE_MULTISCAN_NUM_BUCKETS + " to be a valid integer; was: " + numBucketsProp, e);
    }
  }

  /**
   * Decompose predicates on fields of the row key into one or more HBaseScanRanges. This implementation supports
   * pushing down conditions into each bucket, creating one HBaseScanRange per bucket, if a condition on the bucket isn't
   * otherwise provided. For instance, given the schema:
   *
   * (bucket int, age int)
   *
   * and the condition:
   *
   * WHERE age >= 20 AND age < 25
   *
   * the method will return an HBaseScanRange for each bucket, with the age predicate pushed down:
   *
   * (1/20, 1/25)... (numBuckets/20, numBuckets/25)
   *
   * Given a condition on the bucket, it will return a single range:
   *
   * WHERE bucket = 1 AND age >= 20 AND age < 25 =\>
   *   (1/20, 1/25)
   *
   */
  @Override
  public HBaseDecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
    IndexPredicateAnalyzer analyzer = getIndexPredicateAnalyzer();

    List<IndexSearchCondition> searchConds = Lists.newArrayList();

    ExprNodeDesc residual = analyzer.analyzePredicate(predicate, searchConds);

    StructTypeInfo keyColumnType = (StructTypeInfo) keyMapping.columnType;
    String bucketCol = keyColumnType.getAllStructFieldNames().get(0);
    TypeInfo bucketTypeInfo = keyColumnType.getStructFieldTypeInfo(bucketCol);

    if (! (bucketTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE &&
        ((PrimitiveTypeInfo) bucketTypeInfo).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT)) {
      throw new IllegalArgumentException("Bucket field " + bucketCol + " must be of type int");
    }

    List<HBaseScanRange> scanRanges;

    try {
      // search for a condition already on the bucket
      boolean conditionOnBucket = false;
      for (IndexSearchCondition sc : searchConds) {
        if (sc.getFields()[0].equals(bucketCol)) {
          conditionOnBucket = true;
          break;
        }
      }

      FixedLengthPredicateDecomposer predicateDecomposer = new FixedLengthPredicateDecomposer(keyMapping);
      if (conditionOnBucket) {
        scanRanges = predicateDecomposer.getScanRanges(searchConds);
      } else {
        scanRanges = Lists.newArrayList();
        List<IndexSearchCondition> searchCondsWithBucket = cons(null, searchConds);
        // no condition on the bucket column; create an artificial one for each bucket value.
        for (int i = 0; i < this.numBuckets; i++) {
          ExprNodeColumnDesc keyColumnDesc = searchConds.get(0).getColumnDesc();
          searchCondsWithBucket.set(0, searchConditionForBucketValue(bucketCol, i, keyColumnDesc));
          scanRanges.add(predicateDecomposer.getScanRange(searchCondsWithBucket));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new HBaseDecomposedPredicate(analyzer.translateSearchConditions(searchConds), scanRanges, (ExprNodeGenericFuncDesc) residual);
  }

  /**
   * Create an artificial search condition on the bucket column.
   * @param bucketCol
   * @param value
   * @param keyColumnDesc
   * @return
   */
  private IndexSearchCondition searchConditionForBucketValue(String bucketCol, int value, ExprNodeColumnDesc keyColumnDesc) {
    return new IndexSearchCondition(
        new ExprNodeColumnDesc(
            keyMapping.getColumnType(),
            keyColumnDesc.getColumn(),
            keyColumnDesc.getTabAlias(),
            keyColumnDesc.getIsPartitionColOrVirtualCol()
        ),
        GenericUDFOPEqual.class.getCanonicalName(),

        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, value),
        null,
        new String[]{bucketCol}
    );
  }

  /**
   * Create a new list formed by inserting ele at the front of lst.
   * @param ele
   * @param lst
   * @param <T>
   * @return
   */
  private <T> List<T> cons(T ele, List<T> lst) {
    List<T> rtn = Lists.newArrayListWithCapacity(lst.size() + 1);
    rtn.add(ele);
    rtn.addAll(lst);
    return rtn;
  }

  private IndexPredicateAnalyzer getIndexPredicateAnalyzer() {
    IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(true);

    for (Class<? extends GenericUDFBaseCompare> compareOp : ALLOWED_COMPARISONS) {
      analyzer.addComparisonOp(compareOp.getCanonicalName());
    }
    analyzer.allowColumnName(keyMapping.getColumnName());
    analyzer.setAcceptsFields(true);
    return analyzer;
  }
}
