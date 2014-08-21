package org.apache.hadoop.hive.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import java.io.Serializable;
import java.util.List;

/**
 * A DecomposedPredicate for the HBase handler. The primary customization is that the pushedPredicateObject must be
 * a List<HBaseScanRange>. You *could* hack around this, since the fields are public, but {@link org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat}
 * won't understand any other types.
 */
public class HBaseDecomposedPredicate extends HiveStoragePredicateHandler.DecomposedPredicate {

  public HBaseDecomposedPredicate(ExprNodeGenericFuncDesc pushedPredicate, List<HBaseScanRange> pushedPredicateObject, ExprNodeGenericFuncDesc residualPredicate) {
    super(pushedPredicate, getListAsSerializable(pushedPredicateObject), residualPredicate);
  }

  /**
   * Ensure that pushedPredicateObject is serializable by checking its runtime type. If it's not an instance of Serializable,
   * copy it to an ArrayList (which is Serializable)
   * @param pushedPredicateObject
   * @return
   */
  private static <T extends Serializable> Serializable getListAsSerializable(List<T> pushedPredicateObject) {
    Serializable serializablePushedPred;
    if (! (pushedPredicateObject instanceof Serializable)) {
      serializablePushedPred = Lists.newArrayList(pushedPredicateObject);
    } else {
      serializablePushedPred = (Serializable) pushedPredicateObject;
    }
    return serializablePushedPred;
  }
}
