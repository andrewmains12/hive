package org.apache.hadoop.hive.hbase.fixedlength;

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import java.util.ArrayList;
import java.util.List;

public class StringArrayOI extends BaseStructObjectInspector {

  private int length;

  public StringArrayOI(StructTypeInfo type) {
    List<String> names = type.getAllStructFieldNames();
    List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    for (int i = 0; i < names.size(); i++) {
      ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    init(names, ois, null);
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    return ((FixedLengthed)data).getFields().get(((MyField)fieldRef).getFieldID());
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    return ((FixedLengthed)data).getFields();
  }
}
