CREATE TABLE hbase_multiscan(
  key struct<bucket : int,
             other_val: string>,
  value string)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.hbase.HBaseSerDe'
  STORED BY
  'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES (
  'serialization.format'='1',
  'hbase.columns.mapping'=':key,cf:string',
  'hbase.composite.key.factory'='org.apache.hadoop.hive.hbase.TestMultiScanHBaseKeyFactory'
  )
  TBLPROPERTIES (
  'hbase.table.name'='mscan2',   'hive.hbase.multiscan.num_buckets'='3');

FROM src
INSERT INTO TABLE hbase_multiscan SELECT
named_struct(
        "bucket",  hash(cast(key as int)) % 3,
        "other_val", cast(key as string)),
cast(value as string);


-- {"bucket":"1","other_val":"10"}	val_10
-- {"bucket":"1","other_val":"100"}	val_100
-- {"bucket":"1","other_val":"103"}	val_103
-- {"bucket":"1","other_val":"118"}	val_118
SELECT * FROM hbase_multiscan WHERE key.bucket = 1 AND key.other_val >= "0" AND key.other_val < "12" ORDER BY value;

-- {"bucket":"2","other_val":"128"}	val_128
SELECT * FROM hbase_multiscan WHERE key.other_val = "128";

DROP TABLE hbase_multiscan;
