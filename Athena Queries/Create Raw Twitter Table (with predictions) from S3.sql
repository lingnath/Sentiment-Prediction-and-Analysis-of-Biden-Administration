CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_analysis`.`twitter_labelled` (
  `id` string,
  `username` string,
  `screenname` string,
  `tweetdirty` string,
  `followerscount` int,
  `location` string,
  `geocoordinates` string,
  `createdate` string,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://tweets-data-nl/exporttos3test/'
TBLPROPERTIES ('has_encrypted_data'='true');