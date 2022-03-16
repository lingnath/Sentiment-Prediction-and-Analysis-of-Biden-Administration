CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_analysis`.`twitter_raw` (
  `id` string,
  `username` string,
  `screenname` string,
  `tweet` string,
  `followerscount` int,
  `location` string,
  `geocoordinates` string,
  `createdate` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '	',
  'field.delim' = '	',
  'collection.delim' = '',
  'mapkey.delim' = ''
) LOCATION 's3://tweets-data-nl/2021/' -- Comes from the Kinesis Firehose instance that connected the Twitter API (containing raw tweets) to s3
TBLPROPERTIES ('has_encrypted_data'='false');