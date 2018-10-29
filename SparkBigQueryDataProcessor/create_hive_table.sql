CREATE DATABASE db_name;

use db_name;

CREATE EXTERNAL TABLE TBL_1
(
  user_id STRING, 
  commited_at STRING
)
 COMMENT 'This is the staging TBL_1 table to store fixed_at values for data_processor runs'
 PARTITIONED BY (et_hr int)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 STORED AS TEXTFILE
 LOCATION 'gs://XXXX';

-- ALTER TABLE TBL_1 ADD PARTITION (et_hr=2017012512) location 'gs:/XXXX/2017012512';
-- ALTER TABLE TBL_1 ADD PARTITION (2017012515) location 'gs://XXXX/2017012515';
