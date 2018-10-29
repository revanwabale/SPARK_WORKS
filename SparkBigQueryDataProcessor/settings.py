#TODO For production edit the output_dir, hive_dir, bucket, dataset_prefix, table_schema, hive_db

time_units = 20
days_3_secs = 259200

app_type_list=[1,2,3,4,5,6]

#  'service provider id' : 'shared password'
servicePrPass={'XX':'XX','XX':'XX'}

#may require to change of bucket for production
bucket="xx"
gs_base = "gs://%s" % bucket

output_dir = gs_base + '/hadoop/tmp/bigquery/pyspark_output/'
hive_dir = gs_base + '/stg/data/umb/tbl_1/'

src_format='NEWLINE_DELIMITED_JSON'
projectid='XXXX'
#TODO
dataset_prefix="BigQuery_dev_sp"
table_estimate="xx"
table_xxx="xx"

#below working
table_schema={'xx':'/home/revan/xxx_json_schema1.json','xx':'/home/revan/xxx_json_schema1.json'}

imgateEstimate_url="https://xx"

imdb_url="jdbc:mysql://xx"
user="xx"
password="xx"
dbtable="xx"
column="id"
numPartitions="5"

hive_db="db_name"
hive_table="tbl_1"
