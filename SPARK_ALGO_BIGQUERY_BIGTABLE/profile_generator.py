from pyspark.sql import SparkSession, Row
from pyspark import SparkFiles, SparkConf, SparkContext
from pyspark import SQLContext as sqlContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import argparse
import time
import os
import sys

profile_schema = StructType([
    StructField("ymd", StringType(), False),
    StructField("max_power", StringType(), False),
    StructField("min_power", StringType(), False)
])


def power_minmax(user_id,app_type_id,min_pow,max_pow,ymd):
    '''
    Inserting minimum_power , maximum_power into BigTable
    Return:
       BigTable responces of min_pow,max_pow.
    '''
    import settings
    reload(settings)
    import sys
    import os
    import datetime
    import time
    import calendar
    from datetime import timedelta,datetime,date
    from BigTable import insertCell
    
    table_name = settings.tablename
    yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(ymd))
    table_id = '{}{}'.format(table_name,yyyymm)
    
    max_power=insertCell(table_id,user_id, app_type_id, ymd, 'Weekly', '2wMxpwr', max_pow)
    min_power=insertCell(table_id,user_id, app_type_id, ymd, 'Weekly', '2wMnpwr', min_pow)
    ymd = "{:%Y%m%d}".format(datetime.utcfromtimestamp(ymd))
    return(ymd,max_power,min_power)


def sumOfPower(pow1, pow2):
    '''
    Sum pow1 and pow2.
           null + null = 0.0
           null + number = number
 	   number + null = number
           number + number = number
    Args:
       Pow1, Pow2 
    Returns:
       sum of powers in float.
    '''
    power1 = str(pow1).strip()
    power2 = str(pow2).strip()
    if not power1 == 'None' and not power1 == 'null':
        if not power2 == 'None' and not power2 == 'null':
            return float(float(power1) + float(power2))
        else:
            return float(power1)
    else:
        if not power2 == 'None' and not power2 == 'null':
            return float(power2)
        else:
            return float(0)


def create_table(ymd):
    '''
    Creating Table, ColumnFamilies if not exists for that month.
    args:
        ymd - unix epoch timestamp
    
    '''
    import os
    import sys
    import pip
    import site
    reload(site)
    import settings
    reload(settings)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.credentials
    from google.cloud import bigtable
    project_id = settings.bt_project_id   
    instance_id = settings.bt_instance_id     
    tablename = settings.tablename
    column_families_list = settings.column_family_id
    yyyymm = "{:%Y%m}".format(datetime.utcfromtimestamp(ymd))
    table_id = '{}{}'.format(tablename,yyyymm)
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table_id = instance.table(table_id)
    if not table_id in instance.list_tables():			#create table if not present
            table_id.create()
    for column_family_id in column_families_list:		
        cf = column_family_id
        if not cf in table_id.list_column_families():  
                cf1 = table_id.column_family(cf)
                cf1.create()					#create columnfamily if not present
    

def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        print ("In wait for job")
        time.sleep(1)



if __name__ == "__main__":
    import time
    warehouse_location = 'file:${system:user.dir}/spark-warehouse'
    from google.cloud import bigquery
    from google.cloud.bigquery import job
    from google.cloud.bigquery.table import *
    from datetime import datetime

    time1 = time.time()
    import settings
    reload(settings)

    bq = bigquery.Client(project=settings.project)
    reg_dataset = bq.dataset(settings.datasetid)
    if not reg_dataset.exists():
            print('Dataset {} does not exist.'.format(reg_dataset))
	    reg_dataset.create()
    
    timestamp= int(time.time())
    table_name="ProfileGenerator_{}".format(timestamp)
    table = reg_dataset.table(name=table_name)
    schema = []
    schema.append(SchemaField("user_id", "STRING"))
    schema.append(SchemaField("timestamp", "TIMESTAMP"))
    schema.append(SchemaField("app_type_id", "STRING"))
    schema.append(SchemaField("power", "FLOAT"))
    table.schema = schema
    if not table.exists():
            print('Table {}:{} does not exist.'.format(reg_dataset, table_name))
            table.create()

    db_tbl = "" 
    for sr_prv in settings.servicePrPass.keys():
        db_tbl = db_tbl + "["+ settings.project + ":" + settings.dataset_prefix + str(sr_prv)+"." + settings.bbq_table + "],"
    db_tbl=db_tbl.rstrip(',')
    
    import pytz
    jst = pytz.timezone('Asia/Tokyo')
    now = ""
    if len(sys.argv) == 2:
      now = datetime.strptime(sys.argv[1]+' 00:00:00', '%m%d%Y %H:%M:%S')
    else:
      ts = time.time()
      now = datetime.fromtimestamp(ts, jst)

    import calendar
    now = now.replace(minute=0, hour=0, second=0, microsecond=0)  #jst 0th hour of today
    yesterday_0_jst = now + timedelta(days=-1, hours=-9)          #jst 0 in utc of yesterday
    yesterday_24_jst = yesterday_0_jst + timedelta(hours=24, seconds=-1)
    last2weeks_0_jst = yesterday_24_jst + timedelta(weeks=-2, seconds=1)

    jst_0_utc = yesterday_24_jst + timedelta(seconds=1)
    ymd = calendar.timegm(jst_0_utc.timetuple())                  #JST_0_UTC to epoch ts

    part_st = datetime.strftime(last2weeks_0_jst,'%Y%m%d')       #partition start date
    part_end = datetime.strftime(yesterday_24_jst,'%Y%m%d')      #partition end date
    last2weeks_0_jst = datetime.strftime(last2weeks_0_jst,'%Y-%m-%d %H:%M:%S')  #start timestamp
    yesterday_24_jst = datetime.strftime(yesterday_24_jst,'%Y-%m-%d %H:%M:%S')  #end timestamp

    query = "SELECT user_id, data.timestamp as timestamp, data.app_types.app_type_id as app_type_id, data.app_types.apps.power as power \
             FROM "+db_tbl+" \
             WHERE _PARTITIONTIME BETWEEN TIMESTAMP('"+part_st+"') AND TIMESTAMP('"+part_end+"') \
             AND data.timestamp BETWEEN TIMESTAMP('"+last2weeks_0_jst+"') AND TIMESTAMP('"+yesterday_24_jst+"')" 

    JobName = 'ProfileGenerator-{}'.format(timestamp)
    qj = job.QueryJob(JobName, query, bq)
    qj.write_disposition="WRITE_TRUNCATE"
    qj.use_legacy_sql = True
    qj.destination = table
    qj.begin()
    wait_for_job(qj)
    create_table(ymd)

    #--------------------------- Spark Job
    spark = SparkSession \
        .builder.master("yarn") \
        .appName("ProfileGenerator") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
    spark.sparkContext.addPyFile("./settings.py")
    spark.sparkContext.addPyFile("./BigTable.py")
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('ProfileGenerator script logger initialized at {}'.format(time.time())) 

    bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
    project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")
    ''' Set an input directory for reading data from Bigquery.'''
    input_directory = "gs://{}/tmp/{}-{}".format(bucket,JobName,timestamp)   
    conf = {
        # Input Parameters
        "mapred.bq.project.id": project,
        "mapred.bq.gcs.bucket": bucket,
        "mapred.bq.temp.gcs.path": input_directory,
        "mapred.bq.input.project.id": project,
        "mapred.bq.input.dataset.id": settings.datasetid,
        "mapred.bq.input.table.id": table_name,
    }

    '''Read the data from BigQuery into Spark as an RDD.'''
    table_data = spark.sparkContext.newAPIHadoopRDD(
        "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "com.google.gson.JsonObject",
        conf=conf)

    ''' Extract the JSON strings from the RDD.'''
    table_json = table_data.map(lambda x: x[1])

    ''' Load the JSON strings as a Spark Dataframe.'''
    tabledata = spark.read.json(table_json)
    tabledata.printSchema()
    time2 = time.time()
    LOGGER.info("Total sec's to create DF by using BBQ source is {}".format(time2-time1)) 
    eligible_app_type_id = settings.thresholds.keys()
    df_filterapp=tabledata.filter(col('app_type_id').isin(eligible_app_type_id))              
    users_df =df_filterapp.select("user_id","app_type_id","timestamp","power").distinct()

    users_300 = users_df.filter(col('app_type_id').isin([300]))
    users_301 = users_df.filter(col('app_type_id').isin([301]))
    users_300.createOrReplaceTempView("df_300")
    users_301.createOrReplaceTempView("df_301")
    spark.udf.register("sumOfPower",sumOfPower,FloatType())
    query = """
        SELECT 
        CASE WHEN t1.user_id   != 'null' THEN t1.user_id ELSE t2.user_id END AS user_id, 
        '300301' AS app_type_id,
        CASE WHEN t1.timestamp   != 'null' THEN t1.timestamp ELSE t2.timestamp END AS timestamp,
        sumOfPower(t1.power,t2.power) AS power
        FROM df_300 t1 FULL OUTER JOIN df_301 t2 
        ON t1.timestamp = t2.timestamp
        """
    df_300301 =spark.sql(query)
    all_users_df = users_df.unionAll(df_300301)
    users_df = all_users_df.select("user_id","app_type_id","power").distinct()
    users_df  = users_df.filter(~col('power').isin([0.0])| col('power').isNotNull())
    users_df = users_df.withColumn('ymd', lit(ymd))
    users_df.persist()
    udf_min_max=udf(power_minmax,profile_schema)
    users_df.createOrReplaceTempView("TwoWeeksData")
    query= "SELECT user_id,app_type_id, MIN(power) as MIN_POWER, MAX(power) as MAX_POWER, ymd FROM TwoWeeksData GROUP BY user_id,app_type_id,ymd"
    df_min_max = spark.sql(query)
    max_min_df=df_min_max.select("user_id","app_type_id",udf_min_max(df_min_max.user_id,df_min_max.app_type_id,df_min_max.MIN_POWER,df_min_max.MAX_POWER,df_min_max.ymd).alias("MinMaxPower"))
    max_min_df.show(100,False)
    time3 = time.time()
    LOGGER.info("Total sec's to compute all results for all users is:: {}".format(time3-time1))
    spark.stop()
