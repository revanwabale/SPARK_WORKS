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


concat_schema=  StructType([
    StructField("ts", StringType(), False),
    StructField("power",StringType(),False)    
])


ontime_schema = StructType([
    StructField("ymd", StringType(), False),
    StructField("one_tag", StringType(), False)
])


def on_time(user_id,app_type_id,ts_power,ymd):
    '''
    Calculating LateightOne tag and Inserting Into BigTable.
    Return:
       BigTable responces of LatenightOneTag Insertion.
    '''
    import msgpack  
    import settings
    reload(settings)
    import sys
    import os
    import numpy as np
    import datetime
    import time
    import calendar
    from datetime import timedelta,datetime,date
    from BigTable import insertCell
    threshold_minpower = settings.thresholds[str(app_type_id)][0]
    threshold_duration = settings.thresholds[str(app_type_id)][1]
    threshold_interval = settings.thresholds[str(app_type_id)][2]
    timestamp_list =[]
    special_user = settings.special_user
    ymd = int(ymd)
    table_name = settings.tablename
    if len(ts_power) == 0:
        count = 0
	one_tag =0
    else:
         
        for i in range(len(ts_power)):
            power = ts_power[i][1]
            if power > threshold_minpower:
                timestamp = int(ts_power[i][0])
                timestamp_list.append(timestamp)
        ts = np.array(timestamp_list)
        if ts.size == 0:			# Filtering eligible  powers.                                                           
            count = 0
	    one_tag =0

        else:
            ts.sort()
            one_tag = []
            start = int(ts[0])  
            for i in range(len(ts)):
                if i == (len(ts)) - 1:           # no activity on ontime
                    end = int(ts[i])
                    print("end:-" + str(int(ts[i])))
                    a_round = [start, end]  	 # start and end of array
                    one_tag.append(a_round)
                else:
                    if (int(ts[i+1]) - int(ts[i])) > threshold_interval:                
                        end = int(ts[i])
                        a_round = [start, end]
                        one_tag.append(a_round)
                        start = (ts[i+1])

            one_tag = [u for u in one_tag if u[1] - u[0] > threshold_duration]	# Ignore too-short usage

    one_tag_bt = []
    if(one_tag != 0):
        ''' Eligible for BIG TABLE INSERTION    '''
 	special_user, st_et = special_user.items()[0]
        if(user_id == special_user):
            for i in range(len(one_tag)):
                st= one_tag[i][0] + st_et[0]
                et = one_tag[i][1] + st_et[1]
                one_tag_bt.append([st,et])
       
        else:
    	    for i in range(len(one_tag)):
                st= one_tag[i][0]
                et = one_tag[i][1] + st_et[1]
                one_tag_bt.append([st,et])
        

        yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(ymd))
        idate = ymd
        table_id = '{}{}'.format(table_name,yyyymm)
        serialized_Otag = msgpack.packb(one_tag_bt)
        one_tag=insertCell(table_id,user_id, app_type_id, idate, 'OneTag', 'LOtag', serialized_Otag)
      	
    else:
        ''' Not Eligible for BIGTABLE INSERTION  '''
        one_tag = -1

    ymd = "{:%Y%m%d}".format(datetime.utcfromtimestamp(ymd))
    return(ymd,one_tag)


def timestamp_conversion(user_id,timestamp,ymd):
    '''
    Converting UTC timestamp to unixepoch timestamp for special_user and non special_users.
    '''
    import calendar
    import time
    from datetime import datetime,timedelta
    import settings
    reload(settings)

    ymd = int(ymd)
    hour = 3600

    special_user = settings.special_user
    special_user, st_et = special_user.items()[0]
    input_ts = timestamp[0:19]
    input_time_ts = time.strptime(input_ts,"%Y-%m-%d %H:%M:%S")

    if(user_id == special_user):
        timestamp = calendar.timegm(input_time_ts)
    else:
        input_time_epoch = calendar.timegm(input_time_ts)
        timestamp = input_time_epoch if ymd <= input_time_epoch <= (ymd + (hour*5)) else 0

    return(timestamp)


def main(spark,users_df):
    udf_on_time=udf(on_time,ontime_schema)
    udf_ts =udf(timestamp_conversion,IntegerType())
    concatUdf = udf(lambda ts,power:(ts,power),concat_schema)
    df=users_df.select("user_id","app_type_id","power",udf_ts(users_df.user_id,users_df.timestamp,users_df.ymd).alias("ts"),"ymd")
    df = df.filter(~col('ts').isin([0]))
    df_concat = df.select("user_id","app_type_id",concatUdf(df.ts,df.power).alias("ts_power"),"ymd")
    df_g = df_concat.groupBy('user_id','app_type_id',"ymd")
    df_ts_list = df_g.agg(collect_list('ts_power').alias('ts_power_list'))
    df = df_ts_list.select("user_id", "app_type_id", udf_on_time(df_ts_list.user_id,df_ts_list.app_type_id,"ts_power_list","ymd").alias("LateNightoneTag"))
    df.show(100,False)

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


def wait_for_job(job):
    while True:
        job.reload()  
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        print("In wait for job")
        time.sleep(1)

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
    if not table_id in instance.list_tables():
            table_id.create()
    
    for column_family_id in column_families_list:
        cf = column_family_id
        if not cf in table_id.list_column_families():  
                cf1 = table_id.column_family(cf)
                cf1.create()
    

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
    table_name="LateNight_{}".format(timestamp)
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
    now = now.replace(minute=0, hour=0, second=0, microsecond=0)       #jst 0th hour of today
    yesterday_22_jst = now + timedelta(hours=-11)                      #jst 23 in utc of yesterday
    today_05_jst = yesterday_22_jst + timedelta(hours=7, seconds=-1)   #jst 5th hour of today

    jst_0_utc = yesterday_22_jst + timedelta(hours=2)                  #jst 0th hour in utc
    ymd = calendar.timegm(jst_0_utc.timetuple())                       #JST_0_UTC to epoch ts

    part_st = datetime.strftime( yesterday_22_jst,'%Y%m%d')
    part_end = datetime.strftime(today_05_jst,'%Y%m%d')
    yesterday_22_jst = datetime.strftime(yesterday_22_jst,'%Y-%m-%d %H:%M:%S')
    today_05_jst = datetime.strftime(today_05_jst,'%Y-%m-%d %H:%M:%S')

    query = "SELECT user_id, data.timestamp as timestamp, data.app_types.app_type_id as app_type_id, data.app_types.apps.power as power \
             FROM "+db_tbl+" \
             WHERE _PARTITIONTIME BETWEEN TIMESTAMP('"+part_st+"') AND TIMESTAMP('"+part_end+"') \
             AND data.timestamp BETWEEN TIMESTAMP('"+yesterday_22_jst+"') AND TIMESTAMP('"+today_05_jst+"')" 

    JobName = 'LateNight_{}'.format(timestamp)
    qj = job.QueryJob(JobName, query, bq)
    qj.write_disposition = "WRITE_TRUNCATE"
    qj.use_legacy_sql = True
    qj.destination = table
    qj.begin()
    wait_for_job(qj)
    create_table(ymd)
    #--------------------------- Spark Job
    spark = SparkSession \
        .builder.master("yarn") \
        .appName("LateNight") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('LateNight script logger initialized at {}'.format(time.time())) 

    spark.sparkContext.addPyFile("./settings.py")
    spark.sparkContext.addPyFile("./BigTable.py")

    LOGGER.info('Intermediate table has been created successfully & Now Spark session has started.......') 
    bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
    project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")
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

    ''' Read the data from BigQuery into Spark as an RDD.'''
    table_data = spark.sparkContext.newAPIHadoopRDD(
        "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "com.google.gson.JsonObject",
        conf=conf)

    ''' Extract the JSON strings from the RDD.'''
    table_json = table_data.map(lambda x: x[1])

    ''' Load the JSON strings as a Spark Dataframe.'''
    tabledata = spark.read.json(table_json)
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
    df_300301 = spark.sql(query)
    users_df = users_df.unionAll(df_300301)
    users_df  = users_df.filter(~col('power').isin([0.0])| col('power').isNotNull())
    users_df = users_df.withColumn('ymd', lit(ymd))
    main(spark,users_df)
    LOGGER.info("Deleting Temporary {} table..".format(table))       
    table.delete()
    LOGGER.info("Intermediate Table{} deleted  succesfully after its use.".format(table))   
    LOGGER.info("=====LateNight Completed succesfully.=======")
    time4 = time.time()
    LOGGER.info("Total sec's to run a job using BBQ source for all users taken {} seconds.".format(time4-time1))
    spark.stop()
