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
    StructField("one_tag", StringType(), False),
    StructField("count", StringType(), False),
    StructField("duration", StringType(), False),
    StructField("unusual_h_cnt", StringType(), False),
    StructField("unusual_h_dur", StringType(), False),
    StructField("unusual_l_dur", StringType(), False),
    StructField("unusual_l_cnt", StringType(), False),
    StructField("interval_days", StringType(), False)
])


def on_time(user_id,app_type_id,ts_power,ymd):
    '''
    Calculating count,duration,one-tag,unusual things, interval days.
    Return:
       BigTable Insertion responces of count,duration,one-tag,unusual things, interval days.
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
    from BigTable import readCell
    threshold_minpower = settings.thresholds[str(app_type_id)][0]
    threshold_duration = settings.thresholds[str(app_type_id)][1]
    threshold_interval = settings.thresholds[str(app_type_id)][2]
    timestamp_list=[]
    day = 86400    
    week = day*7
    active_count = []
    ymd = int(ymd)
    count = 0
    duration = 0
    unusual_h_cnt = False
    unusual_l_cnt = False       
    unusual_h_dur = False
    unusual_l_dur = False 
    interval_days = -1
    one_tag = 0

    if len(ts_power) == 0: 
        count = 0
        duration = 0
        unusual_h_cnt = False
        unusual_l_cnt = False       
        unusual_h_dur = False
        unusual_l_dur = False 
        interval_days = -1
	one_tag = 0
    else:
         
        for i in range(len(ts_power)):			# Filtering eligible powers.  
            power = ts_power[i][1]
            if power > threshold_minpower:
                timestamp = int(ts_power[i][0])
                timestamp_list.append(timestamp)
        ts = np.array(timestamp_list)
        if ts.size == 0:                                                      
            count = 0
            duration = 0
            unusual_h_cnt = False
            unusual_l_cnt = False       
    	    unusual_h_dur = False
    	    unusual_l_dur = False 
    	    interval_days = -1
	    one_tag = 0

        else:			#Eligible Powers
            ts.sort()
            one_tag = []
            start = int(ts[0])  
            for i in range(len(ts)):
                if i == (len(ts)) - 1:           		# no activity on ontime
                    end = int(ts[i])
                    a_round = [start, end]  
                    one_tag.append(a_round)
                else:
                    if (int(ts[i+1]) - int(ts[i])) > threshold_interval:                
                        end = int(ts[i])
                        a_round = [start, end]			# start and end of array
                        one_tag.append(a_round)
                        start = (ts[i+1])

            one_tag = [u for u in one_tag if u[1] - u[0] > threshold_duration]  
            count = int(len(one_tag))
            duration = int(np.diff(one_tag).sum())     
            user_record=[]
    	    cal_count = 0
    	    cal_duration = 0
    	    table_name = settings.tablename
            pastday_ymd = ymd - (day*1)            
            past_8weeks_ymd = pastday_ymd - (week*8)
            delta = pastday_ymd - past_8weeks_ymd
     	    delta_days = delta/day
    	    for i in range(1,delta_days+1):
                idate = past_8weeks_ymd + (day*i)
	        yyyymm = "{:%Y%m}".format(datetime.utcfromtimestamp(idate))
                table_id = '{}{}'.format(table_name,yyyymm)
                cal_count = readCell(table_id,user_id, app_type_id, idate,'Daily','cnt')
                cal_duration = readCell(table_id,user_id, app_type_id, idate,'Daily','dur')
                if(cal_count != None and cal_duration !=None):
                    user_record.append((cal_count,cal_duration))
	        if(cal_count != None and cal_count > 0): 	#ActiveCount for Intervaldays calculations
                    active_count.append(idate)

            past_days_active= np.array(active_count)

            past_days = np.array(user_record).astype(np.int)
	
            if len(past_days)!=0:				#filtering new_user or not active since past 8weeks users

                ave_count = np.mean(map(lambda x: x[0], 
                                    filter(lambda x: x[0] != 0, past_days)))	# Filter out zero usages and take average/std of the rest
                ave_duration = np.mean(map(lambda x: x[1], 
                                       filter(lambda x: x[0] != 0, past_days)))
            	std_count = np.std(map(lambda x: x[0], 
                                   filter(lambda x: x[0] != 0, past_days)))
            	std_duration = np.std(map(lambda x: x[1], 
                                      filter(lambda x: x[0] != 0, past_days)))
            	STD_MULTIPLE = settings.STD_MULTIPLE		
            	count_th = STD_MULTIPLE * std_count		# Define thresholds for outlier (i.e. 3x standard deviation)
            	duration_th = STD_MULTIPLE * std_duration
            	if count != 0:					#current count is active ?
                    large_top_count = ave_count + count_th	# Unusual count?
                    small_bottom_count = ave_count - count_th

                    if count > large_top_count:
                        unusual_h_cnt = True
                    elif count < small_bottom_count:
                    	unusual_l_cnt = True      
  
                    large_top_duration = ave_duration + duration_th	# Unusual count?
                    small_bottom_duration = ave_duration - duration_th

                    if duration > large_top_duration:
                        unusual_h_dur = True
                    elif duration < small_bottom_duration:
                          unusual_l_dur = True                            
                          
		else:						# current count is not active 
            	    unusual_h_cnt = False
            	    unusual_l_cnt = False       
            	    unusual_h_dur = False
           	    unusual_l_dur = False  
	
                                     
	    if count != 0:
            	if(len(past_days_active)==0):		#filtering new_user or not active since past 8weeks users
                    interval_days = -1 
            	else:
		
                    interval_days = int((ymd-past_days_active[-1])/day) 
 		    if(interval_days > 28):		#assigining -1 if interval_days result is more than 28days.
			interval_days = -1
			
	    else:
		interval_days = -1           

    if(count != 0 and duration !=0 and one_tag !=0):
	'''Eligible For BigTable Insertion'''
        yyyymm = "{:%Y%m}".format(datetime.utcfromtimestamp(ymd))
   	idate = ymd
        table_id = '{}{}'.format(table_name,yyyymm)
        serialized_Otag = msgpack.packb(one_tag)
        one_tag=insertCell(table_id,user_id, app_type_id, idate, 'OneTag', 'Otag', serialized_Otag)
        count=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'cnt', count)
        duration=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'dur', duration)
        unusual_h_cnt=insertCell(table_id,user_id, app_type_id,idate, 'Daily', 'uHcnt', unusual_h_cnt)
        unusual_l_cnt=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'uLcnt', unusual_l_cnt)
        unusual_h_dur=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'uHdur', unusual_h_dur)
        unusual_l_dur=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'uLdur', unusual_l_dur)
        interval_days=insertCell(table_id,user_id, app_type_id, idate, 'Daily', 'iDay', interval_days)
    else:
	'''Not Eligible For BigTable Insertion '''
        one_tag=count=duration=unusual_h_cnt=unusual_l_cnt=unusual_h_dur=unusual_l_dur=interval_days = -1
    ymd = "{:%Y%m%d}".format(datetime.utcfromtimestamp(ymd))
    return(ymd,str(one_tag),count,duration,unusual_h_cnt,unusual_l_cnt,unusual_h_dur,unusual_l_dur,interval_days)

def timestamp_conversion(timestamp):
    '''
    Converting UTC timestamp to unix epoch timestamp
    '''
    import calendar
    ts=timestamp[0:19]
    t=time.strptime(ts,"%Y-%m-%d %H:%M:%S")
    return(calendar.timegm(t))


def main(spark,users_df):       
    import time
    udf_ts = udf(timestamp_conversion,IntegerType())
    concatUdf = udf(lambda ts,power:(ts,power),concat_schema)
    df=users_df.select("user_id","app_type_id","power",udf_ts(users_df.timestamp).alias("ts"),"ymd")
    df_concat = df.select("user_id","app_type_id",concatUdf(df.ts,df.power).alias("ts_power"),"ymd")
    df_g = df_concat.groupBy("user_id","app_type_id","ymd")
    df_ts_list = df_g.agg(collect_list('ts_power').alias('ts_power_list'))
    udf_on_time = udf(on_time,ontime_schema)
    df = df_ts_list.select("user_id", "app_type_id", udf_on_time(df_ts_list.user_id,df_ts_list.app_type_id,"ts_power_list",'ymd').alias("oneTag"))
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
    table_name="DailySummary_{}".format(timestamp)
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

    '''Pass the date on which the execution of Daily Summary was Missed/Failed through commandline in the date format MMDDYYYY'''
    import pytz
    jst = pytz.timezone('Asia/Tokyo')
    now = ""
    if len(sys.argv) == 2:
      now = datetime.strptime(sys.argv[1]+' 00:00:00', '%m%d%Y %H:%M:%S')
    else:
      ts = time.time()
      now = datetime.fromtimestamp(ts, jst)
    print("now time:- {}".format(now))
    import calendar
    now = now.replace(minute=0, hour=0, second=0, microsecond=0)  #jst 0th hour of today
    yesterday_0_jst = now + timedelta(days=-1, hours=-9)  #jst 0 in utc of yesterday
    yesterday_24_jst = yesterday_0_jst + timedelta(hours=24, seconds=-1)

    jst_0_utc = yesterday_24_jst + timedelta(seconds=1)
    ymd = calendar.timegm(jst_0_utc.timetuple())                           #JST_0_UTC to epoch ts

    part_st = datetime.strftime( yesterday_0_jst,'%Y%m%d')
    part_end = datetime.strftime(yesterday_24_jst,'%Y%m%d')
    yesterday_0_jst = datetime.strftime(yesterday_0_jst,'%Y-%m-%d %H:%M:%S')
    yesterday_24_jst = datetime.strftime(yesterday_24_jst,'%Y-%m-%d %H:%M:%S')

    print('part_st- {}'.format(part_st))
    print('part_end- {}'.format(part_end))
    print('yesterday_0_jst- {}'.format(yesterday_0_jst))
    print('yesterday_24_jst- {}'.format(yesterday_24_jst))

    query = "SELECT user_id, data.timestamp as timestamp, data.app_types.app_type_id as app_type_id, data.app_types.apps.power as power \
             FROM "+db_tbl+" \
             WHERE _PARTITIONTIME BETWEEN TIMESTAMP('"+part_st+"') AND TIMESTAMP('"+part_end+"') \
             AND data.timestamp BETWEEN TIMESTAMP('"+yesterday_0_jst+"') AND TIMESTAMP('"+yesterday_24_jst+"')" 
    
    print('bigquery for intermediate table:- {}'.format(query))
    JobName = 'DailySummary_{}'.format(timestamp)
    print('Bigquery jobname:- {}'.format(JobName))
    qj = job.QueryJob(JobName, query, bq)
    qj.write_disposition="WRITE_TRUNCATE"
    qj.use_legacy_sql = True
    qj.destination = table
    qj.begin()
    wait_for_job(qj)
    create_table(ymd)

    #--------------------------- Spark Job
    spark = SparkSession \
        .builder.master("local[*]") \
        .appName("DailySummary") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('DailySummary script logger initialized at {}'.format(time.time())) 

    spark.sparkContext.addPyFile("./settings.py")
    spark.sparkContext.addPyFile("./BigTable.py")

    LOGGER.info('Intermediate table has been created successfully & Now Spark session has started.......') 
    bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
    project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")
    ''' Set an input directory for reading data from Bigquery.  '''
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
    LOGGER.info("=====DailySummary Completed succesfully.=======")
    time4 = time.time()
    LOGGER.info("Total sec's to run a job using BBQ source for all users taken {} seconds.".format(time4-time1))
    spark.stop()
