from pyspark.sql import SparkSession
from pyspark import SparkFiles, SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import argparse
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import time
import os
import sys

weekly_schema = StructType([
    StructField("ymd", StringType(), False),
    StructField("bt_wcnt", StringType(), False),
    StructField("bt_acnt", StringType(), False),
    StructField("bt_wdur", StringType(), False),
    StructField("bt_adur", StringType(), False),
    StructField("wow_cnt", StringType(), False),
    StructField("wow_dur", StringType(), False),
    StructField("ma4_count", StringType(), False),
    StructField("ma4_duration", StringType(), False),
    StructField("unusual_h_cnt", StringType(), False),
    StructField("unusual_l_cnt", StringType(), False),
    StructField("unusual_h_dur", StringType(), False),
    StructField("unusual_l_dur", StringType(), False)

])

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("app_type_id", StringType(), True)
])


def Weekly(user_id,app_type_id,ymd):
    '''
    Calculating    
          wcnt,acnt,wdur,adur,wow_cnt,wow_dur,ma4_count,ma4_duration,unusual_h_cnt,unusual_l_cnt,unusual_h_dur,unusual_l_dur
    Return:
          BigTable Insertion responces of 
     wcnt,acnt,wdur,adur,wow_cnt,wow_dur,ma4_count,ma4_duration,unusual_h_cnt,unusual_l_cnt,unusual_h_dur,unusual_l_dur
    '''

    import settings
    reload(settings)
    import os,sys
    import numpy as np
    import time
    from datetime import timedelta,datetime,date
    from BigTable import insertCell
    from BigTable import readCell
    import calendar
    day = 86400
    week = day*7

    ymd = int(ymd)
    user_record =[]
    cal_count =0
    cal_duration =0
    table_name = settings.tablename
    today_ymd = ymd- (day*0)
    lastweek_ymd= today_ymd- (week *1)
    delta = today_ymd -lastweek_ymd
    delta_days = delta/day
    for i in range(1,delta_days+1):
        idate= lastweek_ymd + (day * i)
	yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(idate))
        table_id = '{}{}'.format(table_name,yyyymm)
        cal_count = readCell(table_id,user_id, app_type_id, idate,'Daily','cnt')
        cal_duration = readCell(table_id,user_id,app_type_id, idate,'Daily','dur')
        if(cal_count != None and cal_duration !=None):
            user_record.append((cal_count,cal_duration))

    week_days= np.array(user_record).astype(np.int) 
    bt_wcnt =bt_acnt=bt_wdur=bt_adur=0   

    if(len(week_days)!=0):
        counts = week_days[:,0]
        durations = week_days[:,1]
        w_count = counts.sum(0)
        num_days = len([i for i in counts if i > 0])
	ave_count = 0 if w_count == 0 else w_count / num_days
        w_duration = durations.sum()
        ave_duration = 0 if w_duration == 0 else (w_duration / num_days)
   	idate = ymd
 	yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(idate))
        table_id = '{}{}'.format(table_name,yyyymm)
        bt_wcnt=insertCell(table_id,user_id,app_type_id, idate, 'Weekly', 'wCnt', w_count)
        bt_acnt=insertCell(table_id,user_id,app_type_id, idate, 'Weekly', 'aCnt',ave_count)
        bt_wdur=insertCell(table_id,user_id,app_type_id, idate, 'Weekly', 'wDur', w_duration)
        bt_adur=insertCell(table_id,user_id,app_type_id, idate, 'Weekly', 'aDur', ave_duration)
    
    if(bt_wcnt != 0 and bt_acnt !=0 and bt_wdur != 0 and bt_adur !=0):  #BigTable Unsuccesful Insertion i.e if at least anyone is failed to insert into BigTable.
        user_record =[]
        cal_count =0
        cal_duration =0
	today_ymd = ymd - (day * 0 )
	lastweek_ymd = today_ymd- (week *4)
        for i in range(1,5):
            idate= lastweek_ymd + (week * i)
	    yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(idate))
            table_id = '{}{}'.format(table_name,yyyymm)
            cal_count = readCell(table_id,user_id, app_type_id, idate,'Weekly','wCnt')
            cal_duration = readCell(table_id,user_id, app_type_id, idate,'Weekly','wDur')
            if(cal_count != None and cal_duration !=None):
                user_record.append((cal_count,cal_duration))
	
        weeks_data = np.array(user_record).astype(np.int) 
        wow_cnt=0
        wow_dur =0
        ma4_count =0
        ma4_duration =0
        if len(weeks_data) == 0:
            wow_cnt = int(0)
            wow_dur = int(0)
        else: 
            wow_cnt = w_count - int(weeks_data[-1,0])		# Current week minus previous week
            wow_dur = w_duration - int(weeks_data[-1,1])	# Current week minus previous week
        if len(weeks_data) == settings.window_size: 
            weeks_count = weeks_data[:,0]
            weeks_duration = weeks_data[:,1]        
            ma4_count = float(np.ma.average(weeks_count))
            ma4_duration = float(np.ma.average(weeks_duration))
        else:
            ma4_count = float(0)
            ma4_duration = float(0)

        unusual_h_cnt = False
        unusual_l_cnt = False       
        unusual_h_dur = False
        unusual_l_dur = False       
        count_buffer = 1.0
        duration_buffer = 210.0 				# 30min per day * 7 days = 210min
        if(ma4_count != 0): 					# Unusual count?
            large_top_count = (ma4_count + count_buffer) * settings.param_th_top
            small_bottom_count = (ma4_count - count_buffer) * settings.param_th_bottom

            if(w_count > large_top_count):
                unusual_h_cnt = True
            elif w_count < small_bottom_count:
                unusual_l_cnt = True      

            large_top_duration = (ma4_duration + duration_buffer) * settings.param_th_top	# Unusual duration?
            small_bottom_duration = (ma4_duration - duration_buffer) * settings.param_th_bottom
            if(w_duration > large_top_duration):
                unusual_h_dur = True
            elif w_duration < small_bottom_duration:
                unusual_l_dur = True 
        yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(ymd))
	idate = ymd
        table_id = '{}{}'.format(table_name,yyyymm)
        wow_cnt=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'wowCnt',wow_cnt)
        wow_dur=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'wowDur',wow_dur)
        ma4_count=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'ma4Cnt', ma4_count)
        ma4_duration=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'ma4Dur', ma4_duration)
        unusual_h_cnt=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'uHcnt', unusual_h_cnt)
        unusual_l_cnt=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'uLcnt',unusual_l_cnt)
        unusual_h_dur=insertCell(table_id,user_id,app_type_id, idate, 'Weekly',  'uHdur', unusual_h_dur)
        unusual_l_dur=insertCell(table_id,user_id, app_type_id, idate, 'Weekly', 'uLdur', unusual_l_dur)
    else:
        """ Not Eligible For BigTableInsertion """
        bt_wcnt=bt_acnt=bt_wdur=bt_adur=wow_cnt=wow_dur=ma4_count=ma4_duration=unusual_h_cnt=unusual_l_cnt=unusual_h_dur=unusual_l_dur =-1
    ymd = "{:%Y%m%d}".format(datetime.utcfromtimestamp(ymd))    
    return(ymd,bt_wcnt, bt_acnt,bt_wdur,bt_adur,wow_cnt,wow_dur,ma4_count,ma4_duration,unusual_h_cnt,unusual_l_cnt,unusual_h_dur,unusual_l_dur) 


def main(spark,users_df):   
    LOGGER.info("=====WeeklySummary Started.=======")
    udf_Weekly = udf(Weekly,weekly_schema)
    df_Weekly = users_df.select("user_id", "app_type_id", udf_Weekly(users_df.user_id,users_df.app_type_id, users_df.ymd).alias("Weekly"))
    df_Weekly.show(100,False)



def create_table(ymd):
    '''
    Creating Table and ColumnFamilies.
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
    ymd = int(ymd)
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




def getApptypeIds(user_id):
    '''
       Creating list of all eligible app_type_id's for user
    Return:
       list of list containing user_id and app_type_id in every row of DF. 
    '''
    import settings
    reload(settings)
    app_type_id_list = settings.thresholds.keys()
    rec =[]
    for app_type_id in app_type_id_list:
        s_rec = [user_id, app_type_id]
        rec.append(s_rec)
    return rec



if __name__ == "__main__":
    import time
    warehouse_location = 'file:${system:user.dir}/spark-warehouse'
    from datetime import datetime

    time1 = time.time()
    '''
    start time caluclation for script
    '''
    import pytz
    jst = pytz.timezone('Asia/Tokyo')
    now = ""
    if len(sys.argv) == 2:
      now = datetime.strptime(sys.argv[1]+' 00:00:00', '%m%d%Y %H:%M:%S')
    else:
      ts = time.time()
      now = datetime.fromtimestamp(ts, jst)

    print('jst equivalent date in UTC for passed/currentTime value:- {}'.format(now))
    import calendar
    jst_0 = now.replace(minute=0, hour=0, second=0, microsecond=0)            #jst 0th hour of today
    jst_0_utc = jst_0 + timedelta(hours=-9)                                   #jst 0 in utc of today
    ymd = calendar.timegm(jst_0_utc.timetuple())                              #JST_0_UTC to epoch ts

    create_table(ymd)
    #--------------------------- Spark Job
    spark = SparkSession \
        .builder.master("yarn") \
        .appName("WeeklySummary") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('WeeklySummary script logger initialized at {}'.format(time.time())) 

    spark.sparkContext.addPyFile("./settings.py")
    spark.sparkContext.addPyFile("./BigTable.py")

    import settings
    reload(settings)
    
    '''-----Get list of all users from IMDB----'''
    jdbcDF = spark.read.format("jdbc").option("url", settings.imdb_url) \
    .option("dbtable", settings.dbtable) \
    .option("user", settings.user) \
    .option("password", settings.password) \
    .option("column", settings.column) \
    .option("numPartitions", settings.numPartitions) \
    .option("is_active", "true").load()

    jdbcDF = jdbcDF.filter(jdbcDF.is_active == 1) 
    LOGGER.info('Active number of users from imgatedb is : {}'.format(jdbcDF.count()))

    all_users = jdbcDF.select(regexp_extract('user', '(\d+)_([a-zA-Z0-9]+)', 1).alias('ser_id'), (jdbcDF.user).alias("user_id"))
    ser_id = settings.servicePrPass
    eligible_sid = ser_id.keys()
    print('eligible_sid:- {}'.format(eligible_sid))
    eligible_users = all_users.filter(col('ser_id').isin(eligible_sid))

    udf_getAppTypeIds = udf(getApptypeIds, ArrayType(user_schema, containsNull=True)) 
    new_df_users = eligible_users.withColumn("record", udf_getAppTypeIds("user_id"))
    df_users = new_df_users.select(explode("record").alias("apptype_id_user_rec"))
    users_df = df_users.select("apptype_id_user_rec.user_id", "apptype_id_user_rec.app_type_id")
    users_df = users_df.withColumn('ymd', lit(ymd))
    users_df.persist()
    
    main(spark,users_df)

    LOGGER.info("=====WeeklySummary Completed succesfully.=======")
    time2 = time.time()
    LOGGER.info("Total sec's run a job using BBQ source for all users taken {} seconds.".format(time2-time1))   
    spark.stop()
