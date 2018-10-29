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

'''
udf schema to return est dataframe
'''
est_schema = StructType([
    StructField("xxx", StringType(), False),
    StructField("xxx", StringType(), False),
    StructField("xxx", StringType(), False),
    StructField("xxx", FloatType(), False),
    StructField("st", IntegerType(), False)
])

ON_schema = StructType([
    StructField("ymd", StringType(), False),
    StructField("active", StringType(), False)
])


concat_schema=  StructType([
    StructField("ts", StringType(), False),
    StructField("xxx",StringType(),False)    
])


def on_time(st,xxx,xxx,ts_xxx):
    '''
    Calculating last activity
    Return:
       BigTable Insertion responces of last activity
    '''
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
    threshold_minxxx = (settings.thresholds[str(xxx)][0])
    xxx_list =[]
    day = 86400    
    counter = None
    one_tag_et=None
    ts=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(st))
    dt_ts = datetime.strptime(ts,"%Y-%m-%d %H:%M:%S")
    jst_0 = dt_ts.replace(minute=0, hour=0, second=0, microsecond=0)
    jst_0_utc = jst_0 + timedelta(hours=-9)
    ymd = calendar.timegm(jst_0_utc.timetuple())

    if len(ts_xxx) == 0:
        counter = None
        one_tag_et=None
    else:
        for i in range(len(ts_xxx)):
            xxx = ts_xxx[i][1]
            if xxx > threshold_minxxx:			# Filtering eligible xxxs.
                xxx = int(ts_xxx[i][0])
                xxx_list.append(xxx)
        ts = np.array(xxx_list)
        if ts.size == 0:                                                        
            counter = None
	    one_tag_et=None
        else:
            ts.sort()

    table_name = settings.tablename
    yyyymm = "{:%Y%m}".format(datetime.utcfromxxx(ymd))
    table_id = '{}{}'.format(table_name,yyyymm)

    if(len(ts)>0):
         one_tag_et=insertCell(table_id,xxx, xxx, ymd, 'Daily', 'la', ts[-1])	#adding last endtime as latest activity
    else:
  	 yesterday_ymd = ymd - day
	 yyyymm = "{:%Y%m}".format(datetime.utcfromxxx(yesterday_ymd))
	 table_id = '{}{}'.format(table_name,yyyymm)
         ystrday_et=readCell(table_id,xxx, xxx, yesterday_ymd,'Daily','la')
	 if(ystrday_et != None):							#yesterday's last activity
	    yyyymm = "{:%Y%m}".format(datetime.utcfromxxx(ymd))
	    table_id = '{}{}'.format(table_name,yyyymm)
            one_tag_et=insertCell(table_id,xxx, xxx, ymd, 'Daily', 'la', ystrday_et) 
         else:
            one_tag_et = 0								#default one-tag is zero
	    yyyymm = "{:%Y%m}".format(datetime.utcfromxxx(ymd))
            table_id = '{}{}'.format(table_name,yyyymm)
            one_tag_et=insertCell(table_id,xxx, xxx, ymd, 'Daily', 'la', one_tag_et) 
 
    return(ymd,one_tag_et)             
         

def get_estUserList(user, serProvId, st):
    ''' Getting IMGATE API data in json responce for each user-id with all xxx's 
    Args:
      xxx, serviceProviderId,start-time
    Return:
      data in json responce in list of list.
    '''

    import settings
    reload(settings)
    import requests
    import subprocess
    import time

    header = "imSP "+str(serProvId)+":"+str(settings.servicePrPass[serProvId])
    imgateest_url = settings.imgateest_url
    param={'customer':user, 'sts':int(st), 'ets':(int(st)+599), 'time_units':settings.time_units}
    res = requests.get(imgateest_url, params= param, headers={'Authorization': header})
    user_rec = []
    json_data=res.json()

    if (json_data['fixed_at'] != None):
           apptypelist = [json_data['data'][0]['appliance_types'][x]['appliance_type_id']
                    for x in range(len(json_data['data'][0]['appliance_types']))]
           for appid in apptypelist:
              appidx = apptypelist.index(appid)
              xxxs = (json_data['data'][0]['appliance_types'][appidx]['appliances'][0]['xxxs'])
              appliance_id = (json_data['data'][0]['appliance_types'][appidx]['appliances'][0]['appliance_id'])
              ts = (json_data['data'][0]['xxxs'])
              i=0
              while i in range(len(ts)):
                 if((json_data['fixed_at'] >= int(st)) and (ts[i] <= json_data['fixed_at']) and xxxs[i]>0):
                    rec = [user, appid, ts[i], xxxs[i],int(st)]
                    user_rec.append(rec)
                 i=i+1
    else:
          return (None, None, None, None,None)

    return (user_rec)



def create_table(ymd):
    '''
    Creating Table, ColumnFamilies if not exists for that month.
    args:
        ymd - unix epoch xxx
     
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
    yyyymm = "{:%Y%m}".format(datetime.utcfromxxx(ymd+599))
    table_id = '{}{}'.format(tablename,yyyymm)
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table_id = instance.table(table_id)
    if not table_id in instance.list_tables():		#creating table if it not present
            table_id.create()
    
    for column_family_id in column_families_list:
        cf = column_family_id
        if not cf in table_id.list_column_families():  	#creating columnfamily if it is not present in that table.
                cf1 = table_id.column_family(cf)
                cf1.create()


def main(spark,df_users):
    import settings
    reload(settings)
    udf_users=udf(get_estUserList, ArrayType(est_schema, containsNull=True))
    df_users_records = df_users.withColumn("user_record", udf_users("xxx", "ser_id", "st"))
    df = df_users_records.select(explode("user_record").alias("user_record"))
    df = df.select("user_record.xxx", "user_record.xxx", "user_record.xxx", "user_record.xxx","user_record.st")
    udf_on_time=udf(on_time,ON_schema)
    eligible_xxx = settings.thresholds.keys()
    df_filterapp=df.filter(col('xxx').isin(eligible_xxx))  
    concatUdf = udf(lambda xxx,xxx:(xxx,xxx),concat_schema)
    df_concat = df_filterapp.select("xxx","xxx","st",concatUdf(df_filterapp.xxx,df_filterapp.xxx).alias("ts_xxx"))
    df_g = df_concat.groupBy('xxx','xxx','st')
    df_ts_list = df_g.agg(collect_list('ts_xxx').alias('ts_xxx_list'))
    df = df_ts_list.select("xxx", "xxx", udf_on_time (df_ts_list.st,df_ts_list.xxx,df_ts_list.xxx,"ts_xxx_list").alias("Activity"))
    activty_df = df.select("xxx", "xxx","Activity.ymd", "Activity.active")
    activty_df.show(20,False)
    

if __name__ == "__main__":
    import time
    st = int(time.time()) - (4*3600)  #start time 4 hours before as 3 hours required to get fixed_at records
    print st

    warehouse_location = 'file:${system:user.dir}/spark-warehouse'
    spark = SparkSession \
    .builder.master("yarn") \
    .appName("ActivityChecker") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

    time1 = time.time()
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('ActivityChecker script logger initialized at {}'.format(time.time())) 
    spark.sparkContext.addPyFile("./settings.py") 
    spark.sparkContext.addPyFile("./BigTable.py") 

    import settings
    reload(settings)
    
    create_table(st)

    '''  Get list of all users from IMDB   '''
    jdbcDF = spark.read.format("jdbc").option("url", settings.imdb_url) \
    .option("dbtable", settings.dbtable) \
    .option("user", settings.user) \
    .option("password", settings.password) \
    .option("column", settings.column) \
    .option("numPartitions", settings.numPartitions) \
    .option("is_active", "true").load()

    jdbcDF = jdbcDF.filter(jdbcDF.is_active == 1) 
    LOGGER.info('Active number of users from imgatedb is : {}'.format(jdbcDF.count()))

    all_users = jdbcDF.select(regexp_extract('user', '(\d+)_([a-zA-Z0-9]+)', 1).alias('ser_id'), (jdbcDF.user).alias("xxx"))
    ser_id = settings.servicePrPass
    eligible_sid = ser_id.keys()
    LOGGER.info('eligible_sid:- {}'.format(eligible_sid))
    eligible_users = all_users.filter(col('ser_id').isin(eligible_sid))
    all_users = eligible_users.withColumn('st', lit(st))
    main(spark,all_users)
    LOGGER.info("=====ActivityChecker Completed succesfully.=======")
    time2 = time.time()
    LOGGER.info("Total sec's to run a job using IMGATE API source for all users taken {} seconds.".format(time2-time1))
    spark.stop()
    
