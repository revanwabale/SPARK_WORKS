from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkFiles, SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import SparkSession
import argparse
from datetime import datetime, timedelta
import time
import os
import sys




user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("app_type_id", StringType(), True)
])


	
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


def wd_utilization(user_id,app_type_id, ymd):
    '''
    ---weekday [wDay] over last 8 weeks calculating hour wise and Inserting into BigTable of numpy array.
    ---Day of the week as an integer, where Monday is 0 and Sunday is 6. 
    Result : Data Type of numpy array values either 'Integer' or 'nan'.
    Return:
       BigTable insertion responses of weekday and  numpy array.
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
    day = 86400    
    week = day*7
    
    ymd = int(ymd)

    h_tag = np.zeros(24) 			#24hour
    aDay = np.zeros(24)				#24hour
    tuples_list = ()
    user_count =[]
    hours = []
    hour = 3600
    table_name = settings.tablename
    n_weeks = settings.n_weeks
    for i in range(n_weeks):					#generate h_tag
        idate= ymd - (week * i)
	yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(idate))
        table_id = '{}{}'.format(table_name,yyyymm)
        cal_OneTag= readCell(table_id,user_id, app_type_id, idate,'OneTag','Otag')
 	if(cal_OneTag != None):
            one_tag = msgpack.unpackb(cal_OneTag)
            start_times = zip(*one_tag)[0]
 	    tuples_list +=(start_times)
    for index,start_time in enumerate(tuples_list):
        st_time_jst = int(start_time) + (9 * hour)
        st_time_hour = int("{:%H}".format(datetime.utcfromtimestamp(st_time_jst)))
        hours.append(st_time_hour)
    
    nphours = np.array(hours)  
    for h in nphours:
        aDay[h] = 1 
    
    h_tag = h_tag + aDay
    
    ut_pct = ''
    st_return =''
    ut_pct = h_tag / n_weeks
    yyyymm= "{:%Y%m}".format(datetime.utcfromtimestamp(ymd)) 
    table_id = '{}{}'.format(table_name,yyyymm)
    idate = ymd
    date= "{:%Y-%m-%d}".format(datetime.utcfromtimestamp(ymd))
    weekday_res = datetime.strptime(date,"%Y-%m-%d").weekday()
    weekday_bt =insertCell(table_id,user_id, app_type_id,idate, 'Daily', 'wDay',weekday_res)
    ymd ="{:%Y%m%d}".format(datetime.utcfromtimestamp(ymd))
    st_return = "{} {} [".format(ymd,weekday_bt)
    for i in range(24):
        if(i <10):
            column = "0{}".format(i)
            count=insertCell(table_id,user_id, app_type_id,idate, 'Daily', column, str(ut_pct[i]))
            st_return = st_return + str(count) + ","
        else:
	    column = "{}".format(i)
            count=insertCell(table_id,user_id, app_type_id, idate, 'Daily', column, str(ut_pct[i]))
   	    st_return = st_return+str(count) +","
    
    st_return = st_return[:-1]
    st_return = st_return + "]"
    return(st_return)




def main(spark,df_users):
    udf_wd_utilization =udf(wd_utilization,StringType())
    df_wdUtilization = df_users.select("user_id", "app_type_id",udf_wd_utilization(df_users.user_id,df_users.app_type_id, df_users.ymd).alias("WDutilization"))
    df_wdUtilization.show(100,False)

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
        .appName("WDUtilization") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('WDUtilization script logger initialized at {}'.format(time.time()))

    spark.sparkContext.addPyFile("./settings.py")
    spark.sparkContext.addPyFile("./BigTable.py")

    import settings
    reload(settings)

    '''  Get list of all users from IMDB  '''
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
    LOGGER.info('eligible_sid:- {}'.format(eligible_sid))
    eligible_users = all_users.filter(col('ser_id').isin(eligible_sid))

    udf_getAppTypeIds = udf(getApptypeIds, ArrayType(user_schema, containsNull=True))
    new_df_users = eligible_users.withColumn("record", udf_getAppTypeIds("user_id"))
    df_users = new_df_users.select(explode("record").alias("apptype_id_user_rec"))
    users_df = df_users.select("apptype_id_user_rec.user_id", "apptype_id_user_rec.app_type_id")
    users_df = users_df.withColumn('ymd', lit(ymd))
    users_df.persist()
    main(spark,users_df)
 
    LOGGER.info("=====WDUtilization Completed succesfully.=======")
    time4 = time.time()
    LOGGER.info("Total time taken by job:- {}".format(time4-time1))
    spark.stop()


