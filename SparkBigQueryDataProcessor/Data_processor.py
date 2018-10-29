from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime,timedelta
import json
import sys
import time
import threading

'''
udf schema to return user_st_et_schema dataframe
'''
user_st_et_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("ser_id", StringType(), False),
    StructField("st", IntegerType(), False),
    StructField("et", IntegerType(), False)
])

'''
udf schema to return est dataframe
'''
est_xxx_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("commited_at", IntegerType(), False),
    StructField("partition", StringType(), False),
    StructField("xxx_json", StringType(), False),
    StructField("xxx_json", StringType(), False)
])


'''
UDF to get user list with new st and et

Input:
user - userid
pro_id - service provider id for this user
st - start timestamp

Output:
userid, ser_id, st, et
'''
def get_User_ST_ET(user, pro_id, st):
   user_rec = []
   tmp_st = int(float(st))
   et=int(time.time())+32400

   while((et-tmp_st) > 0):
      if((et-tmp_st) >= 86340):
	  rec = [user, pro_id, tmp_st, tmp_st+86339]
          user_rec.append(rec)
          tmp_st += 86340
      else:
          rec = [user, pro_id, tmp_st, et]
          user_rec.append(rec)
          break

   return (user_rec)


'''
UDF To execute imgate API for each service providers user list

Input:
user - userid
pro_id - service provider id for this user
st - start timestamp
et - end timestamp

Output:
userid, app_type_id, timestamp, pwr, commited_at, app_id, xxx, partition  
'''
def get_estUserList(user, pro_id, st, et):

    import settings
    reload(settings)
    import requests
    import subprocess

    header = "xxx "+str(pro_id)+":"+str(settings.servicePrPass[pro_id])
    API_URL = settings.API_URL

    param={'customer':user, 'sts':int(st), 'ets':int(et), 'time_units':int(settings.time_units)}
    res = requests.get(API_URL, params= param, headers={'Authorization': header})

    #if(res.json() is None):
       #return (None, None, None, None, None, None)
  
    from datetime import datetime,timedelta 
    from json import JSONDecoder
    from objdict import ObjDict
    from collections import OrderedDict

    pyDictionary = JSONDecoder().decode(res.text)
    json_data = res.json()
    commited_at = json_data['commited_at']
    if (json_data['commited_at'] != None):
      part = datetime.fromtimestamp(json_data['data'][0]['timestamps'][0]).strftime('%Y%m%d')
      part_list = []

      for j in range(len(json_data['data'][0]['timestamps'])):
        if(part != datetime.fromtimestamp(json_data['data'][0]['timestamps'][j]).strftime('%Y%m%d')):
          part = datetime.fromtimestamp(json_data['data'][0]['timestamps'][j]).strftime('%Y%m%d')
          part_list.append(j)

      part_list.append(len(json_data['data'][0]['timestamps']))

      rec_arr = []
      est_json = ObjDict()
      est_json.user_id = json_data['customer']
      xxx_json = ObjDict()
      xxx_json.user_id = json_data['customer']

      t_et = 0
      for t in part_list:
        t_st = t_et
        t_et = t
        ts_est_arr = []
        ts_xxx_arr = []
        for i in range(t_st, t_et):
            apptypes_arr = []
            appidpow_arr = []
            if(json_data['data'][0]['timestamps'][i] <= json_data['commited_at'] and json_data['data'][0]['timestamps'][i] > int(st)):
               for x in range(len(json_data['data'][0]['app_type'])):
	          app_type_id = json_data['data'][0]['app_type'][x]['app_id']
	          app_id = json_data['data'][0]['app_type'][x]['apps'][0]['id']
	          appidpow_arr = []
                  appidx = settings.app_type_list.index(json_data['data'][0]['app_type'][x]['app_id'])
                  pwrs = (json_data['data'][0]['app_type'][x]['apps'][0]['pwrs'])
                  if(pwrs[i] > 0 or pwrs[i] is None):
	             appidpow_arr.append({"app_id":app_id,"pwr":pwrs[i]})
                     apptypes_arr.append({"app_type_id":app_type_id,"apps":appidpow_arr})

               #end of for loop on appliance types
               if(len(apptypes_arr)>0):
                   ts_est_arr.append(OrderedDict({"timestamp":json_data['data'][0]['timestamps'][i], "app_types":apptypes_arr}))
               	   try:
                  	if(i<len(json_data['data'][0]['main_pwrs'])):
                      		root_pw = json_data['data'][0]['main_pwrs'][i]
	              		#below if condition to add ts and pwr for xxx record
                      		if(root_pw > 0 or root_pw is None):
                         		ts_xxx_arr.append(OrderedDict({"timestamp":json_data['data'][0]['timestamps'][i], "xxx":root_pw}))
                   except:
                        print("main_pwrs missing") 

        #end of for loop on i equal to ts range
        partition = datetime.fromtimestamp(json_data['data'][0]['timestamps'][t_et-1]).strftime('%Y%m%d')
        if(len(ts_est_arr)>0  and len(ts_xxx_arr)>0): 
          est_json.data = ts_est_arr
          xxx_json.data = ts_xxx_arr
          rec = [json_data['customer'],json_data['commited_at'],partition,ObjDict.dumps(est_json),ObjDict.dumps(xxx_json)]
          rec_arr.append(rec)
        elif(len(ts_est_arr)>0 and len(ts_xxx_arr)==0):
          est_json.data = ts_est_arr
          rec = [json_data['customer'],json_data['commited_at'],partition,ObjDict.dumps(est_json),None]
          rec_arr.append(rec)
        elif(len(ts_est_arr)==0 and len(ts_xxx_arr)>0):
          xxx_json.data = ts_xxx_arr
          rec = [json_data['customer'],json_data['commited_at'],partition,None,ObjDict.dumps(xxx_json)]
          rec_arr.append(rec)

      #end of for loop for t i.e. number of partitions in part_list
      if(len(rec_arr)>0):
        return (rec_arr)
      else:
        return (None, None, None, None, None)
   
    #end of if block for commited_at  
    else:
      return (None, None, None, None, None)


'''
@load_into_bbq method to load data to bbq table
'''
def load_into_bbq(src_format, projectid, dataset, table, partition, f_path, schema):
    import subprocess
    try:
       subprocess.check_call('bq load --source_format={src_format} --schema=\'{schema}\' \'{projectid}:{dataset}.{table}${part}\' {files} '.format(src_format=src_format, schema=schema, projectid=projectid, dataset=dataset, table=table, part=partition, files=f_path),shell=True)
    except:
       LOGGER.info('Failed Upload Job: bq load --source_format={src_format} --schema=\'{schema}\' \'{projectid}:{dataset}.{table}${part}\' {files} '.format(src_format=src_format, schema=schema, projectid=projectid, dataset=dataset, table=table, part=partition, files=f_path),shell=True)
       subprocess.check_call('bq load --source_format={src_format} --schema=\'{schema}\' \'{projectid}:{dataset}.{table}${part}\' {files} '.format(src_format=src_format, schema=schema, projectid=projectid, dataset=dataset, table=table, part=partition, files=f_path),shell=True)
       

'''
@main method to create spark session, fetch user_db user list, persist list in memory for each thread,
Execute each thread named : est & xxx for respective service providers which will upload data to their BigQuery project.
'''
if __name__ == "__main__":

  import os.path
  #file_path = "/home/revan/QC/SPARK_PROCESSOR/.RUNNING"    #TODO:In Production set this to static path
  file_path = "./.RUNNING"
  try:
    if (os.path.exists(file_path)):
       print("Please check a spark job for same script may be already running!")
       exit()
    
    open(file_path, 'w+').close()
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = 'file:${system:user.dir}/spark-warehouse'

    spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName("SPARK_PROCESSOR") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info('SPARK_PROCESSOR script logger initialized at {}'.format(time.time()))
    LOGGER.info('Number of Input Arguments: {}'.format(len(sys.argv)))
    
    spark.sparkContext.addPyFile('./settings.py')
   
    import settings
    reload(settings)

    #Get list of all users from IMDB
    jdbcDF = spark.read.format("jdbc").option("url", settings.imdb_url) \
    .option("dbtable", settings.dbtable) \
    .option("user", settings.user) \
    .option("password", settings.password) \
    .option("column", settings.column) \
    .option("numPartitions", settings.numPartitions) \
    .option("is_active", "true").load()

    jdbcDF = jdbcDF.filter(jdbcDF.is_active == 1) 
    LOGGER.info('Active number of users from user_db is : {}'.format(jdbcDF.count()))

    #all_users = jdbcDF.select(regexp_extract('user', '(\d+)_(\d+)', 1).alias('ser_id'), (jdbcDF.user).alias("user_id"))
    all_users = jdbcDF.select(regexp_extract('user', '(\d+)_([a-zA-Z0-9]+)', 1).alias('ser_id'), (jdbcDF.user).alias("user_id"))
    all_users = all_users.where((col('ser_id').isin(settings.servicePrPass.keys())))
    #LOGGER.info('Active number of users from user_db after ser_id filter is : {}'.format(all_users.count()))

    st = (int(time.time())+32400) -  3600
    hive_partition = int((datetime.now() + timedelta(days=0)).strftime('%Y%m%d%H'))
    LOGGER.info('Hive partition : {}'.format(hive_partition))
    first_run = False

    if len(sys.argv) > 2:
        print("Usage: spark-submit umb_test.py is_first_run[True|False]")
        exit(-1)
    elif (len(sys.argv) == 2 and bool(sys.argv[1]) == True):
        LOGGER.info('First run of SPARK_PROCESSOR')
        first_run = True

	#Set Start Time (st)
        st = (int(time.time())+32400) -  settings.days_3_secs                #(259200)   #value of -3 days in seconds should come from settings

	'''
	#Find old users by reading the hive partition    (user,commited_at)
	#Take join of the users from hive partition with the user_dbusers hive-->(user,commited_at) -----> user_db_allusers(ser_id,user)    ====>  (ser_id,userid,st=commited_at,et=now)
	#Find new users by subtracting user_db_allusers - old_users  (all_users, old_users)  all_users--->user_db(ser_id,user)     old_users--->hive(user,commited_at)
	#For new users Add Columns 'st' and 'et' to DataFrame as below
	#Then take union of new_users and old_users to get union_users (ser_id,user,st,et)
        ''' 
	from pyspark.sql.functions import lit
	from pyspark.sql import SQLContext
	new_df_users = all_users.withColumn('st', lit(st))
	
        #Call udf to get st and et
        udf_new_st_et = udf(get_User_ST_ET, ArrayType(user_st_et_schema, containsNull=False)) 
        new_st_et_users = new_df_users.withColumn("user_stet_record", udf_new_st_et("user_id", "ser_id", "st"))
 
        ex_df = new_st_et_users.select(explode("user_stet_record").alias("user_st_et_record"))
        new_df_all_users = ex_df.select("user_st_et_record.user_id", "user_st_et_record.ser_id", "user_st_et_record.st", "user_st_et_record.et")
	
    else:
	LOGGER.info('For non first runs of SPARK_PROCESSOR')
        #Get Hive Partitions
        from pyspark.sql.functions import max
        from pyspark.sql.functions import first

        spark.sql("MSCK REPAIR TABLE "+settings.hive_db+"."+settings.hive_table)
        hive_partitions = spark.sql("SHOW PARTITIONS "+settings.hive_db+"."+settings.hive_table)
        latest_partition = hive_partitions.select(regexp_extract(max(hive_partitions.result),'(et_hr)=(\d+)',2).alias('part_hr'))
        LOGGER.info('Hive Partitions:- {}'.format(hive_partitions.collect()))
        LOGGER.info('Latest Hive Partition:- {}'.format(latest_partition.collect()))

	import pandas as pd
        part_df = latest_partition.toPandas()
	old_users = spark.read.csv(settings.hive_dir + str(part_df.iloc[0]['part_hr']))

	old_users_hive = old_users.select((old_users._c0).alias("user_id"), (old_users._c1).alias("st")).distinct()
        join_users = all_users.join(old_users_hive,"user_id","left_outer")

        #filter new users and set st
        new_users = join_users.filter(old_users_hive.st.isNull()) 
	new_users = new_users.select("user_id","ser_id")
	from pyspark.sql.functions import lit
	from pyspark.sql import SQLContext
        new_user_st = (int(time.time())+32400) -  settings.days_3_secs         #value of st=-3 days in seconds for new users
	new_users = new_users.withColumn('st', lit(new_user_st))
       
        #old users
	old_users = join_users.filter(old_users_hive.st.isNotNull()).select("user_id","ser_id","st")
        et = int(time.time())+32400
	df_union_users = old_users.unionAll(new_users)
        #filter such records where st > -24 hours
	fil_st_gt_users = df_union_users.filter((et - df_union_users.st) >= 86400)
        #filter records where st < -24 hours
        fil_st_lt_users = df_union_users.filter((et - df_union_users.st) < 86400).withColumn('et',lit(et))
 
	#if st - et > 24hours break into days for API call
        #Call udf to get st and et
        udf_new_st_et = udf(get_User_ST_ET, ArrayType(user_st_et_schema, containsNull=False)) 
        new_st_et_users = fil_st_gt_users.withColumn("user_stet_record", udf_new_st_et("user_id", "ser_id", "st"))
        ex_df = new_st_et_users.select(explode("user_stet_record").alias("user_st_et_record"))
        new_st_et_df = ex_df.select("user_st_et_record.user_id", "user_st_et_record.ser_id", "user_st_et_record.st", "user_st_et_record.et")
 
        new_df_all_users = new_st_et_df.unionAll(fil_st_lt_users)

    #UDF CALL for est user
    udf_users=udf(get_estUserList, ArrayType(est_xxx_schema, containsNull=True))
    df_users_records = new_df_all_users.withColumn("user_record", udf_users("user_id", "ser_id", "st", "et"))

    # filter fixed records commited_at with old commited_at records new_commited_at > old_commited_at
    # filter non zero and non null records; null records are for missing data from sensors
    # filter out not fixed records with condition ts < commited_at condition
    # Assumption, Once record fixed , prediction algorithm will never change the value
    df = df_users_records.select(explode("user_record").alias("user_record"),df_users_records.ser_id)
    df = df.select("user_record.user_id", "user_record.commited_at", "user_record.partition", "user_record.xxx_json", "user_record.xxx_json", "ser_id")
 
    LOGGER.info('Completed fetch of est and xxx data from imgate api')
    xxx_users = df.select("ser_id","partition","xxx_json")
    #Filter to remove zero usage records for est table
    df_whole_house_powr_TBL = xxx_users.where(col("xxx_json").isNotNull())
    xxx_output_directory = settings.output_dir+str(hive_partition)+'/'+settings.table_xxx
    df_whole_house_powr_TBL.write.partitionBy("ser_id","partition").text(xxx_output_directory)
    LOGGER.info('xxx user records written in hive partition dir {}'.format(xxx_output_directory))

    #Filter to remove zero usage records for est table
    df_est_TBL = df.where(col('xxx_json').isNotNull())
    df_final_est_TBL = df_est_TBL.select( "ser_id", "partition", "xxx_json")

    #partition the data based on ser_id and partition to upload data to respective partition in bigquery
    est_output_directory = settings.output_dir+str(hive_partition)+'/'+settings.table_est 
    df_final_est_TBL.write.partitionBy("ser_id","partition").text(est_output_directory)
    LOGGER.info('estd pwr user records written in hive partition dir {}'.format(est_output_directory))

    #Store user and commited_at into hive partition for next run
    df_tmp_tbl = (df_est_TBL.select("user_id", "commited_at")).distinct()
    try:
       if(df_tmp_tbl.head()):
          if(first_run is False):
             hive_part_join = df_tmp_tbl.join(old_users_hive,"user_id","right_outer")
             old_hive_part_users= hive_part_join.filter(df_tmp_tbl.user_id.isNull()).select("user_id","st")
             df_tmp_tbl = df_tmp_tbl.unionAll(old_hive_part_users)
             
          spark.sql("ALTER TABLE "+settings.hive_db+"."+settings.hive_table+" ADD PARTITION (et_hr="+str(hive_partition)+") location '"+settings.hive_dir+str(hive_partition)+"'")
          df_tmp_tbl = df_tmp_tbl.coalesce(1)
          df_tmp_tbl.registerTempTable("tempHiveTable")
          spark.sql("insert into table "+settings.hive_db+"."+settings.hive_table+" partition(et_hr="+str(hive_partition)+") from tempHiveTable")
          LOGGER.info('commited_at records written in hive table {}.{}  partition dir et_hr={}'.format(settings.hive_db,settings.hive_table,str(hive_partition)))
    except:
          if(first_run is True):
              LOGGER.info('No new commited_at records to be written in hive table {}.{}  partition dir et_hr={}'.format(settings.hive_db,settings.hive_table,str(hive_partition)))
          else:
              spark.sql("ALTER TABLE "+settings.hive_db+"."+settings.hive_table+" ADD PARTITION (et_hr="+str(hive_partition)+") location '"+settings.hive_dir+str(hive_partition)+"'")
              df_tmp_tbl = old_users_hive.coalesce(1)
              df_tmp_tbl.registerTempTable("tempHiveTable")
              spark.sql("Insert into table "+settings.hive_db+"."+settings.hive_table+" partition(et_hr="+str(hive_partition)+") from tempHiveTable")
              LOGGER.info('Copied commited_at records from previous partition in hive table {}.{} to partition dir et_hr={}'.format(settings.hive_db,settings.hive_table,str(hive_partition)))
          
            
    import os
    threads = [] 
    output_tables = settings.table_schema.keys()
    
    for table in output_tables:
		output_directory = settings.output_dir+str(hive_partition)+'/'+table
		output_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_directory)
                src_format = settings.src_format

		for fileStatus in output_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).listStatus(output_path):
   		   if(fileStatus.isFile()==False):
                      for status in output_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).listStatus(fileStatus.getPath()):
                      	h,t = os.path.split(fileStatus.getPath().toString())
           	      	h,serid = t.split("=")
                      	head,tail = os.path.split(status.getPath().toString())
           	      	head,partition = tail.split("=")
           	      	f_path = status.getPath().toString()+"/*"
                      	projectid = settings.projectid
           	      	dataset = settings.dataset_prefix+serid
           	      	schema = settings.table_schema[table]
		      	t_name='t_{}_{}_{}'.format(table,serid,partition)
                      	t_load_tbl = threading.Thread(name=t_name, target=load_into_bbq, args=(src_format, projectid, dataset, table, partition, f_path, schema))
                      	threads.append(t_load_tbl)
                      	t_load_tbl.start()
                      	LOGGER.info("Thread- {} started".format(t_name))

    for t in threads:
        t.join()
        LOGGER.info('t.join():- {}'.format(t))
    
    if os.path.exists(file_path):
        os.remove(file_path)
    LOGGER.info('SPARK_PROCESSOR pyspark script finished at {}'.format(time.time()))
    LOGGER.info('.............................................................................ENDED')
    spark.sparkContext.stop()
  except:
    if(os.path.exists(file_path)):
        os.remove(file_path)