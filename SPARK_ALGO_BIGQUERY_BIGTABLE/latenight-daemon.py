import subprocess
import time
from datetime import datetime,timedelta
#check if time is == 5 JST
#then proceed ahead with while true as below
#while datetime.fromtimestamp(time.time(), ).strftime('%Y%m%d')
# Assuming 2 is date of sunday
# 9 is start hour of weekly script on sunday
while(True):
 if(int((datetime.now() + timedelta(hours=9)).strftime('%H')) == '9'):
  while(True):
    day_sec = 86400 
    daily_st_time = time.time()
    subprocess.call('/usr/bin/spark-submit ./ln_one_tag_generator.py >> ./log_log/latenight.log',shell=True)
    daily_et_time = time.time()
    trun_around_time = daily_et_time - daily_st_time
    sleep_time = day_sec - trun_around_time
    time.sleep(sleep_time)
 time.sleep(600)

