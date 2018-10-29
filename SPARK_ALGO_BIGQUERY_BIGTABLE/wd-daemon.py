import subprocess
import time
from datetime import datetime,timedelta

#check if time is == 5 JST
#then proceed ahead with while true as below
#where 5 am jst is start time on evryday.

while(True):
 if(int((datetime.now() + timedelta(hours=9)).strftime('%H')) == 5):
  while(True):
    day_sec = 86400
    daily_st_time = time.time()
    subprocess.call('/usr/bin/spark-submit ./wd_utilization_calculator.py >> ./log_log/wd_utilizaton.log',shell=True)
    daily_et_time = time.time()
    trun_around_time = daily_et_time - daily_st_time
    sleep_time = day_sec - trun_around_time
    time.sleep(sleep_time)
 time.sleep(600)

