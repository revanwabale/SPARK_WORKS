import subprocess
import time

while(True):
  t1=int(time.time())
  subprocess.call('/usr/bin/spark-submit ./activity_checker.py >> ./log/activitychecker.log',shell=True)
  t2=int(time.time())
  if((t2-t1)<600):
      time.sleep(600-(t2-t1))
  else:
      continue
