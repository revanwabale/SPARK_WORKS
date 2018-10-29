import subprocess
import time

while(True):
  t1=int(time.time())
  #TODO: In production change the path of the scripts
  subprocess.call('/usr/bin/spark-submit /home/revan/QC/Data_processor/Data_processor.py',shell=True)
  t2=int(time.time())
  if((t2-t1)<3600):
      time.sleep(3600-(t2-t1))
  else:
      time.sleep(3600)