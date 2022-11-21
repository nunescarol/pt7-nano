import subprocess
import time

command = 'spark-shell --class  --master spark://master:7077 class.scala'

start_time = time.time()
subprocess.run(command, shell=True)
end_time = time.time()

execution_time = end_time-start_time
with open('times.txt') as f:
    f.write(execution_time+'\n')