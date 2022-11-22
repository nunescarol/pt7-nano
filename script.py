import subprocess
import time

with open('plan.txt') as f:
    lines = f.read()

    vetor = lines.split('\n')

    for element in vetor:
        parametro = element.split(',')


        command = 'spark-shell --master spark://master:7077 -i class.scala --conf \"spark.shuffle.file.buffer='+parametro[0]+'k\" --conf \"spark.io.compression.lz4.blo>
        start_time = time.time()
        subprocess.run(command, shell=True)
        end_time = time.time()

        execution_time = end_time-start_time
        with open('times.txt', 'a') as f:
            f.write(str(execution_time)+'\n')