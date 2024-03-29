import subprocess
import time

with open('super-factorial-plan.txt') as f:
    lines = f.read()

    vetor = lines.split('\n')

    for element in vetor:
        parametro = element.split(',')

        
        command = 'spark-shell --master spark://master:7077 -i class-bayes.scala --conf \"spark.shuffle.file.buffer='+parametro[0]+'k\" --conf \"spark.io.compression.lz4.blockSize='+parametro[1]+'k\" --conf \"spark.sql.files.maxPartitionBytes='+parametro[2]+'k\" --conf \"spark.sql.shuffle.partitions='+parametro[3]+'\" --conf \"spark.reducer.maxSizeInFlight='+parametro[4]+'k\" --conf \"spark.default.parallelism='+parametro[5]+'\" --conf \"spark.broadcast.blockSize='+parametro[6]+'k\"'

        start_time = time.time()
        subprocess.run(command, shell=True)
        end_time = time.time()

        execution_time = end_time-start_time
        with open('super-bayes-times.txt', 'a') as f:
            f.write(str(execution_time)+'\n')
