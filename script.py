import subprocess
import time

with open('new-plan.txt') as f:
    lines = f.read()

    vetor = lines.split('\n')

    for element in vetor:
        parametro = element.split(',')

        
        command = 'spark-submit --master spark://master:7077 -class NaiveBayesJob --conf \"spark.shuffle.file.buffer='+parametro[0]+'k\" --conf \"spark.io.compression.lz4.blockSize='+parametro[1]+'k\" --conf \"spark.sql.files.maxPartitionBytes='+parametro[2]+'k\" --conf \"spark.sql.shuffle.partitions='+parametro[3]+'\" --conf \"spark.reducer.maxSizeInFlight='+parametro[4]+'k\" --conf \"spark.default.parallelism='+parametro[5]+'\" --conf \"spark.broadcast.blockSize='+parametro[6]+'k\" simple-project_2.12-1.0.jar'

        start_time = time.time()
        subprocess.run(command, shell=True)
        end_time = time.time()

        execution_time = end_time-start_time
        with open('times-bayes.txt', 'a') as f:
            f.write(str(execution_time)+'\n')
