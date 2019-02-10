spark-submit --class org.scut.ccnl.tools.SparkPipeWithoutPy --master yarn --deploy-mode cluster --total-executor-cores 4 --executor-cores 4 --executor-memory 8G ~/pipemem/build/libs/pipeMEM-1.0-SNAPSHOT.jar /efs/execute_mem.sh \
hdfs:///user/ec2-user/merge.fastq \
hdfs:///user/ec2-user/result 
