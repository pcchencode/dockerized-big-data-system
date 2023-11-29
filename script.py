import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()



print("reading data from hdfs")
s = time.time()
df = spark.read.csv("hdfs:///usr/spotify/dataset.csv", header=True, escape='"')
df.show(5)
e = time.time()
exec_time = e-s
print(f"It takes {exec_time} to read data from hdfs")


print("reading data from hive")
s = time.time()
spark.sql("select * from dataset").show(5)
e = time.time()
exec_time = e-s
print(f"It takes {exec_time} to read data from hive")