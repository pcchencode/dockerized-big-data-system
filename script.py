import time
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Reading
print("reading data from hdfs")
s = time.time()
df = spark.read.csv("hdfs:///usr/spotify/dataset.csv", header=True, escape='"')
df.show(5)
e = time.time()
exec_time = e-s
print(f"It takes {exec_time}s to read data from hdfs")

print("print out the schema")
df.printSchema()

# Analyzing
## 1. average popularity within track_genre
print("Calculating average popularity within track_genre")
df = df.withColumn('popularity', df['popularity'].cast(IntegerType()))
s = time.time()
stat_df = df.groupby('track_genre').mean('popularity')
e = time.time()
stat_df.show()
exec_time = e-s
print(f"It takes {exec_time}s to calculate average popularity within track_genre")

# Filtering
print("filtering data")
s = time.time()
low_df = df.filter(df.popularity<=40)
med_df = df.filter((df.popularity>40) & (df.popularity<=80))
high_df = df.filter(df.popularity>80)
e = time.time()
exec_time = e-s
print(f"It takes {exec_time}s to filter the data")
# low_df.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/usr/spotify/low.csv")
# med_df.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/usr/spotify/med.csv")
# high_df.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/usr/spotify/high.csv")

# Exporting
print("exporting popularity statistics data")
stat_df.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save("/usr/spotify/mean_populaarity.csv")