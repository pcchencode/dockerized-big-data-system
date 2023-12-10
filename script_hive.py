import time
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Reading
print("reading data from Hive")
s = time.time()
df = spark.sql("SELECT * FROM dataset")
df.show(5)
e = time.time()
exec_time = e-s
print(f"It takes {exec_time}s to read data from hive")

print("print out the schema")
df.printSchema()

# Analyzing
print("Calculating average popularity within track_genre")
sql = """
SELECT track_genre, avg(popularity) as mean_popularity
FROM dataset
GROUP BY track_genre
"""
s = time.time()
stat_df = spark.sql(sql)
e = time.time()
stat_df.show()
exec_time = e-s
print(f"Hive takes {exec_time}s to calculate average popularity within track_genre")

# Filtering
print("filtering data by Hive sql")
s = time.time()
low_df = spark.sql("SELECT * FROM dataset WHERE popularity<=40")
med_df = spark.sql("SELECT * FROM dataset WHERE popularity>40 and popularity<=80")
high_df = spark.sql("SELECT * FROM dataset WHERE popularity>80")
e = time.time()
exec_time = e-s
print(f"It takes {exec_time}s to filter the data")
# low_df.write.mode('overwrite').saveAsTable("low_df")
# med_df.write.mode('overwrite').saveAsTable("med_df")
# high_df.write.mode('overwrite').saveAsTable("high_df")

# Exporting
print("exporting popularity statistics data to Hive")
stat_df.write.mode('overwrite').saveAsTable("stat")

