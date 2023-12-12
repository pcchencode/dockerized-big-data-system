import time
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, desc, stddev, avg, sum, count

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

# Analyzing - 1 Start
## 1. average popularity within track_genre
print("Calculating average popularity within track_genre")
df = df.withColumn('popularity', df['popularity'].cast(IntegerType()))
s = time.time()
stat_df = df.groupby('track_genre').mean('popularity')
e = time.time()
stat_df.show()
exec_time = e-s
print(f"It takes {exec_time}s to calculate average popularity within track_genre")

## 2. duration vs popularity to show the relationship between the duration of songs and popularity
print("Calculating duration vs popularity")
s = time.time()
# shows the duration_ms in relation to popularity of a song and displays it
duration_popularity_df = df.select('duration_ms', 'popularity')
duration_popularity_df.show()
# converts duration from milliseconds to seconds
duration_popularity_df = duration_popularity_df.withColumn('duration_sec', col('duration_ms') / 1000)
# calculates the mean popularity for different durations and displays results
duration_popularity_df = duration_popularity_df.groupBy('duration_sec').mean('popularity')
duration_popularity_df.show()
e = time.time()
exec_time = e - s
print(f"It takes {exec_time}s to caluclate duration vs popularity")

##3. key and mode in relation with popularity
print("Analyzing Key and Mode vs. Popularity")
s = time.time()
# groups the data by key and mode and calculating the mean popularity for each group.
key_mode_popularity_df = df.groupBy('key', 'mode').mean('popularity')
key_mode_popularity_df.show()

e = time.time()
exec_time = e - s
print(f"It takes {exec_time}s to analyze Key and Mode vs. Popularity")

##4. acousticness and instrumentalness in relation with popularity
print("Analyzing Acousticness and Instrumentalness vs. Popularity")
s = time.time()
# groups the data by acousticness and instrumentalness and calculates the mean of the popularity for each group.
acousticness_instrumentalness_popularity_df = df.groupBy('acousticness', 'instrumentalness').agg({'popularity': 'mean'})
acousticness_instrumentalness_popularity_df.show()

e = time.time()
exec_time = e-  s
print(f"It takes {exec_time}s to analyze Acousticness and Instrumentalness vs. Popularity")
# Analyzing - 1 End


# Analysis - 2 Start
# 1. Calculating the avg tempo for each genre
start_time = time.time()
tempo_genre_df = df.select('tempo', 'track_genre')
# tempo_genre_df = df.groupBy('genre').agg({'tempo': 'mean'})
tempo_genre_df = df.groupBy('track_genre').agg(avg("tempo").alias("avg_tempo"))
tempo_genre_df.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze Acousticness and Instrumentalness vs. Popularity")

# 2. Calculate average popularity of genre when not taking songs in 4x4 in consideration

start_time = time.time()
filtered_df = df.filter(col("time_signature") != 4)
genre_avg_popularity = (
    filtered_df
    .groupBy("track_genre")
    .agg({"popularity": "avg"})
    .withColumnRenamed("avg(popularity)", "avg_popularity")
)
genre_avg_popularity.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze average popularity of genre when not taking songs in 4x4 in consideration")

# 3. Calculate average popularity of genre when only taking songs in 4x4 in consideration

start_time = time.time()
filtered_df = df.filter(col("time_signature") == 4)
genre_avg_popularity = (
    filtered_df
    .groupBy("track_genre")
    .agg({"popularity": "avg"})
    .withColumnRenamed("avg(popularity)", "avg_popularity")
)
genre_avg_popularity.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze average popularity of genre when only taking songs in 4x4 in consideration")

# 4. Compare Average Speechiness, Instrumentalness and the ratio Speechiness/Instrumentalness per genre

start_time = time.time()

# average speechiness per genre
avg_speechiness_per_genre = (
    df.groupBy("track_genre")
    .agg({"speechiness": "avg"})
    .withColumnRenamed("avg(speechiness)", "avg_speechiness")
)

# average instrumentalness per genre
avg_instrumentalness_per_genre = (
    df.groupBy("track_genre")
    .agg({"instrumentalness": "avg"})
    .withColumnRenamed("avg(instrumentalness)", "avg_instrumentalness")
)

# joining the two dataframes
avg_speech_instr = (
    avg_speechiness_per_genre
    .join(avg_instrumentalness_per_genre, "track_genre")
)

# avg speechiness / instrumentalness per genre
avg_speech_instr = avg_speech_instr.withColumn(
    "avg_speech_instr_ratio",
    avg_speech_instr.avg_speechiness / avg_speech_instr.avg_instrumentalness
)
avg_speech_instr.show()


end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze Speechiness, Instrumentalness per genre")
# Analysis - 2 End

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