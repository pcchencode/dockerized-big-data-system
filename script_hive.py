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

# Analyzing - 1 Start
# 1 calculating averages of popularity within track genre
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

# 2 Duration vs. Popularity Analysis
print("Analyzing duration vs. popularity")
s = time.time()

# Select duration_ms and popularity columns
sql_duration = """
SELECT duration_ms, popularity
FROM dataset
"""
duration_df = spark.sql(sql_duration)

# Convert duration_ms to seconds
sql_duration_convert = """
SELECT duration_ms, popularity, duration_ms / 1000 AS duration_sec
FROM dataset
"""
duration_popularity_df = spark.sql(sql_duration_convert)

# Group by duration_sec and calculate mean popularity
sql_duration_group = """
SELECT duration_sec, AVG(popularity) AS mean_popularity
FROM (
    SELECT duration_ms, popularity, duration_ms / 1000 AS duration_sec
    FROM dataset
) subquery
GROUP BY duration_sec
"""
duration_df = spark.sql(sql_duration_group)
duration_df.show()

e = time.time()
exec_time = e - s
print(f"Hive takes {exec_time}s to analyze duration vs. popularity")

# 4. Key and Mode Analysis
print("Analyzing Key and Mode vs. Popularity")
s = time.time()

# Selects key, mode and average popularity from dataset and  groups them to compare relations
sql_key_mode = """
SELECT key, mode, AVG(popularity) AS mean_popularity
FROM dataset
GROUP BY key, mode
"""
key_mode_df = spark.sql(sql_key_mode)
key_mode_df.show()

e = time.time()
exec_time = e - s
print(f"Hive takes {exec_time}s to analyze Key and Mode vs. Popularity")

# 5. Valence Analysis
print("Analyzing Valence vs. Popularity")
s = time.time()
# Selects valence and average popularity from dataset and  groups them to compare relations
sql_valence = """
SELECT valence, AVG(popularity) AS mean_popularity
FROM dataset
GROUP BY valence
"""
valence_df = spark.sql(sql_valence)
valence_df.show()

e = time.time()
exec_time = e - s
print(f"Hive takes {exec_time}s to analyze valence in relation to popularity")
# Analysis - 1 End



# Analysis - 2 Start
# 1. Calculating the avg tempo for each genre
start_time = time.time()

avg_tempo_per_genre = """
SELECT track_genre, AVG(tempo) AS avg_tempo
FROM dataset
GROUP BY track_genre;
"""
avg_tempo_per_genre = spark.sql(avg_tempo_per_genre)
avg_tempo_per_genre.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze Acousticness and Instrumentalness vs. Popularity")

# 2. Calculate average popularity of genre when not taking songs in 4x4 in consideration

start_time = time.time()

avg_popul_time_sig_per_genre = """
SELECT track_genre, AVG(popularity) AS avg_popularity
FROM dataset
WHERE time_signature <> 4
GROUP BY track_genre;
"""

avg_popul_time_sig_per_genre = spark.sql(avg_popul_time_sig_per_genre)
avg_popul_time_sig_per_genre.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze average popularity of genre when not taking songs in 4x4 in consideration")

# 3. Calculate average popularity of genre when only taking songs in 4x4 in consideration

start_time = time.time()

avg_popul_time_sig_per_genre = """
SELECT track_genre, AVG(popularity) AS avg_popularity
FROM dataset
WHERE time_signature = 4
GROUP BY track_genre;
"""
avg_popul_time_sig_per_genre = spark.sql(avg_popul_time_sig_per_genre)
avg_popul_time_sig_per_genre.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze average popularity of genre when only taking songs in 4x4 in consideration")

# 4. Compare Average Speechiness, Instrumentalness and the ratio Speechiness/Instrumentalness per genre

start_time = time.time()

speech_instr_per_genre = """
SELECT 
    track_genre,
    AVG(speechiness) AS avg_speechiness,
    AVG(instrumentalness) AS avg_instrumentalness,
    AVG(speechiness) / AVG(instrumentalness) AS avg_speech_instr_ratio
FROM 
    dataset
GROUP BY 
    track_genre;
"""
speech_instr_per_genre = spark.sql(speech_instr_per_genre)
speech_instr_per_genre.show()

end_time = time.time()
exec_time = end_time -  start_time
print(f"It takes {exec_time}s to analyze Speechiness, Instrumentalness per genre")
# Analysis - 2 End



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