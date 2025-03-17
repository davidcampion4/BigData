import time
import psutil
import os
from pyspark import SparkContext

def memory_used():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

sc = SparkContext("local", "BigData")

start_time = time.time()
rdd_charts = sc.textFile("charts.csv")
rdd_spotify = sc.textFile("Top_spotify_songs.csv")
end_time = time.time()
print(f"Loading CSV files: {end_time - start_time:.4f} seconds")
print(f"Memory usage after loading CSV files: {memory_used():.2f} MB")

start_time = time.time()

split_rdd_charts = rdd_charts.map(lambda line: line.split(","))
split_rdd_spotify = rdd_spotify.map(lambda line: [entry.replace('"', '') for entry in line.split(",")])

key_value_charts = split_rdd_charts.map(lambda entry: ((entry[2], entry[3]), entry))
key_value_spotify = split_rdd_spotify.map(lambda entry: ((entry[1], entry[2]), entry))

merged_rdd = key_value_charts.join(key_value_spotify)

cleaned_merged_rdd = merged_rdd.map(lambda line: line[1][0] + line[1][1])

end_time = time.time()
print(f"Merging datasets: {end_time - start_time:.4f} seconds")
print(f"Memory usage after merging datasets: {memory_used():.2f} MB")

sc.stop()
