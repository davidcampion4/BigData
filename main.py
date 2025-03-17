import pandas as pd
import time
import psutil
import os

def memory_used():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

total_start_time = time.time()

start_time = time.time()
df1 = pd.read_csv('charts.csv')
df2 = pd.read_csv('Top_spotify_songs.csv')
end_time = time.time()
print(f"Loading CSV files: {end_time - start_time:.4f} seconds")
print(f"Memory usage after loading CSV files: {memory_used():.2f} MB")

start_time = time.time()
merged_df = pd.merge(df1, df2, left_on=['song', 'artist'], right_on=['name', 'artists'], how='inner')
end_time = time.time()
print(f"Merging CSV files: {end_time - start_time:.4f} seconds")
print(f"Memory usage after merging CSV files: {memory_used():.2f} MB")

start_time = time.time()
merged_df.drop(columns=['name', 'artists'], inplace=True)
end_time = time.time()
print(f"Dropping track_name column: {end_time - start_time:.4f} seconds")
print(f"Memory usage after dropping column: {memory_used():.2f} MB")

start_time = time.time()
merged_df.drop_duplicates(subset=['song'], inplace=True)
end_time = time.time()
print(f"Dropping duplicate songs: {end_time - start_time:.4f} seconds")
print(f"Memory usage after dropping duplicates: {memory_used():.2f} MB")

start_time = time.time()
merged_df.to_csv('combined_file.csv', index=False)
end_time = time.time()
print(f"Saving to CSV: {end_time - start_time:.4f} seconds")
print(f"Memory usage after saving to CSV: {memory_used():.2f} MB")

start_time = time.time()
spotify_columns = [
    'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
    'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
    'time_signature', 'duration_ms'
]

most_common_entries = {}
for column in spotify_columns:
    most_common_entries[column] = merged_df[column].mode().iloc[0]

end_time = time.time()
print(f"Finding most common entries: {end_time - start_time:.4f} seconds")
print(f"Memory usage after finding most common entries: {memory_used():.2f} MB")

total_end_time = time.time()
total_time = total_end_time - total_start_time
print(f"\nTotal execution time: {total_time:.4f} seconds")

print("\nMost Common Entries:")
for column, value in most_common_entries.items():
    if column != "duration_ms":
        print(f"{column}: {value}")
    else:
        print(f"Duration in minutes: {value / 60000:.4f}")
