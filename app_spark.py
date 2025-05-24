import time
import findspark
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, max, min, count
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TemperatureProcessing").getOrCreate()

file_path = "D:/Studying/1Luxembourg/5 semester/CloudBased/assignment2spark/temperature.tsv"
df = spark.read.csv(file_path, sep="\t", header=False).toDF("Station_id", "Year", "Temperature", "Sensor_quality")
df = df.withColumn("Temperature", df["Temperature"].cast(FloatType()))

# 1. For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?
start_time = time.time()

valid_information = df.filter(df["Sensor_quality"] >= 0.95)
average_temperature = valid_information.groupBy("Year").avg("Temperature").orderBy("Year")
results = average_temperature.collect()

end_time = time.time()

print("Task 1:")
elapsed_time = end_time - start_time
print(f"Execution time for task 1: {elapsed_time:.5f} seconds")

print("Average temperature per year:")
for row in results:
    print(f"Year: {row['Year']}, Average temperature: {row['avg(Temperature)']}")


# 2. For every year in the dataset, find the station ID with the highest / lowest temperature.
start_time_highest = time.time()

max_temperature = df.groupBy("Year").agg(max("Temperature").alias("Max_Temperature"))
highest_temperature_station = df.join(max_temperature, ["Year"]) \
    .filter(df["Temperature"] == col("Max_Temperature")) \
    .select("Year", "Station_id", "Temperature")

highest_temperature_stations = highest_temperature_station.groupBy("Year").agg(count("Station_id").alias("Count")).orderBy("Year")
highest_stations_results = highest_temperature_stations.collect()  

end_time_highest = time.time()
elapsed_time_highest = end_time_highest - start_time_highest

start_time_lowest = time.time()

min_temperature = df.groupBy("Year").agg(min("Temperature").alias("Min_Temperature"))
lowest_temperature_station = df.join(min_temperature, ["Year"]) \
    .filter(df["Temperature"] == col("Min_Temperature")) \
    .select("Year", "Station_id", "Temperature")

lowest_stations_count = lowest_temperature_station.groupBy("Year").agg(count("Station_id").alias("Count")).orderBy("Year")
lowest_stations_results = lowest_stations_count.collect()  

end_time_lowest = time.time()
elapsed_time_lowest = end_time_lowest - start_time_lowest

print("\nTask 2:")
print(f"Execution time for finding stations with the highest temperature: {elapsed_time_highest:.5f} seconds")
print("Stations with the highest temperature per year:")
for row in highest_stations_results:
    print(f"Year: {row['Year']}, Number of stations: {row['Count']}")

print(f"\nExecution time for finding stations with the lowest temperature: {elapsed_time_lowest:.5f} seconds")
print("Stations with the lowest temperature per year:")
for row in lowest_stations_results:
    print(f"Year: {row['Year']}, Number of stations: {row['Count']}")

print(f"Total execution time for task 2: {elapsed_time_highest + elapsed_time_lowest:.5f} seconds")


# 3. For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.
start_time = time.time()

valid_information = df.filter(df["Sensor_quality"] >= 0.95)
max_temperature = valid_information.groupBy("Year").agg(max("Temperature").alias("Max_Temperature"))

stations_with_highest_temp = valid_information.join(max_temperature, ["Year"]) \
    .filter(valid_information["Temperature"] == col("Max_Temperature")) \
    .select("Year", "Station_id", "Temperature")

stations_count = stations_with_highest_temp.groupBy("Year").agg(count("Station_id").alias("Count")).orderBy("Year")

results = stations_count.collect()

end_time = time.time()
elapsed_time = end_time - start_time

print("\nTask 3:")
print(f"Execution time for task 3: {elapsed_time:.5f} seconds")
print("Stations with the highest temperature per year:")
for row in results:
    print(f"Year: {row['Year']}, Number of stations: {row['Count']}")