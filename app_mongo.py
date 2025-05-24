from pymongo import MongoClient
import pandas as pd
import time

file_path = "D:/Studying/1Luxembourg/5 semester/CloudBased/assignment2spark/temperature.tsv"
df = pd.read_csv(file_path, sep="\t", header=None, names=["Station_id", "Year", "Temperature", "Sensor_quality"])

data = df.to_dict("records")

client = MongoClient("mongodb://localhost:27017/")
db = client["temperature_db"]
collection = db["temperature_data"]

collection.drop()
collection.insert_many(data)
print("Data is successfully inserted into MongoDB.")

# 1. For all valid information (i.e. sensor quality >= 0.95), what is the average temperature per year?
def average_temperature_per_year(collection):
    start_time = time.time()

    pipeline = [
        {"$match": {"Sensor_quality": {"$gte": 0.95}}},
        {"$group": {"_id": "$Year", "avg_temp": {"$avg": "$Temperature"}}},
        {"$sort": {"_id": 1}} 
    ]

    results = list(collection.aggregate(pipeline))

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Execution time for 1 MongoDB query: {elapsed_time:.5f} seconds")

    print("Average temperature per year:")
    for result in results:
        print(f"Year: {result['_id']}, Average Temperature: {result['avg_temp']}")        

print("\nTask 1:")
average_temperature_per_year(collection)

# 2. For every year in the dataset, find the station ID with the highest / lowest temperature.
def highest_temperature_stations_per_year(collection):
    start_time = time.time()

    max_temp_per_year = list(collection.aggregate([
        {"$group": {"_id": "$Year", "max_temp": {"$max": "$Temperature"}}}
    ]))

    results = []
    for entry in max_temp_per_year:
        year = entry["_id"]
        max_temp = entry["max_temp"]
        count = collection.count_documents({"Year": year, "Temperature": max_temp})
        results.append((year, count))

    end_time = time.time()

    sorted_results = sorted(results, key=lambda x: x[0])

    print(f"Execution time for 2 MongoDB query (highest temperature): {end_time - start_time:.5f} seconds")
    print("Highest temperature stations per year:")
    for year, count in sorted_results:
        print(f"Year: {year}, Number of Stations: {count}")

def lowest_temperature_stations_per_year(collection):
    start_time = time.time()

    min_temp_per_year = list(collection.aggregate([
        {"$group": {"_id": "$Year", "min_temp": {"$min": "$Temperature"}}}
    ]))

    results = [] 
    for entry in min_temp_per_year:
        year = entry["_id"]
        min_temp = entry["min_temp"]
        count = collection.count_documents({"Year": year, "Temperature": min_temp})
        results.append((year, count))

    end_time = time.time()

    sorted_results = sorted(results, key=lambda x: x[0])

    print(f"Execution time for 2 MongoDB query (lowest temperature): {end_time - start_time:.5f} seconds")
    print("Lowest temperature stations per year:")
    for year, count in sorted_results:
        print(f"Year: {year}, Number of Stations: {count}")

print("\nTask 2:")
highest_temperature_stations_per_year(collection)
lowest_temperature_stations_per_year(collection)

# 3. For every year in the dataset, find the station ID with the highest maximal temperature for all stations with sensor quality >= 0.95.
def highest_valid_temperature_stations(collection):
    start_time = time.time()

    max_temp_per_year_valid = list(collection.aggregate([
        {"$match": {"Sensor_quality": {"$gte": 0.95}}}, 
        {"$group": {"_id": "$Year", "max_temp": {"$max": "$Temperature"}}}
    ]))

    results = []  
    for entry in max_temp_per_year_valid:
        year = entry["_id"]
        max_temp = entry["max_temp"]
        count = collection.count_documents({"Year": year, "Temperature": max_temp, "Sensor_quality": {"$gte": 0.95}})
        results.append((year, count))

    end_time = time.time()

    sorted_results = sorted(results, key=lambda x: x[0])

    print(f"Execution time for 3 MongoDB query: {end_time - start_time:.5f} seconds")
    print("Highest maximal temperature stations per year (valid sensors):")
    for year, count in sorted_results:
        print(f"Year: {year}, Number of Stations: {count}")

print("\nTask 3:")
highest_valid_temperature_stations(collection)