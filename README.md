# Temperature Data Analysis — PySpark & Python-MongoDB

This project demonstrates how to process and analyze large-scale temperature sensor data using two different approaches: **Apache Spark** and **Python-MongoDB**. The dataset contains 50,000+ tab-separated records with station temperature readings and sensor quality scores.

## Dataset Format

Each record in `temperature.tsv` is structured as:

```
Station_id \t Year \t Measured Temperature \t Sensor Quality
```

## Objectives

**Using PySpark and MongoDB**, the following tasks are performed:

1. Compute the average temperature per year for records with sensor quality ≥ 0.95.
2. For each year, find the station with the highest and lowest temperature.
3. For each year, find the station with the highest *maximum* temperature (sensor quality ≥ 0.95).

## Timing Results

The table below summarizes the execution times for both Python-Spark and Python-MongoDB implementations for the three tasks:

| Task Number | Python - Spark Execution Time [seconds] | Python - MongoDB Execution Time [seconds] |
|--------------|----------------------------------------|------------------------------------------|
| 1            | 1.18093                                | 0.031                                    |
| 2            | 1.74104 (highest temp: 1.05552 + lowest temp: 0.68553) | 0.2455 (highest temp: 0.125 + lowest temp: 0.1205) |
| 3            | 0.658                                  | 0.131                                    |


The table shows that MongoDB is much more faster for all tasks, particularly for simple queries. In this case, MongoDB handles individual tasks quickly due to its minimal overhead, while Spark's distributed nature introduces delays for relatively simple computations.
