# Big Data Processing using Apache Spark

## Description
This project demonstrates how to process large datasets using Apache Spark. The focus is on analyzing a user activity log dataset to extract insights such as the most active users, session durations, and filtering anomalies.

## Problem
Handling and processing large datasets efficiently can be challenging with traditional tools. Apache Spark, with its distributed computing capabilities, offers a solution for processing big data at scale.

## Methodology
1. Analyze user activity logs to calculate:
   - The most active users based on session activity.
   - The average session duration for users.
   - Filter out anomalies where the session duration exceeds 12 hours.

## Requirements
- Apache Spark
- Python 3.x
- `pyspark` library

## Code Example
```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("BigDataAnalysis").getOrCreate()

# Load Dataset
df = spark.read.csv('user_log_data.csv', header=True)
