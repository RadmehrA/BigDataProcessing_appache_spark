{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 from pyspark.sql import SparkSession\
from pyspark.sql.functions import col, count, avg\
\
# Initialize Spark\
spark = SparkSession.builder \\\
    .appName("BigDataAnalysis") \\\
    .getOrCreate()\
\
# Load Dataset\
df = spark.read.csv('user_log_data.csv', header=True)\
\
# Most Active Users\
active_users = df.groupBy('user_id').agg(count('*').alias('activity_count')).orderBy('activity_count', ascending=False)\
active_users.show()\
\
# Average Session Duration\
avg_session_duration = df.groupBy('user_id').agg(avg('session_duration').alias('avg_duration'))\
avg_session_duration.show()\
\
# Filter out Anomalies\
filtered_data = df.filter(col('session_duration') < 43200)\
filtered_data.show()\
\
# Save results to a CSV file\
active_users.write.csv('active_users.csv')\
avg_session_duration.write.csv('avg_session_duration.csv')\
}