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
# Load Online Retail Dataset\
df = spark.read.csv('online_retail.csv', header=True)\
\
# Most Active Users\
active_users = df.groupBy('CustomerID').agg(count('*').alias('activity_count')).orderBy('activity_count', ascending=False)\
active_users.show()\
\
# Average Session Duration (Assuming session_duration is a column)\
avg_session_duration = df.groupBy('CustomerID').agg(avg('InvoiceValue').alias('avg_duration'))\
avg_session_duration.show()\
\
# Filter out Anomalies (e.g., InvoiceValue > 1000)\
filtered_data = df.filter(col('InvoiceValue') > 1000)\
filtered_data.show()\
}