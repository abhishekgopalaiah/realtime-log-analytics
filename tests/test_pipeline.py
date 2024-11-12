# %run /Users/abhishekgopalaiah@gmail.com/realtime-log-analytics/config/config

from pyspark.sql import SparkSession
import time
import json

# import LogStreamingMedallion class code

# Configuration for paths
config = {
    "paths": {
        "log_file_path": "/tmp/log_medallion/log_data_json",
        "bronze": "/tmp/log_medallion/bronze",
        "silver": "/tmp/log_medallion/silver",
        "gold": "/tmp/log_medallion/gold",
        "checkpoint": "/tmp/log_medallion/checkpoints"
    }
}

# # Initialize the Spark session and class instance
# spark = SparkSession.builder.appName("LogMedallionTest").getOrCreate()
log_pipeline = LogStreamingMedallion(config)

# Step 1: Test Bronze Layer
bronze_query = log_pipeline.write_logs_to_bronze()
time.sleep(10)  # Let the stream run for a while to collect data
bronze_query.stop()  # Stop the streaming query

# Check the data in the Bronze layer
bronze_data = spark.read.format("delta").load(config['paths']['bronze'])
bronze_data.show()

# Step 2: Test Silver Layer
silver_query = log_pipeline.process_to_silver()
time.sleep(10)  # Let the Silver layer process data
silver_query.stop()

# Check the data in the Silver layer
silver_data = spark.read.format("delta").load(config['paths']['silver'])
silver_data.show()

# Step 3: Test Gold Layer
gold_query = log_pipeline.aggregate_to_gold()
time.sleep(10)  # Allow Gold layer processing
gold_query.stop()

# Check the data in the Gold layer
gold_data = spark.read.format("delta").load(config['paths']['gold'])
gold_data.show()
