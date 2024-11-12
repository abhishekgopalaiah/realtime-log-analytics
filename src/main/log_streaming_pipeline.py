from pyspark.sql.functions import col, count

class LogStreamingMedallion:
    def __init__(self, config: dict):
        # self.spark = spark
        self.config = config
        self.log_file_path = config['paths']['log_file_path']
        self.bronze_path = config['paths']['bronze']
        self.silver_path = config['paths']['silver']
        self.gold_path = config['paths']['gold']
        self.checkpoint_dir = config['paths']['checkpoint']

    def write_logs_to_bronze(self):
        log_df = spark.readStream.format("json").schema(" log_level STRING, message STRING, timestamp STRING").load(self.log_file_path)
        query = log_df.writeStream \
                .outputMode("append") \
                .format("delta") \
                .option("path", self.bronze_path) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/bronze") \
                .start()
        return query  # Return the query handle
    
    def process_to_silver(self):
        bronze_df = spark.readStream.format("delta").load(self.bronze_path)
        silver_df = bronze_df.select("timestamp", "log_level", "message") \
                    .withColumn("processed_timestamp", col("timestamp").cast("timestamp"))
        query = silver_df.writeStream \
                 .format("delta") \
                 .option("path", self.silver_path) \
                 .option("checkpointLocation", f"{self.checkpoint_dir}/silver") \
                 .start()
        return query  # Return the query handle

    def aggregate_to_gold(self):
        silver_df = spark.readStream.format("delta").load(self.silver_path)
        gold_df = silver_df.groupBy("log_level") \
                    .agg(count("log_level").alias("log_count"))
        query = gold_df.writeStream \
                .outputMode("complete") \
                .format("delta") \
                .option("path", self.gold_path) \
                .option("checkpointLocation", f"{self.checkpoint_dir}/gold") \
                .start()
        return query  # Return the query handle