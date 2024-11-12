import json
import random
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

class LogGenerator:
    def __init__(self, log_file_path: str, num_logs: int = 1):
        # self.spark = spark
        self.log_file_path = log_file_path
        self.num_logs = num_logs
        self.log_levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
        self.messages = ["User logged in", "File not found", "Transaction completed", "User logged out", "Access denied"]


    def generate_log_entry(self) -> Row:
        """Generates a single log entry."""
        log_level = random.choice(self.log_levels)
        messages = random.choice(self.messages)
        timestamp = datetime.now().isoformat()
        message = f"{log_level}: {messages}"
        return Row(timestamp=timestamp, log_level=log_level, message=message)

    # def generate_logs(self):
    #     """Generates multiple log entries and writes them to a JSON file."""
    #     logs = [self.generate_log_entry() for _ in range(self.num_logs)]
    #     log_df = self.spark.createDataFrame(logs)
        
    #     # Write logs to the specified path in JSON format
    #     log_df.write.mode("append").json(self.log_file_path)
    #     print(f"Generated {self.num_logs} log entries and saved to {self.log_file_path}")

    def generate_logs(self):
        """Continuously generates log entries at specified intervals."""
        while True:
            logs = [self.generate_log_entry() for _ in range(self.num_logs)]  # Generate one log entry
            log_df = spark.createDataFrame(logs)
            
            # Write logs to the specified path in JSON format
            log_df.write.mode("append").json(self.log_file_path)
            print(f"Generated log entry: {log_df.collect()[0]}")
            time.sleep(1)  # Wait for the specified interval
            # print(f"Generated {self.num_logs} log entries and saved to {self.log_file_path}")


if __name__ == "__main__":
    # Initialize Spark session
    # spark = SparkSession.builder \
    #     .appName("LogGenerator") \
    #     .getOrCreate()

    # Specify the path to write logs
    log_file_path = "dbfs:/tmp/log_medallion/log_data_json"

    # Create the LogGenerator instance
    log_generator = LogGenerator(log_file_path)

    # Generate and save logs
    log_generator.generate_logs()