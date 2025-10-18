import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, max, when, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("NetworkDiskAlertingFinal") \
    .getOrCreate()

# 2. Define thresholds and schemas
net_in_threshold = 3688.63
disk_io_threshold = 4166.53
schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("value", DoubleType(), True)
])

# 3. Load and prepare the data
net_df = spark.read.csv(
    "net_data.csv",
    header=True,
    schema=schema
).withColumnRenamed("value", "net_in")

disk_df = spark.read.csv(
    "disk_data.csv",
    header=True,
    schema=schema
).withColumnRenamed("value", "disk_io")

# Convert string to timestamp type
net_df = net_df.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))
disk_df = disk_df.withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))

# 4. Join the two dataframes
combined_df = net_df.join(disk_df, ["timestamp", "server_id"])

# 5. Perform windowed aggregation with updated parameters
# Group data into 30-second windows, sliding every 10 seconds
windowed_agg_df = combined_df.groupBy(
    col("server_id"),
    window(col("timestamp"), "30 seconds", "10 seconds")
).agg(
    max("net_in").alias("max_net_in"),
    max("disk_io").alias("max_disk_io")
)

# 6. Apply the alerting logic
alerts_df = windowed_agg_df.withColumn("alert",
    when((col("max_net_in") > net_in_threshold) & (col("max_disk_io") > disk_io_threshold),
         "Network flood + Disk thrash suspected")
    .when((col("max_net_in") > net_in_threshold) & (col("max_disk_io") <= disk_io_threshold),
         "Possible DDoS")
    .when((col("max_disk_io") > disk_io_threshold) & (col("max_net_in") <= net_in_threshold),
         "Disk thrash suspected")
    .otherwise(lit(None))
)

# 7. Format, filter, and sort the output
final_df = alerts_df.filter(
    col("window.start") >= to_timestamp(lit("20:52:00"), "HH:mm:ss")
).select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
    col("alert")
).orderBy("server_id", "window_start")

# 8. Save the result to a single CSV file
final_df.coalesce(1).write.csv(
    "team_47_NET_DISK.csv",
    header=True,
    mode="overwrite"
)

print(" Spark job completed successfully!")
print("CSV file 'team_47_NET_DISK.csv' has been created with the final parameters.")
print(f"Total rows generated: {final_df.count()}")

# Stop the Spark session
spark.stop()