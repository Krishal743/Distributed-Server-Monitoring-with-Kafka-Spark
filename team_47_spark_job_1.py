import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, when, date_format, lit, round
from pyspark.sql.functions import sum as _sum, count as _count
from pyspark.sql.types import DecimalType, FloatType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("CpuMemoryAlerting_Final_Filtered") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session initialized.")

# 2. Define thresholds and high-precision type
CPU_THRESHOLD = 82.1
MEM_THRESHOLD = 80.02
DEC_HIGH = DecimalType(38, 10)

# 3. Load and prepare the data
try:
    cpu_df_raw = spark.read.csv("cpu_data.csv", header=True)
    mem_df_raw = spark.read.csv("mem_data.csv", header=True)
    print("CSV files loaded successfully.")
except Exception as e:
    print(f"Error loading data files: {e}")
    spark.stop()
    raise

# 4. Clean and Standardize Data
# Cast types, convert to timestamp, and remove duplicates
cpu_df = cpu_df_raw.withColumn("cpu_pct", col("cpu_pct").cast(FloatType())) \
                   .withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss")) \
                   .dropDuplicates(["timestamp", "server_id"])

mem_df = mem_df_raw.withColumn("mem_pct", col("mem_pct").cast(FloatType())) \
                   .withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss")) \
                   .dropDuplicates(["timestamp", "server_id"])

print(f"Cleaned records | CPU: {cpu_df.count()}, Memory: {mem_df.count()}")

# 5. Perform High-Precision Windowed Aggregation (in parallel)
print("Performing high-precision aggregation...")
cpu_agg = cpu_df.groupBy("server_id", window(col("timestamp"), "30 seconds", "10 seconds")) \
                .agg(
                    _sum(col("cpu_pct").cast(DEC_HIGH)).alias("cpu_sum"),
                    _count("cpu_pct").alias("cpu_count")
                )

mem_agg = mem_df.groupBy("server_id", window(col("timestamp"), "30 seconds", "10 seconds")) \
                .agg(
                    _sum(col("mem_pct").cast(DEC_HIGH)).alias("mem_sum"),
                    _count("mem_pct").alias("mem_count")
                )

# 6. Join Aggregated Data and Calculate Final Averages
joined_agg = cpu_agg.join(
    mem_agg,
    on=[
        cpu_agg.server_id == mem_agg.server_id,
        cpu_agg.window == mem_agg.window
    ],
    how="inner"
).select(
    cpu_agg.server_id,
    cpu_agg.window,
    round(col("cpu_sum") / col("cpu_count"), 2).alias("avg_cpu"),
    round(col("mem_sum") / col("mem_count"), 2).alias("avg_mem")
)

# 7. Apply the Start Time Filter
filtered_by_time = joined_agg.filter(
    col("window.start") >= to_timestamp(lit("20:52:00"), "HH:mm:ss")
)

# 8. Apply Alerting Logic
alerts_df = filtered_by_time.withColumn("alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD),
         "High CPU + Memory stress")
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD),
         "CPU spike suspected")
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD),
         "Memory saturation suspected")
    .otherwise(lit(""))
)

# 9. Format and Sort Final Output
final_df = alerts_df.select(
    col("server_id"),
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
    col("avg_cpu"),
    col("avg_mem"),
    col("alert")
).orderBy("server_id", "window_start")

# 10. Save the Result
output_path = "team_47_CPU_MEM.csv"
final_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

print(f"\nJob completed successfully!")
print(f"Output saved to: {output_path}")
print(f"Total rows generated: {final_df.count()}")

# Stop the Spark session
spark.stop()