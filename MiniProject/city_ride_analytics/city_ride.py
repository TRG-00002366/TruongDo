from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.sql.window import Window

#Spark Session
spark = (SparkSession.builder 
    .appName("City Ride Analytics") 
    .getOrCreate()
)
#T1 Create DataFrame 
#load rides
rides_df = (spark.read 
    .option("header", "true") 
    .option("inferSchema", "true") 
    .csv("rides.csv")
)
#load drivers 
drivers_df = (spark.read 
    .option("header", "true") 
    .option("inferSchema", "true") 
    .csv("drivers.csv")
)
rides_df.show()
drivers_df.show()

#T2 Display Schema 
print("Rides Schema")
rides_df.printSchema()
print("Drivers Schema")
drivers_df.printSchema()
print("rides dataset:")
rides_df.show(5)
print("drivers dataset:")
drivers_df.show(5)

#T3 Column Selection
rides_selected = rides_df.select(
    "ride_id",
    "pickup_location",
    "dropoff_location",
    "fare_amount"
).show(5)

#T4 Filtering Rides
premium_long_rides = rides_df.filter(
    (col("distance_miles") > 5.0) & (col("ride_type") == "premium")
    )

#T5 Adding a Derived Column 
rides_with_fare_per_mile = rides_df.withColumn(
    "fare_per_mile",
    col("fare_amount") / col("distance_miles")
)

#T6 Removing Columns
rides_without_type = rides_df.drop("ride_type")

#T7 Remaning Columns
rides_renamed = rides_df.withColumnRenamed(
    "pickup_location", "start_area"
).withColumnRenamed(
    "dropoff_location", "end_area"
)

#T8 Aggregation Total 
fare_by_type = rides_df.groupBy("ride_type").agg(
    sum("fare_amount").alias("total_fare")
)

#T9 Aggregation Average
fare_by_type = rides_df.groupBy("ride_type").agg(
    sum("fare_amount").alias("total_fare")
)

#T10 Join
rides_drivers_joined = rides_df.join(
    drivers_df,
    on="driver_id",
    how="inner"
)

#T11 set operations 
peak_df = rides_df.filter(
    col("ride_date").startswith("2025-01")
)
off_peak_df = rides_df.filter(
    col("ride_date").startswith("2025-02")
)
combined_df = peak_df.union(off_peak_df)

#T12 Spark SQL
rides_df.createOrReplaceTempView("rides")
top_3_fares = spark.sql("""
    SELECT
        ride_id,
        fare_amount,
        pickup_location,
        dropoff_location
    FROM rides
    ORDER BY fare_amount DESC
    LIMIT 3
    """)

#O1 Multi-Column
sorted_rides = rides_df.orderBy(
    col("fare_amount").desc(),
    col("distance_miles").asc()
)

#O2 Handling Nulls
null_ratings = rides_df.filter(col("rating").isNull()).count()
rides_df = rides_df.fillna({"rating": 0.0})

#O3 Conditional Column
rides_with_category = rides_df.withColumn(
    "ride_category",
    when(col("distance_miles") < 3, "short")
    .when((col("distance_miles") >= 3) & (col("distance_miles") <= 8), "medium")
    .otherwise("long")
)

#O4 Window Function
window_spec = Window.partitionBy("driver_id") \
                    .orderBy("ride_date") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
rides_running_total = rides_df.withColumn(
    "running_total_fare",
    sum("fare_amount").over(window_spec)
)

#O5 Saving Results

(rides_drivers_joined.write 
    .mode("overwrite") 
    .parquet("Ride_Analytics_Results/joined_rides.parquet"))


(fare_by_type.write 
    .mode("overwrite") 
    .option("header", True) 
    .csv("Ride_Analytics_Results/fare_by_type.csv"))