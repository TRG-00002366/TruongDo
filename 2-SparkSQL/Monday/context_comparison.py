"""
Exercise: Context Comparison
============================
Week 2, Monday

Explore the relationship between SparkSession and SparkContext.
Complete the TODOs and answer the conceptual questions in comments.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Understanding the Relationship
# =============================================================================

print("=== Task 1: SparkSession and SparkContext Relationship ===")

# TODO 1a: Create a SparkSession
spark = (
    SparkSession.builder
    .appName("ContextComparison")
    .master("local[*]")
    .getOrCreate()
)  # Your code here


# TODO 1b: Access the SparkContext
sc = spark.sparkContext  # Your code here (HINT: spark.sparkContext)


# TODO 1c: Prove they are connected
# Print app name from BOTH SparkSession and SparkContext
print(f"SparkSession app name: {spark.sparkContext.appName}")  # Complete
print(f"SparkContext app name: {sc.appName}")  # Complete

# Verify they share the same application ID
print(f"SparkSession app ID: {spark.sparkContext.applicationId}")  # Complete
print(f"SparkContext app ID: {sc.applicationId}")  # Complete


# TODO 1d: Answer these questions in comments below:
# Q1: Can you create a SparkContext after SparkSession exists?
# ANSWER: no when Sparksession exist, it already created 
# the SparkContext
#

# Q2: What happens if you try? (You can test this if you want)
# ANSWER:It throws an error
#
#


# =============================================================================
# TASK 2: RDD vs DataFrame Operations
# =============================================================================

print("\n=== Task 2: RDD vs DataFrame Operations ===")

# TODO 2a: Create an RDD with [1, 2, 3, 4, 5]
rdd = sc.parallelize([1, 2, 3, 4, 5])  # Your code here (HINT: sc.parallelize(...))


# TODO 2b: Create a DataFrame with the same data
# HINT: spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)],
     ["value"]  # Your code here
)

# TODO 2c: Double the values in the RDD using map()
rdd_doubled = rdd.map(lambda x: x * 2)  # Your code here


# TODO 2d: Double the values in the DataFrame using withColumn
from pyspark.sql.functions import col
df_doubled = df.withColumn("value_doubled", col("value") * 2)  # Your code here


# Print results
print("RDD doubled:")
print(rdd_doubled.collect())

print("DataFrame doubled:")
df_doubled.show()


# TODO 2e: Convert RDD to DataFrame
rdd_to_df = rdd.map(lambda x: (x,)).toDF(["value"])  # Your code here


# TODO 2f: Convert DataFrame to RDD
df_to_rdd = df.rdd  # Your code here (HINT: df.rdd)


# TODO 2g: Answer these questions:
# Q3: Which approach (RDD or DataFrame) felt more natural for this task?
# ANSWER: DataFrame because it has named columns, aggregations, and joins
#
#

# Q4: What data type are the elements in df.rdd? (print first element to check)
# ANSWER: pyspark.sql.types.Row Objects
#
#


# =============================================================================
# TASK 3: Broadcast and Accumulator Access
# =============================================================================

print("\n=== Task 3: Broadcast and Accumulator ===")

# TODO 3a: Create a broadcast variable with a lookup dictionary
# Example: {"NY": "New York", "CA": "California", "TX": "Texas"}
lookup_data = {"NY": "New York", "CA": "California", "TX": "Texas"}
broadcast_lookup = sc.broadcast(lookup_data)  # Your code here (HINT: sc.broadcast(...))


# TODO 3b: Create an accumulator initialized to 0
counter = sc.accumulator(0)  # Your code here (HINT: sc.accumulator(0))


# TODO 3c: Use both in an RDD operation
# Create an RDD of state codes and:
# 1. Map each code to its full name using the broadcast variable
# 2. Count how many items are processed using the accumulator

states_rdd = sc.parallelize(["NY", "CA", "TX", "NY", "CA"])

# Your code here to use broadcast and accumulator
def map_state(code: str):
    counter.add(1)
    return broadcast_lookup.value.get(code, "UNKNOWN")

result = states_rdd.map(map_state)

# Print results
print(f"Mapped states: {result.collect()}")
print(f"Items processed: {counter.value}")


# Q5: Why are broadcast and accumulator accessed via SparkContext instead of SparkSession?
# ANSWER: Broadcast and accumulators are Spark Core distributed they both live on SparkContext
# SparkSession is a higher-level entry point mainly for SQL
#


# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n=== Conceptual Questions ===")

# Answer these questions in the comments below:

# Q6: In a new PySpark 3.x project, which entry point would you use and why?
# ANSWER:SparkSession, because it is unified entry point for DataFrames and SQL
#
#
#

# Q7: You inherit legacy Spark 1.x code that uses SQLContext. 
#     What is the minimal change to modernize it?
# ANSWER:Create a SparkSession and use it instead of SQLContext
#
#
#

# Q8: Describe the relationship between SparkSession, SparkContext, 
#     SQLContext, and HiveContext (you can use ASCII art):
# ANSWER: SparkSession is the unified entry point in Spark 2.x that manages SparkContext
# it replaces the older SQLContext and HiveContext. SparkContext remains the core engine responsible for distributed execution
# and RDD operations 
#
#
#


# =============================================================================
# CLEANUP
# =============================================================================

# TODO: Stop the SparkSession
spark.stop()