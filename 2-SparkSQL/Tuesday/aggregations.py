"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min as spark_min, max as spark_max, countDistinct, desc

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# TODO 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy
agg_all = df.agg(
    spark_sum(col("amount")).alias("total_amount"),
    avg(col("amount")).alias("avg_amount"),
    spark_min(col("amount")).alias("min_amount"),
    spark_max(col("amount")).alias("max_amount"),
)
agg_all.show()
# TODO 1b: Count the total number of sales transactions
total_txns = df.agg(count("*").alias("total_transactions"))
total_txns.show()

# TODO 1c: Count distinct categories
distinct_cats = df.agg(countDistinct(col("category")).alias("distinct_categories"))
distinct_cats.show()

# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

# TODO 2a: Total sales amount by category
total_by_category = (
    df.groupBy("category")
    .agg(spark_sum(col("amount")).alias("total_revenue"))
    .orderBy(desc("total_revenue"))
)
total_by_category.show()

# TODO 2b: Average sale amount by month
avg_by_month = (
    df.groupBy("month")
    .agg(avg(col("amount")).alias("avg_sale_amount"))
    .orderBy("month")
)
avg_by_month.show()

# TODO 2c: Count of transactions by salesperson
txns_by_salesperson = (
    df.groupBy("salesperson")
    .agg(count("*").alias("transaction_count"))
    .orderBy(desc("transaction_count"), "salesperson")
)
txns_by_salesperson.show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# TODO 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!
category_metrics = (
    df.groupBy("category")
    .agg(count("*").alias("transaction_count"),
    spark_sum(col("amount")).alias("total_revenue"),
    avg(col("amount")).alias("avg_sale_amount"),
    spark_max(col("amount")).alias("highest_single_sale"),
    )
    .orderBy(desc("total_revenue"))
)
category_metrics.show()

# TODO 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)
salesperson_metrics = (
    df.groupBy("salesperson")
    .agg(count("*").alias("sales_count"),
    spark_sum(col("amount")).alias("total_revenue"),
    countDistinct(col("product")).alias("distinct_products_sold"),
)
    .orderBy(desc("total_revenue"))
)
salesperson_metrics.show()


# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# TODO 4a: Calculate total sales by month AND category
total_by_month_category = (
    df.groupBy("month", "category")
    .agg(spark_sum(col("amount")).alias("total_revenue"))
    .orderBy("month", desc("total_revenue"))
)
total_by_month_category.show()

# TODO 4b: Find the top salesperson by month (hint: use multi-column groupBy)
month_salesperson_revenue = (
    df.groupBy("month", "salesperson")
    .agg(spark_sum(col("amount")).alias("total_revenue"),
    count("*").alias("transaction_count"),
    )
)

top_salesperson_by_month = (
    month_salesperson_revenue
    .orderBy("month", desc("total_revenue"))
    .dropDuplicates(["month"])
    .orderBy("month")
)
top_salesperson_by_month.show()

# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# TODO 5a: Find categories with total revenue > 2000
categories_over_2000 = (
    df.groupBy("category")
    .agg(spark_sum(col("amount")).alias("total_revenue"))
    .filter(col("total_revenue") > 2000)
    .orderBy(desc("total_revenue"))
)
categories_over_2000.show()

# TODO 5b: Find salespeople who made more than 2 transactions
salespeople_over_2_txns = (
    df.groupBy("salesperson")
    .agg(count("*").alias("transaction_count"))
    .filter(col("transaction_count") > 2)
    .orderBy(desc("transaction_count"))
)
salespeople_over_2_txns.show()

# TODO 5c: Find month-category combinations with average sale > 500
month_cat_avg_over_500 = (
    df.groupBy("month", "category")
    .agg(avg(col("amount")).alias("avg_sale_amount"))
    .filter(col("avg_sale_amount") > 500)
    .orderBy("month", desc("avg_sale_amount"))
)
month_cat_avg_over_500.show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# TODO 6a: Which category had the highest average transaction value?
highest_avg_category = (
    df.groupBy("category")
    .agg(avg(col("amount")).alias("avg_transaction_value"))
    .orderBy(desc("avg_transaction_value"))
    .limit(1)
)
highest_avg_category.show()

# TODO 6b: Who is the top salesperson by total revenue?
top_salesperson = (
    df.groupBy("salesperson")
    .agg(spark_sum(col("amount")).alias("total_revenue"))
    .orderBy(desc("total_revenue"))
    .limit(1)
)
top_salesperson.show()

# TODO 6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column
most_diverse_month = (
    df.groupBy("month")
    .agg(countDistinct(col("product")).alias("distinct_products_sold"))
    .orderBy(desc("distinct_products_sold"))
    .limit(1)
)
most_diverse_month.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()