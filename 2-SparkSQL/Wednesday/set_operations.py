"""
Exercise: Set Operations
========================
Week 2, Wednesday

Practice union, intersect, except operations on customer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Set Ops").master("local[*]").getOrCreate()

# January customers
jan_customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (4, "Diana", "diana@email.com")
], ["customer_id", "name", "email"])

# February customers  
feb_customers = spark.createDataFrame([
    (2, "Bob", "bob@email.com"),
    (4, "Diana", "diana@email.com"),
    (5, "Eve", "eve@email.com"),
    (6, "Frank", "frank@email.com")
], ["customer_id", "name", "email"])

# March customers (different column order!)
mar_customers = spark.createDataFrame([
    ("grace@email.com", "Grace", 7),
    ("henry@email.com", "Henry", 8),
    ("bob@email.com", "Bob", 2)  # Returning customer
], ["email", "name", "customer_id"])

print("=== Exercise: Set Operations ===")
print("\nJanuary Customers:")
jan_customers.show()
print("February Customers:")
feb_customers.show()
print("March Customers (different column order!):")
mar_customers.show()

# =============================================================================
# TASK 1: Union Operations (20 mins)
# =============================================================================

print("\n--- Task 1: Union ---")

# TODO 1a: Union January and February customers (keep duplicates)
jan_feb_union = jan_customers.union(feb_customers)
jan_feb_union.show()

# TODO 1b: Union January and February, then remove duplicates
jan_feb_unique_rows = jan_feb_union.distinct()
jan_feb_unique_rows.show()

# TODO 1c: Try union with March customers - what happens? throws errors
# Use unionByName to fix it
jan_feb_mar_union = jan_feb_union.unionByName(mar_customers)
jan_feb_mar_union.show()

# TODO 1d: How many unique customers do you have across all three months?
unique_customers_all_months = jan_feb_mar_union.dropDuplicates(["customer_id"])
print("Unique customers across Jan+Feb+Mar:", unique_customers_all_months.count())
unique_customers_all_months.orderBy("customer_id").show()

# =============================================================================
# TASK 2: Intersect (15 mins)
# =============================================================================

print("\n--- Task 2: Intersect ---")

# TODO 2a: Find customers who appear in BOTH January AND February
returning_jan_feb = jan_customers.intersect(feb_customers)
returning_jan_feb.show()

# TODO 2b: Verify the result makes sense - who are the returning customers?
print("Returning customers Jan->Feb:")
returning_jan_feb.select("customer_id", "name").orderBy("customer_id").show()

# =============================================================================
# TASK 3: Subtract/Except (15 mins)
# =============================================================================

print("\n--- Task 3: Subtract/Except ---")

# TODO 3a: Find customers in January who did NOT return in February
jan_not_feb = jan_customers.exceptAll(feb_customers)
jan_not_feb.show()

# TODO 3b: Find NEW customers in February (not in January)
feb_not_jan = feb_customers.exceptAll(jan_customers)
feb_not_jan.show()


# TODO 3c: Business question: What is the customer churn from Jan to Feb?
# Answer in a comment: chun is present in Jan but not in Feb


# =============================================================================
# TASK 4: Distinct and DropDuplicates (15 mins)
# =============================================================================

print("\n--- Task 4: Deduplication ---")

# Combined data with duplicates
all_data = jan_customers.union(feb_customers)

# TODO 4a: Use distinct() to remove exact duplicate rows
all_distinct = all_data.distinct()
all_distinct.show()

# TODO 4b: Use dropDuplicates() on email column only
# (Keep first occurrence of each email)
email_deduped = all_data.dropDuplicates(["email"])
email_deduped.show()

# TODO 4c: What is the difference between distinct() and dropDuplicates()?
# Answer in a comment: distinct removes duplicates based on all columns
# dropduplicates removes duplicates based only on the specified columns

# =============================================================================
# CHALLENGE: Data Reconciliation (20 mins)
# =============================================================================

print("\n--- Challenge: Data Reconciliation ---")

# System A data (source)
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300)
], ["id", "name", "price"])

# System B data (target)
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price difference!
    (4, "Product D", 400)   # New product!
], ["id", "name", "price"])

# TODO 5a: Find exact matches between source and target
matched = source.intersect(target)
matched.show()

# TODO 5b: Find records in source but not in target (or different)
source_only = source.exceptAll(target)
source_only.show()

# TODO 5c: Find records in target but not in source (or different)
target_only = target.exceptAll(source)
target_only.show()

# TODO 5d: Create a reconciliation report showing:
# - Matched count
# - Source-only count
# - Target-only count
report = spark.createDataFrame([
    ("matched", matched.count()),
    ("source_only_or_changed", source_only.count()),
    ("target_only_or_changed", target_only.count()),
],
["metric", "count"])
report.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()