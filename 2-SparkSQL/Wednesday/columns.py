"""
Exercise: Column Management
===========================
Week 2, Wednesday

Practice adding, removing, and transforming columns on product inventory data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, concat, concat_ws,
    split, substring, regexp_replace, coalesce, current_date
)

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Columns").master("local[*]").getOrCreate()

# Product inventory data (messy data for cleaning!)
inventory = spark.createDataFrame([
    (1, "  LAPTOP pro  ", "Electronics", 999.99, 50, None),
    (2, "  phone X ", "Electronics", 799.99, 100, "NY"),
    (3, "Winter JACKET", "Clothing", 149.99, 200, "CA"),
    (4, " running shoes ", "Clothing", 89.99, None, "TX"),
    (5, "coffee MAKER", "Home", 49.99, 75, None),
    (6, "  Desk Lamp  ", "Home", 29.99, 120, "NY")
], ["product_id", "product_name", "category", "price", "quantity", "warehouse"])

print("=== Exercise: Column Management ===")
print("\nRaw Inventory Data:")
inventory.show(truncate=False)

# =============================================================================
# TASK 1: String Cleaning (20 mins)
# =============================================================================

print("\n--- Task 1: String Cleaning ---")

# TODO 1a: Clean product_name: trim whitespace, convert to title case
# HINT: trim() removes whitespace, initcap() for title case
cleaned_name = concat(
    upper(substring(trim(col("product_name")), 1, 1)),
    lower(substring(trim(col("product_name")), 2, 1000))
)


# TODO 1b: Standardize category to lowercase
t1 = (
    inventory
    .withColumn("product_name", cleaned_name)
    .withColumn("category", lower(col("category")))
)


# TODO 1c: Create a "product_code" column by:
# - Taking first 3 letters of category (uppercase)
# - Adding the product_id
# - Example: "ELE-1" for Electronics product 1
t1 = t1.withColumn("product_code",
    concat(upper(substring(col("category"), 1, 3)), lit("-"), col("product_id").cast("string"))
)
t1.show()
# =============================================================================
# TASK 2: Handling Nulls (15 mins)
# =============================================================================

print("\n--- Task 2: Handling Nulls ---")

# TODO 2a: Replace null warehouse with "CENTRAL"
t2 = (t1
    .withColumn("warehouse", coalesce(col("warehouse"), lit("CENTRAL")))


# TODO 2b: Replace null quantity with 0
    .withColumn("quantity", coalesce(col("quantity"), lit(0)))

# TODO 2c: Create an "in_stock" boolean column (quantity > 0 or not null)
    .withColumn("in_stock", col("quantity") > 0)
)
t2.show()

# =============================================================================
# TASK 3: Calculated Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Calculated Columns ---")

# TODO 3a: Add "inventory_value" = price * quantity (handle nulls!)
t3 = (t2
    .withColumn("inventory_value", col("price") * col("quantity"))

# TODO 3b: Add "price_tier" based on price:
# - "Budget" if price < 50
# - "Mid" if 50 <= price < 200
# - "Premium" if price >= 200
    .withColumn(
        "price_tier",
        when(col("price") < 50, lit("Budget"))
        .when((col("price") >= 50) & (col("price") < 200), lit("Mid"))
        .otherwise(lit("Premium"))
    )

# TODO 3c: Add "last_updated" column with today's date
    .withColumn("last_update", current_date())
)
t3.show()
# =============================================================================
# TASK 4: Removing and Renaming (10 mins)
# =============================================================================

print("\n--- Task 4: Removing and Renaming ---")

# TODO 4a: Drop the "warehouse" column
t4 = (t3
    .drop("warehouse")

# TODO 4b: Rename columns:
# - product_id -> id
# - product_name -> name
    .withColumnRenamed("product_id", "id")
    .withColumnRenamed("product_name", "name")
)
t4.show()
# =============================================================================
# TASK 5: Complete Data Pipeline (25 mins)
# =============================================================================

print("\n--- Task 5: Complete Data Pipeline ---")

# Create a clean, analysis-ready version of the data:
# 1. Clean product_name (trim, title case)
# 2. Fill null warehouse with "CENTRAL"
# 3. Fill null quantity with 0
# 4. Add inventory_value column
# 5. Add price_tier column
# 6. Add last_updated column
# 7. Rename product_id to id, product_name to name
# 8. Drop warehouse column
# 9. Order columns: id, name, category, price, quantity, inventory_value, price_tier, last_updated
name_clean = concat(
    upper(substring(trim(col("product_name")), 1, 1)),
    lower(substring(trim(col("product_name")), 2, 1000))
)
clean_inventory = (inventory  # Your pipeline here
        .withColumn("product_name", name_clean)
        .withColumn("warehouse", coalesce(col("warehouse"), lit("CENTRAL")))
        .withColumn("quantity", coalesce(col("quantity"), lit(0)))
        .withColumn("category", lower(col("category")))
        .withColumn("inventory_value", col("price") * col("quantity"))
        .withColumn("price_tier", when(col("price") < 50, lit("Budget"))
        .when((col("price") >= 50) & (col("price") < 200), lit("Mid"))
        .otherwise(lit("Premium"))
        )
        .withColumn("last_update", current_date())
        .withColumnRenamed("product_id", "id")
        .withColumnRenamed("product_name", "name")
        .drop("warehouse")
        .select("id", "name", "category", "price", "quantity", "inventory_value", "price_tier", "last_update")
)
clean_inventory.show()


# =============================================================================
# CHALLENGE: Extract and Parse (15 mins)
# =============================================================================

print("\n--- Challenge: String Parsing ---")

# Product descriptions
descriptions = spark.createDataFrame([
    ("Widget A - Size: Large, Color: Blue",),
    ("Gadget B - Size: Medium, Color: Red",),
    ("Tool C - Size: Small, Color: Green",)
], ["description"])

# TODO 6a: Extract just the product name (before the dash)
parsed = (descriptions
    .withColumn("product_name", trim(split(col("description"), "-").getItem(0)))
    

# TODO 6b: Extract the size value
    .withColumn("size", trim(regexp_replace(split(col("description"), "Size:").getItem(1), ",.*", "")))

# TODO 6c: Extract the color value
    .withColumn("color", trim(split(col("description"), "Color:").getItem(1)))
)
parsed.show()
# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()