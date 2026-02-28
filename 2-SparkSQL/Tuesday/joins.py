"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, sum as spark_sum

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
inner_join = (
    customers.join(orders, on="customer_id", how="inner")
    .select(
        col("name").alias("customer_name"),
        col("order_id"),
        col("order_date"),
        col("amount")
    )
)
inner_join.show()

# TODO 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
matching_orders_count = customers.join(orders, on="customer_id", how="inner").count()
total_orders_count = orders.count()
print(f"Orders with matching customers: {matching_orders_count}")
print(f"Total orders: {total_orders_count}")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
left_join = (
    customers.join(orders, on="customer_id", how="left")
    .select(
        col("customer_id"),
        col("name").alias("customer_name"),
        col("state"),
        col("order_id"),
        col("order_date"),
        col("amount")
    )
)
left_join.show()
# Who has NOT placed any orders?
customers_no_orders = (
    left_join
    .filter(col("order_id").isNull())
    .select("customer_id", "customer_name", "state")
)
print("Customers with NO orders:")
customers_no_orders.show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
right_join = (
    customers.join(orders, on="customer_id", how="right")
    .select(
        col("customer_id"),
        col("order_id"),
        col("order_date"),
        col("amount"),
        col("name").alias("customer_name"),
        col("state")
    )
)
right_join.show()

# Which order has no matching customer?
orders_no_customer = (
    right_join
    .filter(col("customer_name").isNull())
    .select("order_id", "customer_id", "order_date", "amount")
)
print("Orders with NO matching customer:")
orders_no_customer.show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment: left join keeps all rows from customers table and matches orders when possible
#right join keeps all rows from orders and matches customers when possible
#

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
full_outer = (
    customers.join(orders, on="customer_id", how="full")
    .select(
        col("customer_id"),
        col("name").alias("customer_name"),
        col("state"),
        col("order_id"),
        col("order_date"),
        col("amount")
    )
)
full_outer.show()

# TODO 3b: Filter to show only rows where there is a mismatch
# (customer without order OR order without customer)
mismatches = full_outer.filter(col("customer_name").isNull() | col("order_id").isNull())
print("Mismatched rows (customer w/o order or order w/o customer):")
mismatches.show

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
customers_with_orders = customers.join(orders, on="customer_id", how="left_semi")
customers_with_orders.show()

# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers_without_orders = customers.join(orders, on="customer_id", how="left_anti")
customers_without_orders.show()

# TODO 4c: When would you use anti join in real data work?
# Answer in a comment: to find records in one dataset that have no match in another
#
#

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id

# TODO 5a: Join and then DROP the duplicate customer_id column
joined_expr = (
    customers.join(orders, customers["customer_id"] == orders["customer_id"], "inner")
)
joined_drop_dup = joined_expr.drop(orders["customer_id"])
joined_drop_dup.select(
    customers["customer_id"],
    customers["name"].alias("customer_name"),
    orders["order_id"],
    orders["amount"]
).show()

# TODO 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
c = customers.alias("c")
o = orders.alias("o")

alias_join = (
    c.join(o, col("c.customer_id") == col("o.customer_id"), "inner")
    .select(
        col("c.customer_id"),
        col("c.name").alias("customer_name"),
        col("c.state"),
        col("o.order_id"),
        col("o.order_date"),
        col("o.amount")
    )
)
alias_join.show()

# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
c = customers.alias("c")
o = orders.alias("o")
p = products.alias("p")

cust_orders_products = (
    c.join(o, col("c.customer_id") == col("o.customer_id"), "inner")
    .join(p, col("o.order_id") == col("p.order_id"), "left")
    .select(
        col("c.name").alias("customer_name"),
        col("o.order_id"),
        col("o.amount"),
        col("p.product_name")
    )
)
cust_orders_products.show()
# TODO 6b: What kind of join should you use when some orders might not have products?
# Left join from orders to products 

# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum
total_spend = (
    customers.join(orders, on="customer_id", how="inner")
    .groupBy("customer_id", "name")
    .agg(spark_sum(col("amount")).alias("total_spent"))
    .orderBy(col("total_spent").desc())
)
total_spend.show()

# TODO 7b: Find customers from CA who placed orders > $150
ca_big_orders = (
    customers.join(orders, on="customer_id", how="inner")
    .filter((col("state") == "CA") & (col("amount") > 150))
    .select(
        col("customer_id"),
        col("name").alias("customer_name"),
        col("state"),
        col("order_id"),
        col("amount")
    )
)
ca_big_orders.show()

# TODO 7c: Find orders without valid product information
# (anti join pattern)
orders_without_products = (
    orders.join(products, on="order_id", how="left_anti")
)
orders_without_products.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()