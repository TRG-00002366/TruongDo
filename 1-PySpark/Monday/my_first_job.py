from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

def main():

    #  Step 1: Create SparkSession
    spark = (
        SparkSession.builder
        .appName("MyFirstJob")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Step 2: YOUR CODE HERE - Create some data
        sales_data = [
            ("Laptop", "Electronics", 999.99, 5),
            ("Mouse", "Electronics", 29.99, 50),
            ("Desk", "Furniture", 199.99, 10),
            ("Chair", "Furniture", 149.99, 20),
            ("Monitor", "Electronics", 299.99, 15),
        ]
        # Create DataFrame with column names
        df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])

        # Show the DataFrame
        print("\nDataFrame:")
        df.show(truncate=False)

        # Count total records
        total_products = df.count()
        print(f"Total products: {total_products}\n")

        # Step 3: YOUR CODE HERE - Perform transformations
        df_with_revenue = df.withColumn("revenue",F.round(F.col("price") * F.col("quantity"), 2))

        print("Revenue per product:")
        df_with_revenue.show(truncate=False)

        # Filter by category (Electronics)
        electronics_df = df.filter(F.col("category") == "Electronics")
        print("Electronics only:")
        electronics_df.show(truncate=False)

        # Aggregate by category 
        revenue_by_category = (
            df_with_revenue
            .groupBy("category")
            .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
            .orderBy("category")
        )

        print("Revenue by category:")
        revenue_by_category.show(truncate=False)

    finally:
        # Step 5: Clean up
        spark.stop()

if __name__ == "__main__":
    main()