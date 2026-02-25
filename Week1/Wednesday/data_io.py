from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

# Load the CSV file
lines = sc.textFile("sales_data.csv")

# Skip header line
header = lines.first()
data = lines.filter(lambda line: line != header)

print(f"Header: {header}")
print(f"Data records: {data.count()}")
print(f"First record: {data.first()}")

def parse_record(line):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4])
    }

# Parse all records
parsed = data.map(parse_record)

# Show parsed data
for record in parsed.take(3):
    print(record)

# Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data
# YOUR CODE: Create sales_data_2.csv with more records
# YOUR CODE: Load all CSV files using wildcard pattern
# all_data = sc.textFile("sales_data*.csv")

# YOUR CODE: Use coalesce(1) before saveAsTextFile
# This creates a single output file instead of multiple parts
revenue.coalesce(1).saveAsTextFile("output/revenue_data")

all_data = sc.textFile("sales_data*.csv")

print(f"Total combined records: {all_data.count()}")