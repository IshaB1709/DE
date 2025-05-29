"""
PySpark Script: Handling Missing Values in Sales Data

- Drops rows where 'product' is null
- Fills missing 'quantity' with 1
- Replaces missing 'price' with the mean price
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

# Create Spark session
spark = SparkSession.builder.appName("Handle Missing Values").getOrCreate()

# Sample sales data
data = [
    (1, "Laptop", 1000, 5),
    (2, "Mouse", None, None),
    (3, "Keyboard", 50, 2),
    (4, "Monitor", 200, None),
    (5, None, 500, None),
]
columns = ["product_id", "product", "price", "quantity"]

# Create DataFrame
df_data = spark.createDataFrame(data, columns)

# Drop rows where the product column is null
df_products = df_data.na.drop(subset=["product"])

# Fill missing values in the quantity column with 1
df_qty = df_products.fillna(1, subset=["quantity"])

# Calculate mean price (excluding nulls)
mean_price = df_products.select(mean(col("price"))).collect()[0][0]

# Replace missing price values with the mean price
df_cleaned = df_qty.fillna({"price": mean_price})

# Show final cleaned DataFrame
df_cleaned.show()
