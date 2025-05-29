data = [ (1, "Laptop", 1000, 5), (2, "Mouse", None, None), (3, "Keyboard", 50, 2), (4, "Monitor", 200, None), (5, None, 500, None), ] 
columns = ["product_id", "product", "price", "quantity"]
df_data = spark.createDataFrame(data, columns)

*Handle Missing Data Efficiently
You have a DataFrame containing sales data. Write a PySpark script to:
Replace missing values in the price column with the mean price.
Drop rows where the product column is null.
Fill missing values in the quantity column with 1.*

  #Drop rows where the product column is null.
df_products=df_data.na.drop(subset=["product"])

#Fill missing values in the quantity column with 1.

df_qty=df_products.fillna(1,subset=["quantity"])

from pyspark.sql.functions import col, mean
#Replace missing values in the price column with the mean price.

mean_price = df_data.select(mean(col("price"))).collect()[0][0]
df_cleaned = df_qty.fillna({"price": mean_price})

df_cleaned.show()
