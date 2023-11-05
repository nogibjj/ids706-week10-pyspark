from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def calculate_total_sales(amount, unit_price):
    return amount * unit_price

if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("SalesDataProcessing").getOrCreate()

    # Define schema of the dataset
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Amount", IntegerType(), True),
        StructField("Unit_Price", FloatType(), True),
        StructField("Customer_ID", StringType(), True)
    ])

    # Create a static dataset in the code
    data = [
        ("2023-01-01", "Product_1", 2, 100.0, "Customer_1"),
        ("2023-01-02", "Product_2", 1, 300.0, "Customer_2"),
        ("2023-01-03", "Product_3", 3, 120.0, "Customer_3"),
        ("2023-01-04", "Product_4", 4, 200.0, "Customer_4"),
        # ... add more rows as needed
    ]

    # Create DataFrame
    sales_data = spark.createDataFrame(data, schema=schema)

    # Register the DataFrame as a SQL temporary view
    sales_data.createOrReplaceTempView("sales")

    # Calculate the total sales using Spark SQL
    spark.sql(
        "SELECT Product, SUM(Amount * Unit_Price) AS Total_Sales "
        "FROM sales "
        "GROUP BY Product"
    ).createOrReplaceTempView("product_sales")

    # Get the top 10 selling products
    top_selling_products = spark.sql(
        "SELECT Product, Total_Sales "
        "FROM product_sales "
        "ORDER BY Total_Sales DESC "
        "LIMIT 10"
    )
    top_selling_products.show()

    # Perform the data transformation to categorize sales into groups
    sales_data = sales_data.withColumn(
        "Sale_Category",
        when(calculate_total_sales(col("Amount"), col("Unit_Price")) < 100, "Small")
        .otherwise("Large")
    )

    # Show the transformed DataFrame
    sales_data.show()

    # Stop the SparkSession
    spark.stop()
