from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

# Read CSV file from GCS
df = spark.read.csv("gs://nimble-courier-449405-f7/ecommerce.csv", header=True, inferSchema=True)

# Register DataFrame as SQL Table
df.createOrReplaceTempView("sales")

# Perform SQL Query
result_df = spark.sql("""
    SELECT *, (price * quantity) AS total_sales
    FROM sales
    ORDER BY total_sales DESC
""")

# Write output to BigQuery
result_df.write \
    .format("bigquery") \
    .option("table", "nimble-courier-449405-f7.first.sales_summary_large") \
    .option("temporaryGcsBucket", "nimble-courier-449405-f7") \
    .mode("overwrite") \
    .save()

print("Data successfully written to BigQuery!")

# Stop Spark session
spark.stop()

