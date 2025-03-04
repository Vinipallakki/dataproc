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

#====================================================
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

# Read CSV file from GCS
df = spark.read.csv("gs://vini123654789/sample_large_dataset.csv", header=True, inferSchema=True)

# Register DataFrame as SQL Table
df.createOrReplaceTempView("sales")

# Perform SQL Query
result_df = spark.sql("""
    SELECT *
    FROM sales
    WHERE age > 20
""")

# Write output to BigQuery
result_df.write \
    .format("bigquery") \
    .option("table", "nimble-courier-449405-f7.first.three") \
    .option("temporaryGcsBucket", "nimble-courier-449405-f7") \
    .mode("overwrite") \
    .save()

print("Data successfully written to BigQuery!")

# Stop Spark session
spark.stop()
#===========================================
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceSalesAnalysis").getOrCreate()

# Read CSV file from GCS
df = spark.read.csv("gs://vini123654789/sample_large_dataset.csv", header=True, inferSchema=True)

# Apply filter directly in PySpark
filtered_df = df.filter(df.age > 20)

# Write output to BigQuery
filtered_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "vini123654789") \
    .mode("overwrite") \
    .save("nimble-courier-449405-f7.first.three")

print("Data successfully written to BigQuery!")

# Stop Spark session
spark.stop()


#gcloud dataproc jobs submit pyspark gs://vini123654789/main.py     --cluster=my-dataproc-cluster     --region=us-central1     --properties spark.executor.memory=4g,spark.executor.cores=2     --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

