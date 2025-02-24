# Spark SQL Query for Sales Analysis

## Overview
This project uses Apache Spark SQL to analyze sales data by calculating the total sales amount and sorting the results in descending order.

## Prerequisites
- Apache Spark installed
- PySpark library
- A dataset named `sales` containing at least the following columns:
  - `id`
  - `category`
  - `price`
  - `quantity`

## SQL Query
The following SQL query is executed on the `sales` table:

```sql
SELECT *, (price * quantity) AS total_sales
FROM sales
ORDER BY total_sales DESC;
```

### Explanation
- `SELECT *` ensures that all columns are included in the result.
- `(price * quantity) AS total_sales` calculates the total sales for each row.
- `ORDER BY total_sales DESC` sorts the records in descending order of `total_sales`.

## Usage
1. Load your sales data into a Spark DataFrame.
2. Register the DataFrame as a temporary table:
   ```python
   sales_df.createOrReplaceTempView("sales")
   ```
3. Run the SQL query using:
   ```python
   result_df = spark.sql("""
       SELECT *, (price * quantity) AS total_sales
       FROM sales
       ORDER BY total_sales DESC
   """)
   ```
4. Show or save the results:
   ```python
   result_df.show()
   result_df.write.csv("output.csv", header=True)
   ```

## Output Example
| id | category | price | quantity | total_sales |
|----|----------|--------|---------|------------|
| 3  | Books    | 200    | 5       | 1000       |
| 1  | Electronics | 500  | 2       | 1000       |
| 2  | Clothing | 100    | 8       | 800        |

## Notes
- Ensure your DataFrame has all required columns before executing the query.
- Modify the query if additional aggregations or filters are needed.

## License
This project is open-source under the MIT License.

