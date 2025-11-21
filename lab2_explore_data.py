import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, sum as _sum, avg, min as _min, max as _max,
    month, to_date, lit, round as _round
)

# Configuration pour Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("=" * 80)
print("LAB 1.2: E-COMMERCE DATA EXPLORATION")
print("=" * 80)

# ============================================================================
# SECTION 1: CREATE SPARK SESSION
# ============================================================================
print("\n[1] Creating SparkSession...")
spark = (
    SparkSession.builder
    .appName("Day1-DataExploration")
    .master("local[*]")  # Mode local pour Windows
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
print("[OK] SparkSession created successfully")

# ============================================================================
# SECTION 2: LOAD CSV DATASETS
# ============================================================================
print("\n[2] Loading CSV datasets...")

customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/customers.csv")
    .cache()
)
print("[OK] Loaded customers.csv")

products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/products.csv")
    .cache()
)
print("[OK] Loaded products.csv")

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/orders.csv")
    .cache()
)
print("[OK] Loaded orders.csv")

# ============================================================================
# SECTION 3: INSPECT SCHEMAS
# ============================================================================
print("\n[3] Inspecting schemas...")
print("\n--- CUSTOMERS Schema ---")
customers.printSchema()

print("\n--- PRODUCTS Schema ---")
products.printSchema()

print("\n--- ORDERS Schema ---")
orders.printSchema()

# ============================================================================
# SECTION 4: BASIC STATISTICS (SIZE)
# ============================================================================
print("\n[4] Computing basic statistics...")
total_customers = customers.count()
total_products = products.count()
total_orders = orders.count()

print(f"Total number of customers: {total_customers}")
print(f"Total number of products: {total_products}")
print(f"Total number of orders: {total_orders}")

# ============================================================================
# SECTION 5: DATA PREVIEW
# ============================================================================
print("\n[5] Data preview (first 5 rows)...")
print("\n--- CUSTOMERS ---")
customers.show(5, truncate=False)

print("\n--- PRODUCTS ---")
products.show(5, truncate=False)

print("\n--- ORDERS ---")
orders.show(5, truncate=False)

# ============================================================================
# SECTION 6: DATA QUALITY CHECKS
# ============================================================================
print("\n[6] Data quality checks...")

# Null checks for customers
print("\n--- NULL VALUES in CUSTOMERS ---")
customers.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in customers.columns
]).show(vertical=True)

# Null checks for orders
print("\n--- NULL VALUES in ORDERS ---")
orders.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in orders.columns
]).show(vertical=True)

# Duplicate checks
print("\n--- DUPLICATE CHECKS ---")
customers_distinct = customers.select("customerNumber").distinct().count()
customers_duplicates = total_customers - customers_distinct
print(f"Customer duplicates: {customers_duplicates}")

orders_distinct = orders.select("orderNumber").distinct().count()
orders_duplicates = total_orders - orders_distinct
print(f"Order duplicates: {orders_duplicates}")

# ============================================================================
# SECTION 7: EXPLORATORY ANALYSIS
# ============================================================================
print("\n[7] Exploratory analysis...")

print("\n--- Customers by Segment ---")
customers.groupBy("customerSegment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("\n--- Top 10 Countries by Customer Count ---")
customers.groupBy("country") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

print("\n--- Orders by Status ---")
orders.groupBy("status") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("\n--- Orders by Payment Method ---")
orders.groupBy("paymentMethod") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("\n--- Products by Category ---")
products.groupBy("productCategory") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# ============================================================================
# SECTION 8: NUMERICAL ANALYSIS
# ============================================================================
print("\n[8] Numerical analysis...")

print("\n--- Order Amount Statistics ---")
orders.select(
    count("totalAmount").alias("order_count"),
    _min("totalAmount").alias("min_amount"),
    _max("totalAmount").alias("max_amount"),
    _round(avg("totalAmount"), 2).alias("avg_amount"),
    _round(_sum("totalAmount"), 2).alias("total_revenue")
).show()

print("\n--- Credit Limit Statistics by Segment ---")
customers.groupBy("customerSegment") \
    .agg(
        count("creditLimit").alias("count"),
        _round(avg("creditLimit"), 2).alias("avg_credit"),
        _round(_max("creditLimit"), 2).alias("max_credit")
    ) \
    .orderBy(col("avg_credit").desc()) \
    .show()

print("\n--- Product Price Statistics ---")
products.select(
    _round(_min("buyPrice"), 2).alias("min_buyPrice"),
    _round(_max("buyPrice"), 2).alias("max_buyPrice"),
    _round(avg("buyPrice"), 2).alias("avg_buyPrice"),
    _round(_min("MSRP"), 2).alias("min_MSRP"),
    _round(_max("MSRP"), 2).alias("max_MSRP"),
    _round(avg("MSRP"), 2).alias("avg_MSRP")
).show()

# ============================================================================
# SECTION 9: SUMMARY REPORT EXPORT
# ============================================================================
print("\n[9] Creating summary report...")

total_revenue = orders.select(_sum("totalAmount")).collect()[0][0]
avg_order_value = orders.select(avg("totalAmount")).collect()[0][0]

summary_data = [
    ("Total Customers", total_customers),
    ("Total Products", total_products),
    ("Total Orders", total_orders),
    ("Total Revenue", round(total_revenue, 2)),
    ("Average Order Value", round(avg_order_value, 2))
]

summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
summary_df.show()

summary_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("spark-data/ecommerce/summary/")

print("[OK] Summary report saved to spark-data/ecommerce/summary/")

# ============================================================================
# SECTION 10: BUSINESS QUESTIONS
# ============================================================================
print("\n" + "=" * 80)
print("BUSINESS QUESTIONS")
print("=" * 80)

# Question 1: Country with highest total credit limit
print("\n[Q1] Country with highest total credit limit:")
customers.groupBy("country") \
    .agg(_round(_sum("creditLimit"), 2).alias("total_credit_limit")) \
    .orderBy(col("total_credit_limit").desc()) \
    .show(1)

# Question 2: Most common order status
print("\n[Q2] Most common order status:")
orders.groupBy("status") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(1)

# Question 3: Product category with most stock
print("\n[Q3] Product category with most stock:")
products.groupBy("productCategory") \
    .agg(_sum("quantityInStock").alias("total_stock")) \
    .orderBy(col("total_stock").desc()) \
    .show(1)

# Question 4: Percentage of Enterprise customers
print("\n[Q4] Percentage of Enterprise customers:")
enterprise_count = customers.filter(col("customerSegment") == "Enterprise").count()
percentage = (enterprise_count / total_customers) * 100
print(f"Enterprise customers: {enterprise_count}")
print(f"Total customers: {total_customers}")
print(f"Percentage: {percentage:.2f}%")

# Display as DataFrame
spark.createDataFrame([
    ("Total Customers", total_customers),
    ("Enterprise Customers", enterprise_count),
    ("Percentage", round(percentage, 2))
], ["Metric", "Value"]).show()

# Question 5: Distribution of orders by month
print("\n[Q5] Distribution of orders by month:")
orders_with_date = orders.withColumn("orderDate_converted", to_date(col("orderDate"), "yyyy-MM-dd"))
orders_with_date.withColumn("order_month", month(col("orderDate_converted"))) \
    .groupBy("order_month") \
    .count() \
    .orderBy("order_month") \
    .show(12)

# ============================================================================
# END
# =========