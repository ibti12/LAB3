import os
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, sum as _sum, avg

# Supprimer les warnings
warnings.filterwarnings('ignore')

# Forcer Spark à utiliser Python du venv
os.environ["PYSPARK_PYTHON"] = r"C:\Users\pc\spark-ecommerce-lab\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\pc\spark-ecommerce-lab\venv\Scripts\python.exe"

# Créer la SparkSession
spark = SparkSession.builder.appName("Advanced E-Commerce Analysis").getOrCreate()
print("✓ SparkSession créée")

# Chemin des données
data_dir = "spark-data/ecommerce"
analysis_dir = os.path.join(data_dir, "analysis")
os.makedirs(analysis_dir, exist_ok=True)

# Charger les datasets existants
customers = spark.read.option("header", True).option("inferSchema", True).csv(os.path.join(data_dir, "customers.csv"))
products = spark.read.option("header", True).option("inferSchema", True).csv(os.path.join(data_dir, "products.csv"))
orders = spark.read.option("header", True).option("inferSchema", True).csv(os.path.join(data_dir, "orders.csv"))

# Vérifier si orderdetails.csv existe
orderdetails_path = os.path.join(data_dir, "orderdetails.csv")
orderdetails = None
if os.path.exists(orderdetails_path):
    orderdetails = spark.read.option("header", True).option("inferSchema", True).csv(orderdetails_path)
    print(f"OrderDetails columns: {orderdetails.columns}")

print("✓ Datasets loaded")
print(f"Customers columns: {customers.columns}")
print(f"Products columns: {products.columns}")
print(f"Orders columns: {orders.columns}")
if orderdetails:
    print(f"OrderDetails columns: {orderdetails.columns}")

# Joins pour obtenir dataset complet
if orderdetails:
    orders_full = (
        orderdetails
        .join(products, "productCode", "inner")
        .join(orders, "orderNumber", "inner")
        .join(customers, "customerNumber", "inner")
    )
else:
    orders_full = orders.join(customers, "customerNumber", "inner")

orders_full.show(5)

# Générer un résumé global
summary_data = [
    ("Total Customers", float(customers.count())),
    ("Total Products", float(products.count())),
    ("Total Orders", float(orders.count()))
]

if orderdetails:
    total_revenue = float(orders_full.agg(_sum("priceEach")).collect()[0][0])
    avg_order_value = float(
        orders_full.groupBy("orderNumber")
        .agg(_sum("priceEach").alias("orderValue"))
        .agg(avg("orderValue"))
        .collect()[0][0]
    )
    summary_data.append(("Total Revenue", total_revenue))
    summary_data.append(("Average Order Value", avg_order_value))

summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
summary_df.show()
summary_df.write.mode("overwrite").csv(os.path.join(analysis_dir, "summary"))

# Analyse exploratoire

# Clients par segment
df_clients_segment = customers.groupBy("customerSegment").count()
df_clients_segment.show()
df_clients_segment.write.mode("overwrite").csv(os.path.join(analysis_dir, "clients_by_segment"))

# Top pays par nombre de clients
df_top_countries = customers.groupBy("country").count().orderBy("count", ascending=False)
df_top_countries.show(10)
df_top_countries.write.mode("overwrite").csv(os.path.join(analysis_dir, "top_countries"))

# Commandes par statut
df_orders_status = orders.groupBy("status").count()
df_orders_status.show()
df_orders_status.write.mode("overwrite").csv(os.path.join(analysis_dir, "orders_by_status"))

# Commandes par méthode de paiement
df_orders_payment = orders.groupBy("paymentMethod").count()
df_orders_payment.show()
df_orders_payment.write.mode("overwrite").csv(os.path.join(analysis_dir, "orders_by_payment"))

# Statistiques sur le montant des commandes
orders.agg(
    _sum("totalAmount").alias("totalRevenue"),
    avg("totalAmount").alias("avgOrderValue")
).show()

# Produits par catégorie
df_products_category = products.groupBy("productCategory").count()
df_products_category.show()
df_products_category.write.mode("overwrite").csv(os.path.join(analysis_dir, "products_by_category"))

# Statistiques sur les prix
products.agg(
    _sum("buyPrice").alias("totalBuyPrice"),
    avg("buyPrice").alias("avgBuyPrice"),
    _sum("MSRP").alias("totalMSRP"),
    avg("MSRP").alias("avgMSRP")
).show()

# Analyse avancée si orderdetails existe
if orderdetails:
    df_revenue_by_product = orders_full.groupBy("productName").agg(_sum("priceEach").alias("revenue")).orderBy("revenue", ascending=False)
    df_revenue_by_product.show(10)
    df_revenue_by_product.write.mode("overwrite").csv(os.path.join(analysis_dir, "revenue_by_product"))

# ============================================================
# SECTION 9: BUSINESS QUESTIONS (Part C)
# ============================================================

print("\n" + "="*60)
print("BUSINESS QUESTIONS")
print("="*60)

# Question 1 - Country with highest total credit limit
print("\n--- Question 1: Country with highest total credit limit ---")
q1_result = (customers
    .groupBy("country")
    .agg(_sum("creditLimit").alias("totalCreditLimit"))
    .orderBy("totalCreditLimit", ascending=False)
    .limit(1)
)
q1_result.show()
q1_result.write.mode("overwrite").csv(os.path.join(analysis_dir, "q1_top_country_credit"))

# Question 2 - Most common order status
print("\n--- Question 2: Most common order status ---")
q2_result = (orders
    .groupBy("status")
    .count()
    .orderBy("count", ascending=False)
    .limit(1)
)
q2_result.show()
q2_result.write.mode("overwrite").csv(os.path.join(analysis_dir, "q2_most_common_status"))

# Question 3 - Product category with most stock
print("\n--- Question 3: Product category with most stock ---")
q3_result = (products
    .groupBy("productCategory")
    .agg(_sum("quantityInStock").alias("totalStock"))
    .orderBy("totalStock", ascending=False)
    .limit(1)
)
q3_result.show()
q3_result.write.mode("overwrite").csv(os.path.join(analysis_dir, "q3_category_most_stock"))

# Question 4 - Percentage of Enterprise customers
print("\n--- Question 4: Percentage of Enterprise customers ---")
from pyspark.sql.functions import col, when, count as spark_count

total_customers = customers.count()
enterprise_customers = customers.filter(col("customerSegment") == "Enterprise").count()
enterprise_percentage = (enterprise_customers / total_customers) * 100

q4_result = spark.createDataFrame([
    ("Total Customers", float(total_customers)),
    ("Enterprise Customers", float(enterprise_customers)),
    ("Enterprise Percentage", enterprise_percentage)
], ["Metric", "Value"])
q4_result.show()
q4_result.write.mode("overwrite").csv(os.path.join(analysis_dir, "q4_enterprise_percentage"))

print(f"Enterprise customers represent {enterprise_percentage:.2f}% of all customers")

# Question 5 - Distribution of orders by month
print("\n--- Question 5: Distribution of orders by month ---")
from pyspark.sql.functions import to_date, month as spark_month

# Convert orderDate to date type if it's a string
orders_with_month = orders.withColumn("orderDateParsed", to_date(col("orderDate"), "yyyy-MM-dd"))
orders_with_month = orders_with_month.withColumn("orderMonth", spark_month(col("orderDateParsed")))

q5_result = (orders_with_month
    .groupBy("orderMonth")
    .count()
    .orderBy("orderMonth")
)
q5_result.show(12)
q5_result.write.mode("overwrite").csv(os.path.join(analysis_dir, "q5_orders_by_month"))

print("\n" + "="*60)
print("BUSINESS QUESTIONS COMPLETED")
print("="*60 + "\n")

# Arrêter Spark
spark.stop()
print("✓ SparkSession arrêtée")