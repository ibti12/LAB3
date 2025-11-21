import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month

# Forcer Spark à utiliser Python du venv

os.environ["PYSPARK_PYTHON"] = r"C:\Users\pc\spark-ecommerce-lab\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\pc\spark-ecommerce-lab\venv\Scripts\python.exe"

# Créer la SparkSession

spark = SparkSession.builder.appName("E-Commerce Data Exploration").getOrCreate()
print("✓ SparkSession créée")

# Définir le chemin des fichiers CSV

data_dir = "spark-data/ecommerce"
customers_csv = os.path.join(data_dir, "customers.csv")
products_csv = os.path.join(data_dir, "products.csv")
orders_csv = os.path.join(data_dir, "orders.csv")

# Charger les CSV

customers = spark.read.option("header", True).option("inferSchema", True).csv(customers_csv)
products = spark.read.option("header", True).option("inferSchema", True).csv(products_csv)
orders = spark.read.option("header", True).option("inferSchema", True).csv(orders_csv)
print("✓ CSV chargés")

# Afficher le schéma

print("Customers Schema:")
customers.printSchema()
print("Products Schema:")
products.printSchema()
print("Orders Schema:")
orders.printSchema()

# Afficher quelques données

print("Top 5 Customers:")
customers.show(5)
print("Top 5 Products:")
products.show(5)
print("Top 5 Orders:")
orders.show(5)

# Statistiques de base

total_customers = customers.count()
total_products = products.count()
total_orders = orders.count()

print(f"Nombre total de clients: {total_customers}")
print(f"Nombre total de produits: {total_products}")
print(f"Nombre total de commandes: {total_orders}")

# Nombre de commandes par mois

orders_with_month = orders.withColumn("order_month", month("orderDate"))
orders_per_month = orders_with_month.groupBy("order_month").count().orderBy("order_month")
orders_per_month.show(12)

# Créer un DataFrame de résumé avec types uniformes (float)

summary_data = [
("Nombre total de clients", float(total_customers)),
("Nombre total de produits", float(total_products)),
("Nombre total de commandes", float(total_orders))
]
summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
summary_df.show()

# Sauvegarder le résumé dans un CSV

summary_output = os.path.join(data_dir, "summary")
summary_df.write.mode("overwrite").csv(summary_output)
print(f"✓ Résumé sauvegardé dans {summary_output}")

# Arrêter Spark

spark.stop()
print("✓ SparkSession arrêtée")
