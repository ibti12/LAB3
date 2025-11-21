import glob

# Liste tous les fichiers Spark part-xxxxx.csv
files = glob.glob("spark-data/ecommerce/summary/part-*.csv")

output_path = "spark-data/ecommerce/summary/summary.csv"

with open(output_path, "w", encoding="utf-8") as out:
    for f in files:
        with open(f, "r", encoding="utf-8") as fin:
            content = fin.read().strip()
            if content:            # Évite les fichiers vides
                out.write(content + "\n")

print("✓ summary.csv created:", output_path)
