from pyspark.sql import SparkSession
from extraction import load_data

DATA_PATH = r"C:\Users\PC\Desktop\imdb-data"  # ← свій шлях

spark = SparkSession.builder \
    .appName("IMDB Project") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

dfs = load_data(spark, DATA_PATH)

for name, df in dfs.items():
    print(f"\n{'='*40}")
    print(f"📄 {name}")
    print(f"{'='*40}")
    df.printSchema()
    df.show(5, truncate=True)