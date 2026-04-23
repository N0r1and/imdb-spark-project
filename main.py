from pyspark.sql import SparkSession
from extraction import load_data
from preprocessing import (
    get_general_info,
    get_numeric_stats,
    cast_types,
    analyze_informativeness,
    analyze_missing_and_duplicates
)

DATA_PATH = r"C:\Users\PC\Desktop\imdb-data"

spark = SparkSession.builder \
    .appName("IMDB Project") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

dfs = load_data(spark, DATA_PATH)
get_general_info(dfs)
get_numeric_stats(dfs)
dfs = cast_types(dfs)
dfs = analyze_informativeness(dfs)
dfs = analyze_missing_and_duplicates(dfs)

print("\n✅ Попередня обробка завершена!")