from pyspark.sql import SparkSession
from extraction import load_data
from preprocessing import (
    get_general_info,
    get_numeric_stats,
    cast_types,
    analyze_informativeness,
    analyze_missing_and_duplicates
)
from transformation import (
    q1_top10_rated_movies,
    q2_top_genres_by_rating,
    q3_top_directors,
    q4_movies_ranked_by_genre,
    q5_most_active_actors,
    q6_rating_by_decade
)

DATA_PATH = r"C:\Users\PC\Desktop\imdb-data"

spark = SparkSession.builder \
    .appName("IMDB Project") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Етап видобування
dfs = load_data(spark, DATA_PATH)

# Етап попередньої обробки
get_general_info(dfs)
get_numeric_stats(dfs)
dfs = cast_types(dfs)
dfs = analyze_informativeness(dfs)
dfs = analyze_missing_and_duplicates(dfs)
print("\n✅ Попередня обробка завершена!")

# Етап трансформації
print("\n" + "="*60)
print("🔁 ЕТАП ТРАНСФОРМАЦІЇ")
print("="*60)

q1_top10_rated_movies(dfs)
q2_top_genres_by_rating(dfs)
q3_top_directors(dfs)
q4_movies_ranked_by_genre(dfs)
q5_most_active_actors(dfs)
q6_rating_by_decade(dfs)

print("\n✅ Трансформація завершена!")