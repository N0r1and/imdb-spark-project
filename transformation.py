from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def q1_top10_rated_movies(dfs: dict) -> DataFrame:
    """
    Питання 1: Топ-10 найрейтинговіших фільмів з кількістю голосів більше 100 000.
    Використовує: filter, sort
    """
    print("\n" + "="*60)
    print("❓ Питання 1: Топ-10 найрейтинговіших фільмів (votes > 100k)")
    print("="*60)

    result = dfs["title_basics"] \
        .join(dfs["title_ratings"], on="tconst", how="inner") \
        .filter(F.col("numVotes") > 100000) \
        .filter(F.col("titleType") == "movie") \
        .select("primaryTitle", "startYear", "genres", "averageRating", "numVotes") \
        .orderBy(F.col("averageRating").desc()) \
        .limit(10)

    result.show(truncate=False)
    return result


def q2_top_genres_by_rating(dfs: dict) -> DataFrame:
    """
    Питання 2: Які жанри мають найвищий середній рейтинг?
    Використовує: filter, group by
    """
    print("\n" + "="*60)
    print("❓ Питання 2: Топ жанрів за середнім рейтингом")
    print("="*60)

    result = dfs["title_basics"] \
        .join(dfs["title_ratings"], on="tconst", how="inner") \
        .filter(F.col("numVotes") > 1000) \
        .filter(F.col("genres").isNotNull()) \
        .groupBy("genres") \
        .agg(
            F.round(F.avg("averageRating"), 2).alias("avgRating"),
            F.count("tconst").alias("movieCount")
        ) \
        .filter(F.col("movieCount") > 10) \
        .orderBy(F.col("avgRating").desc()) \
        .limit(15)

    result.show(truncate=False)
    return result


def q3_top_directors(dfs: dict) -> DataFrame:
    """
    Питання 3: Топ-10 режисерів за середнім рейтингом їх фільмів.
    Використовує: join, group by
    """
    print("\n" + "="*60)
    print("❓ Питання 3: Топ-10 режисерів за середнім рейтингом")
    print("="*60)

    result = dfs["title_crew"] \
        .join(dfs["title_ratings"], on="tconst", how="inner") \
        .join(dfs["title_basics"], on="tconst", how="inner") \
        .filter(F.col("titleType") == "movie") \
        .filter(F.col("numVotes") > 10000) \
        .filter(F.col("directors").isNotNull()) \
        .groupBy("directors") \
        .agg(
            F.round(F.avg("averageRating"), 2).alias("avgRating"),
            F.count("tconst").alias("movieCount")
        ) \
        .filter(F.col("movieCount") >= 3) \
        .join(dfs["name_basics"], dfs["title_crew"].directors == dfs["name_basics"].nconst, how="left") \
        .select("primaryName", "avgRating", "movieCount") \
        .orderBy(F.col("avgRating").desc()) \
        .limit(10)

    result.show(truncate=False)
    return result


def q4_movies_ranked_by_genre(dfs: dict) -> DataFrame:
    """
    Питання 4: Рейтинг фільмів всередині кожного жанру (місце кожного фільму).
    Використовує: window function
    """
    print("\n" + "="*60)
    print("❓ Питання 4: Місце кожного фільму в своєму жанрі (window function)")
    print("="*60)

    window_spec = Window \
        .partitionBy("genres") \
        .orderBy(F.col("averageRating").desc())

    result = dfs["title_basics"] \
        .join(dfs["title_ratings"], on="tconst", how="inner") \
        .filter(F.col("titleType") == "movie") \
        .filter(F.col("numVotes") > 50000) \
        .filter(F.col("genres").isNotNull()) \
        .withColumn("rankInGenre", F.rank().over(window_spec)) \
        .filter(F.col("rankInGenre") <= 3) \
        .select("genres", "rankInGenre", "primaryTitle", "averageRating", "numVotes") \
        .orderBy("genres", "rankInGenre")

    result.show(50, truncate=False)
    return result


def q5_most_active_actors(dfs: dict) -> DataFrame:
    """
    Питання 5: Актори які знялись у найбільшій кількості фільмів після 2000 року.
    Використовує: filter, join, group by
    """
    print("\n" + "="*60)
    print("❓ Питання 5: Топ-10 найактивніших акторів після 2000 року")
    print("="*60)

    result = dfs["title_principals"] \
        .filter(F.col("category").isin("actor", "actress")) \
        .join(dfs["title_basics"], on="tconst", how="inner") \
        .filter(F.col("titleType") == "movie") \
        .filter(F.col("startYear") >= 2000) \
        .groupBy("nconst") \
        .agg(F.count("tconst").alias("movieCount")) \
        .join(dfs["name_basics"], on="nconst", how="inner") \
        .select("primaryName", "movieCount") \
        .orderBy(F.col("movieCount").desc()) \
        .limit(10)

    result.show(truncate=False)
    return result


def q6_rating_by_decade(dfs: dict) -> DataFrame:
    """
    Питання 6: Середній рейтинг фільмів по десятиліттях з порівнянням з попереднім.
    Використовує: filter, window function
    """
    print("\n" + "="*60)
    print("❓ Питання 6: Середній рейтинг фільмів по десятиліттях")
    print("="*60)

    window_spec = Window.orderBy("decade")

    result = dfs["title_basics"] \
        .join(dfs["title_ratings"], on="tconst", how="inner") \
        .filter(F.col("titleType") == "movie") \
        .filter(F.col("numVotes") > 1000) \
        .filter(F.col("startYear").isNotNull()) \
        .withColumn("decade", (F.col("startYear") / 10).cast("int") * 10) \
        .groupBy("decade") \
        .agg(
            F.round(F.avg("averageRating"), 2).alias("avgRating"),
            F.count("tconst").alias("movieCount")
        ) \
        .withColumn("prevDecadeRating", F.lag("avgRating").over(window_spec)) \
        .withColumn("change", F.round(F.col("avgRating") - F.col("prevDecadeRating"), 2)) \
        .orderBy("decade")

    result.show(truncate=False)
    return result