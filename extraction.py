from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType
)

# ==============================
# СХЕМИ
# ==============================

title_basics_schema = StructType([
    StructField("tconst",         StringType(),  True),
    StructField("titleType",      StringType(),  True),
    StructField("primaryTitle",   StringType(),  True),
    StructField("originalTitle",  StringType(),  True),
    StructField("isAdult",        StringType(),  True),
    StructField("startYear",      StringType(),  True),
    StructField("endYear",        StringType(),  True),
    StructField("runtimeMinutes", StringType(),  True),
    StructField("genres",         StringType(),  True),
])

title_ratings_schema = StructType([
    StructField("tconst",        StringType(), True),
    StructField("averageRating", FloatType(),  True),
    StructField("numVotes",      IntegerType(), True),
])

title_crew_schema = StructType([
    StructField("tconst",    StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers",   StringType(), True),
])

title_akas_schema = StructType([
    StructField("titleId",        StringType(),  True),
    StructField("ordering",       IntegerType(), True),
    StructField("title",          StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("language",       StringType(),  True),
    StructField("types",          StringType(),  True),
    StructField("attributes",     StringType(),  True),
    StructField("isOriginalTitle", StringType(), True),
])

title_episode_schema = StructType([
    StructField("tconst",        StringType(),  True),
    StructField("parentTconst",  StringType(),  True),
    StructField("seasonNumber",  StringType(),  True),
    StructField("episodeNumber", StringType(),  True),
])

title_principals_schema = StructType([
    StructField("tconst",     StringType(),  True),
    StructField("ordering",   IntegerType(), True),
    StructField("nconst",     StringType(),  True),
    StructField("category",   StringType(),  True),
    StructField("job",        StringType(),  True),
    StructField("characters", StringType(),  True),
])

name_basics_schema = StructType([
    StructField("nconst",            StringType(), True),
    StructField("primaryName",       StringType(), True),
    StructField("birthYear",         StringType(), True),
    StructField("deathYear",         StringType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles",    StringType(), True),
])


# ==============================
# ФУНКЦІЯ ЗАВАНТАЖЕННЯ
# ==============================

def load_data(spark: SparkSession, data_path: str) -> dict:
    """
    Зчитує всі файли IMDB датасету у DataFrame.

    Args:
        spark: активна SparkSession
        data_path: шлях до папки з .tsv файлами

    Returns:
        Словник з DataFrame для кожного файлу
    """

    def read_tsv(filename, schema):
        return spark.read \
            .option("header", "true") \
            .option("sep", "\t") \
            .option("nullValue", "\\N") \
            .schema(schema) \
            .csv(f"{data_path}/{filename}")

    dataframes = {
        "title_basics":     read_tsv("title.basics.tsv",     title_basics_schema),
        "title_ratings":    read_tsv("title.ratings.tsv",    title_ratings_schema),
        "title_crew":       read_tsv("title.crew.tsv",       title_crew_schema),
        "title_akas":       read_tsv("title.akas.tsv",       title_akas_schema),
        "title_episode":    read_tsv("title.episode.tsv",    title_episode_schema),
        "title_principals": read_tsv("title.principals.tsv", title_principals_schema),
        "name_basics":      read_tsv("name.basics.tsv",      name_basics_schema),
    }

    return dataframes