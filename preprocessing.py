from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType


def get_general_info(dfs: dict):
    """
    Загальна статистична інформація про датасет.
    """
    print("\n" + "="*60)
    print("📊 ЗАГАЛЬНА ІНФОРМАЦІЯ ПРО ДАТАСЕТ")
    print("="*60)

    for name, df in dfs.items():
        print(f"\n📄 {name}:")
        print(f"   Кількість рядків : {df.count()}")
        print(f"   Кількість колонок: {len(df.columns)}")
        print(f"   Колонки          : {', '.join(df.columns)}")


def get_numeric_stats(dfs: dict):
    """
    Статистика по числових ознаках.
    """
    print("\n" + "="*60)
    print("📈 СТАТИСТИКА ЧИСЛОВИХ ОЗНАК")
    print("="*60)

    print("\n📄 title_ratings:")
    dfs["title_ratings"].describe().show()

    print("\n📄 title_basics (runtimeMinutes після конвертації):")
    dfs["title_basics"] \
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").try_cast(IntegerType())) \
        .select("runtimeMinutes") \
        .describe() \
        .show()


def cast_types(dfs: dict) -> dict:
    """
    Приводить ознаки до потрібних типів.
    """
    print("\n" + "="*60)
    print("🔄 КОНВЕРТАЦІЯ ТИПІВ")
    print("="*60)

    # Схеми ДО конвертації
    print("\n❌ title_basics ДО конвертації:")
    dfs["title_basics"].printSchema()

    print("\n❌ title_episode ДО конвертації:")
    dfs["title_episode"].printSchema()

    print("\n❌ name_basics ДО конвертації:")
    dfs["name_basics"].printSchema()

    # title_basics — числові поля з рядків в числа
    dfs["title_basics"] = dfs["title_basics"] \
        .withColumn("startYear",      F.col("startYear").try_cast(IntegerType())) \
        .withColumn("endYear",        F.col("endYear").try_cast(IntegerType())) \
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").try_cast(IntegerType())) \
        .withColumn("isAdult",        F.col("isAdult").try_cast(IntegerType()))

    dfs["title_episode"] = dfs["title_episode"] \
        .withColumn("seasonNumber",  F.col("seasonNumber").try_cast(IntegerType())) \
        .withColumn("episodeNumber", F.col("episodeNumber").try_cast(IntegerType()))

    dfs["name_basics"] = dfs["name_basics"] \
        .withColumn("birthYear", F.col("birthYear").try_cast(IntegerType())) \
        .withColumn("deathYear", F.col("deathYear").try_cast(IntegerType()))

    # Схеми ПІСЛЯ конвертації
    print("\n✅ title_basics ПІСЛЯ конвертації:")
    dfs["title_basics"].printSchema()

    print("\n✅ title_episode ПІСЛЯ конвертації:")
    dfs["title_episode"].printSchema()

    print("\n✅ name_basics ПІСЛЯ конвертації:")
    dfs["name_basics"].printSchema()

    return dfs


def analyze_informativeness(dfs: dict) -> dict:
    """
    Аналіз інформативності ознак — видалення непотрібних колонок.
    """
    print("\n" + "="*60)
    print("🔍 АНАЛІЗ ІНФОРМАТИВНОСТІ ОЗНАК")
    print("="*60)

    # title_basics — перевіряємо скільки разів primaryTitle == originalTitle
    same_title = dfs["title_basics"] \
        .filter(F.col("primaryTitle") == F.col("originalTitle")) \
        .count()
    total = dfs["title_basics"].count()
    print(f"\n📄 title_basics:")
    print(f"   primaryTitle == originalTitle: {same_title} з {total} ({round(same_title/total*100, 1)}%)")
    print(f"   → Залишаємо обидві колонки (різниця є для не-англійських фільмів)")

    # title_akas — колонка 'attributes' майже завжди NULL
    akas_total = dfs["title_akas"].count()
    akas_null = dfs["title_akas"].filter(F.col("attributes").isNull()).count()
    print(f"\n📄 title_akas:")
    print(f"   'attributes' NULL: {akas_null} з {akas_total} ({round(akas_null/akas_total*100, 1)}%)")
    print(f"   → Видаляємо колонку 'attributes' як неінформативну")
    dfs["title_akas"] = dfs["title_akas"].drop("attributes")

    # name_basics — колонка 'deathYear'
    names_total = dfs["name_basics"].count()
    death_null = dfs["name_basics"].filter(F.col("deathYear").isNull()).count()
    print(f"\n📄 name_basics:")
    print(f"   'deathYear' NULL: {death_null} з {names_total} ({round(death_null/names_total*100, 1)}%)")
    print(f"   → Залишаємо (інформативна для живих і померлих)")

    return dfs


def analyze_missing_and_duplicates(dfs: dict) -> dict:
    """
    Аналіз пропущених значень та дублікатів.
    """
    print("\n" + "="*60)
    print("🔎 ПРОПУЩЕНІ ЗНАЧЕННЯ ТА ДУБЛІКАТИ")
    print("="*60)

    # Великі таблиці — distinct().count() потребує забагато пам'яті
    large_tables = {"title_principals", "title_akas"}

    for name, df in dfs.items():
        print(f"\n📄 {name}:")

        # Пропущені значення по кожній колонці
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ])
        null_counts.show()

        # Дублікати
        if name in large_tables:
            print(f"   Дублікати: пропускаємо ({name} занадто велика для повної перевірки)")
        else:
            total = df.count()
            distinct = df.distinct().count()
            duplicates = total - distinct
            print(f"   Дублікатів: {duplicates}")
            if duplicates > 0:
                print(f"   → Видаляємо дублікати")
                dfs[name] = df.dropDuplicates()

    return dfs