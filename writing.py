def save_results(dfs: dict, output_path: str):
    """
    Зберігає результати трансформацій у CSV файли.
    """
    from transformation import (
        q1_top10_rated_movies,
        q2_top_genres_by_rating,
        q3_top_directors,
        q4_movies_ranked_by_genre,
        q5_most_active_actors,
        q6_rating_by_decade
    )

    print("\n" + "="*60)
    print("💾 ЕТАП ЗАПИСУ РЕЗУЛЬТАТІВ")
    print("="*60)

    results = {
        "q1_top10_rated_movies":    q1_top10_rated_movies(dfs),
        "q2_top_genres_by_rating":  q2_top_genres_by_rating(dfs),
        "q3_top_directors":         q3_top_directors(dfs),
        "q4_movies_ranked_by_genre":q4_movies_ranked_by_genre(dfs),
        "q5_most_active_actors":    q5_most_active_actors(dfs),
        "q6_rating_by_decade":      q6_rating_by_decade(dfs),
    }

    for name, df in results.items():
        path = f"{output_path}/{name}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
        print(f"✅ Збережено: {path}")

    print("\n✅ Всі результати збережено!")