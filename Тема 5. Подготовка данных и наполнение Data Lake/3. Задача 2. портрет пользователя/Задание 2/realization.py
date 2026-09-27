"""Заготовка решения. Замените TODO своим кодом перед отправкой."""

import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def input_event_paths(date, depth):
    """Возвращает список путей к событиям за depth дней, начиная с date."""
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/user/s1414928/data/events/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(depth)
    ]


def reaction_tag_tops(date, depth, spark):
    """Возвращает датафрейм с топ-1..3 тегов в лайкнутых и дизлайкнутых постах."""
    # 1. Читаем события за depth дней (все типы: message + reaction)
    paths = input_event_paths(date, depth)
    events = spark.read.parquet(*paths)

    # 2. Выделяем реакции: кто, что за реакция, на какое сообщение
    reactions = events \
        .filter(F.col("event_type") == "reaction") \
        .filter(F.col("event.reaction_type").isNotNull()) \
        .filter(F.col("event.message_id").isNotNull()) \
        .select(
            F.col("event.reaction_from").alias("user_id"),
            F.col("event.reaction_type").alias("reaction_type"),
            F.col("event.message_id").alias("message_id")
        )

    # 3. Выделяем сообщения: message_id + tag (explode)
    messages = events \
        .filter(F.col("event_type") == "message") \
        .filter(F.col("event.tags").isNotNull()) \
        .select(
            F.col("event.message_id").alias("message_id"),
            F.explode("event.tags").alias("tag")
        )

    # 4. Join реакций с сообщениями по message_id → user_id, reaction_type, tag
    reacted_tags = reactions.join(messages, "message_id", "inner")

    # 5. Считаем количество тегов у пользователя по типу реакции
    tag_counts = reacted_tags.groupBy("user_id", "reaction_type", "tag").agg(
        F.count("*").alias("tag_count")
    )

    # 6. Ранжируем внутри (user_id, reaction_type):
    #    count DESC, tag DESC (при равенстве — старше по алфавиту)
    window = Window.partitionBy("user_id", "reaction_type").orderBy(
        F.col("tag_count").desc(),
        F.col("tag").desc()
    )
    ranked = tag_counts.withColumn("rank", F.row_number().over(window))

    # 7. Оставляем топ-3
    top3 = ranked.filter(F.col("rank") <= 3)

    # 8. Pivot: строки → колонки по (reaction_type, rank)
    #    Сначала создаём уникальный ключ "like_1", "dislike_2" и т.д.
    top3 = top3.withColumn(
        "pivot_key",
        F.concat(F.col("reaction_type"), F.lit("_"), F.col("rank"))
    )

    pivot_values = [
        "like_1", "like_2", "like_3",
        "dislike_1", "dislike_2", "dislike_3"
    ]

    result = top3 \
        .groupBy("user_id") \
        .pivot("pivot_key", pivot_values) \
        .agg(F.first("tag"))

    # 9. Переименовываем колонки
    result = result \
        .withColumnRenamed("like_1", "like_tag_top_1") \
        .withColumnRenamed("like_2", "like_tag_top_2") \
        .withColumnRenamed("like_3", "like_tag_top_3") \
        .withColumnRenamed("dislike_1", "dislike_tag_top_1") \
        .withColumnRenamed("dislike_2", "dislike_tag_top_2") \
        .withColumnRenamed("dislike_3", "dislike_tag_top_3")

    return result
