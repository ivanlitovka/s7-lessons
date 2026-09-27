"""Заготовка решения. Замените TODO своим кодом перед отправкой."""

import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def input_event_paths(date, depth):
    """Возвращает список путей к событиям за depth дней, начиная с date."""
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/user/s1414928/data/events/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(depth)
    ]


def tag_tops(date, depth, spark):
    """Возвращает датафрейм с топ-1..3 тегов в постах самого пользователя."""
    # 1. Получаем пути и читаем сообщения
    paths = input_event_paths(date, depth)
    messages = spark.read.parquet(*paths)

    # 2. Разворачиваем теги: user_id + tag
    tags = messages \
        .filter(F.col("event.message_channel_to").isNotNull()) \
        .filter(F.col("event.tags").isNotNull()) \
        .select(
            F.col("event.message_from").alias("user_id"),
            F.explode("event.tags").alias("tag")
        )

    # 3. Считаем количество появлений каждого тега у пользователя
    tag_counts = tags.groupBy("user_id", "tag").agg(
        F.count("*").alias("tag_count")
    )

    # 4. Ранжируем: count DESC, tag DESC (при равенстве — старше по алфавиту)
    window = Window.partitionBy("user_id").orderBy(
        F.col("tag_count").desc(),
        F.col("tag").desc()
    )
    ranked = tag_counts.withColumn("rank", F.row_number().over(window))

    # 5. Оставляем топ-3
    top3 = ranked.filter(F.col("rank") <= 3)

    # 6. Pivot: строки → колонки
    result = top3 \
        .groupBy("user_id") \
        .pivot("rank", [1, 2, 3]) \
        .agg(F.first("tag"))

    # 7. Переименовываем колонки
    result = result \
        .withColumnRenamed("1", "tag_top_1") \
        .withColumnRenamed("2", "tag_top_2") \
        .withColumnRenamed("3", "tag_top_3")

    return result
