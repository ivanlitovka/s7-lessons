import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def input_event_paths(date, depth):
    """Пути к событиям за depth дней до date включительно."""
    dt = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    return [
        f"/user/master/data/events/date={dt - datetime.timedelta(days=i)}"
        for i in range(depth)
    ]


def tag_tops(date, depth, spark):
    """
    Топ-1..3 тегов в постах пользователя.
    Возвращает датафрейм: user_id, tag_top_1, tag_top_2, tag_top_3.
    """
    paths = input_event_paths(date, depth)
    events = spark.read.parquet(*paths)

    posts = events.filter(F.col("event_type") == "post")

    # message_id -> все теги этого сообщения (по всем постам с этим id)
    message_tags = (
        posts.select("message_id", F.explode("tags").alias("tag"))
        .dropDuplicates(["message_id", "tag"])
    )

    # user_id автора + все теги его постов (через message_id)
    author_tags = (
        posts.select("user_id", "message_id")
        .dropDuplicates()
        .join(message_tags, on="message_id", how="inner")
        .select("user_id", "tag")
    )

    # считаем частоту тега у пользователя
    counts = author_tags.groupBy("user_id", "tag").agg(
        F.count("*").alias("cnt"))

    # ранжирование: cnt desc, tag asc (при равенстве — старше по алфавиту)
    w = Window.partitionBy("user_id").orderBy(
        F.col("cnt").desc(), F.col("tag").asc()
    )
    ranked = (
        counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 3)
    )

    wide = (
        ranked.groupBy("user_id")
        .pivot("rn", [1, 2, 3])
        .agg(F.first("tag"))
        .withColumnRenamed("1", "tag_top_1")
        .withColumnRenamed("2", "tag_top_2")
        .withColumnRenamed("3", "tag_top_3")
    )
    return wide


def reaction_tag_tops(date, depth, spark):
    """
    Топ-1..3 тегов в лайках и дизлайках пользователя.
    Возвращает датафрейм: user_id, like_tag_top_1..3, dislike_tag_top_1..3.
    """
    paths = input_event_paths(date, depth)
    events = spark.read.parquet(*paths)

    posts = events.filter(F.col("event_type") == "post")
    reactions = events.filter(F.col("event_type") == "reaction")

    # message_id -> все теги этого сообщения
    message_tags = (
        posts.select("message_id", F.explode("tags").alias("tag"))
        .dropDuplicates(["message_id", "tag"])
    )

    reactions_with_tags = reactions.join(
        message_tags, on="message_id", how="inner"
    )

    def _top(df, prefix):
        counts = df.groupBy("user_id", "tag").agg(F.count("*").alias("cnt"))
        w = Window.partitionBy("user_id").orderBy(
            F.col("cnt").desc(), F.col("tag").asc()
        )
        ranked = (
            counts.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") <= 3)
        )
        wide = (
            ranked.groupBy("user_id")
            .pivot("rn", [1, 2, 3])
            .agg(F.first("tag"))
            .withColumnRenamed("1", f"{prefix}_top_1")
            .withColumnRenamed("2", f"{prefix}_top_2")
            .withColumnRenamed("3", f"{prefix}_top_3")
        )
        return wide

    likes = reactions_with_tags.filter(F.col("reaction_type") == "like")
    dislikes = reactions_with_tags.filter(F.col("reaction_type") == "dislike")

    like_tops = _top(likes.select("user_id", "tag"), "like_tag")
    dislike_tops = _top(dislikes.select("user_id", "tag"), "dislike_tag")

    return like_tops.join(dislike_tops, on="user_id", how="full_outer")


def calculate_user_interests(date, depth, spark):
    """Объединяет топы тегов из постов и реакций в один датафрейм."""
    post_df = tag_tops(date, depth, spark)
    reaction_df = reaction_tag_tops(date, depth, spark)

    result = post_df.join(reaction_df, on="user_id", how="full_outer")

    return result.select(
        "user_id",
        "tag_top_1", "tag_top_2", "tag_top_3",
        "like_tag_top_1", "like_tag_top_2", "like_tag_top_3",
        "dislike_tag_top_1", "dislike_tag_top_2", "dislike_tag_top_3",
    )


# ---- Сохранение результатов ----
calculate_user_interests('2022-04-04', 5, spark).write.mode("overwrite").parquet(
    '/user/s1414928/data/tmp/user_interests_04_04_5'
)
calculate_user_interests('2022-05-04', 5, spark).write.mode("overwrite").parquet(
    '/user/s1414928/data/tmp/user_interests_05_04_5'
)
calculate_user_interests('2022-04-04', 1, spark).write.mode("overwrite").parquet(
    '/user/s1414928/data/tmp/user_interests_04_04_1'
)
