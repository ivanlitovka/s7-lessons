"""user_interests.py
PySpark-джоба: топ-3 тега пользователя по постам, лайкам и дизлайкам.
Запуск:
    spark-submit --master yarn --deploy-mode cluster user_interests.py \
        <date> <days_count> <events_base_path> <output_base_path>
"""

import sys
import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def input_event_paths(base_path, date, depth):
    """Пути к событиям за depth дней до date включительно."""
    dt = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    return [
        f"{base_path}/date={dt - datetime.timedelta(days=i)}"
        for i in range(depth)
    ]


def _top_tags(df, user_col, prefix, ranks=3):
    """
    df: user_col, tag
    Возвращает wide: user_col, {prefix}_top_1..N.
    Ранжирование: cnt desc, tag asc (при равенстве — старше по алфавиту).
    """
    counts = df.groupBy(user_col, "tag").agg(F.count("*").alias("cnt"))

    w = Window.partitionBy(user_col).orderBy(
        F.col("cnt").desc(), F.col("tag").asc()
    )
    ranked = (
        counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= ranks)
    )

    wide = (
        ranked.groupBy(user_col)
        .pivot("rn", list(range(1, ranks + 1)))
        .agg(F.first("tag"))
    )
    for r in range(1, ranks + 1):
        wide = wide.withColumnRenamed(str(r), f"{prefix}_top_{r}")
    return wide


def tag_tops(posts):
    """posts: user_id, tag."""
    return _top_tags(posts, "user_id", "tag")


def reaction_tag_tops(posts_all, reactions):
    """
    posts_all: message_id, tag
    reactions: user_id, message_id, reaction_type
    """
    joined = reactions.join(posts_all, on="message_id", how="inner")

    likes = joined.filter(F.col("reaction_type") == "like")
    dislikes = joined.filter(F.col("reaction_type") == "dislike")

    like_tops = _top_tags(
        likes.select("user_id", "tag"), "user_id", "like_tag"
    )
    dislike_tops = _top_tags(
        dislikes.select("user_id", "tag"), "user_id", "dislike_tag"
    )
    return like_tops, dislike_tops


def calculate_user_interests(posts_part, posts_all, reactions):
    """Объединяет топы из постов и реакций в один датафрейм."""
    post_df = tag_tops(posts_part)
    like_df, dislike_df = reaction_tag_tops(posts_all, reactions)

    result = (
        post_df
        .join(like_df, on="user_id", how="full_outer")
        .join(dislike_df, on="user_id", how="full_outer")
    )

    return result.select(
        "user_id",
        "tag_top_1", "tag_top_2", "tag_top_3",
        "like_tag_top_1", "like_tag_top_2", "like_tag_top_3",
        "dislike_tag_top_1", "dislike_tag_top_2", "dislike_tag_top_3",
    )


def main():
    if len(sys.argv) != 5:
        print(
            "Usage: user_interests.py <date> <days_count> "
            "<events_base_path> <output_base_path>",
            file=sys.stderr,
        )
        sys.exit(1)

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    events_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    conf = SparkConf().setAppName(f"UserInterestsJob-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # ---- Чтение событий ----
    paths = input_event_paths(events_base_path, date, days_count)
    events = (
        sql.read
        .option("basePath", events_base_path)
        .parquet(*paths)
    )

    # ⬇⬇⬇ ЗДЕСЬ ВСЕ ОБРАЩЕНИЯ К ПОЛЯМ — ЧЕРЕЗ event.* ⬇⬇⬇
    posts = events.filter(F.col("event_type") == "post")
    reactions = events.filter(F.col("event_type") == "reaction")

    # message_id -> все теги этого сообщения
    posts_all = (
        posts.select(
            F.col("event.message_id").alias("message_id"),
            F.explode("event.tags").alias("tag"),
        )
        .dropDuplicates(["message_id", "tag"])
    )

    # посты авторов: user_id (= message_from) + все теги их message_id
    posts_part = (
        posts.select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_id").alias("message_id"),
        )
        .dropDuplicates()
        .join(posts_all, on="message_id", how="inner")
        .select("user_id", "tag")
    )

    # реакции: user_id (= reaction_from), message_id, reaction_type
    reactions_df = reactions.select(
        F.col("event.reaction_from").alias("user_id"),
        F.col("event.message_id").alias("message_id"),
        F.col("event.reaction_type").alias("reaction_type"),
    )

    # ---- Считаем и сохраняем ----
    result = calculate_user_interests(posts_part, posts_all, reactions_df)

    result.write.mode("overwrite").parquet(
        f"{output_base_path}/date={date}"
    )

    sc.stop()


if __name__ == "__main__":
    main()
