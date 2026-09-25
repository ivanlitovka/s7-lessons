"""Заготовка решения. Замените TODO своим кодом перед отправкой."""

import sys
import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def find_candidates(messages, verified_tags, suggested_cutoff):
    """Считает теги-кандидаты с фильтрацией по порогу и исключением verified_tags."""
    all_tags = messages \
        .where("event.message_channel_to is not null") \
        .selectExpr([
            "event.message_from as user",
            "explode(event.tags) as tag"
        ]) \
        .groupBy("tag") \
        .agg(F.expr("count(distinct user) as suggested_count")) \
        .where(f"suggested_count >= {suggested_cutoff}")
    
    candidates = all_tags.join(verified_tags, "tag", "left_anti")
    
    return candidates


def main():
    # 1. Получаем аргументы командной строки в порядке из задания
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    suggested_cutoff = int(sys.argv[3])
    base_input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    base_output_path = sys.argv[6]
    
    # 2. Создаём SparkSession
    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}") \
        .getOrCreate()
    
    # 3. Генерируем пути инлайн (без отдельной функции input_paths)
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    paths = [
        f"{base_input_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(days_count)
    ]
    
    # 4. Читаем данные
    messages = spark.read.parquet(*paths)
    verified_tags = spark.read.parquet(verified_tags_path)
    
    # 5. Вычисляем кандидатов
    candidates = find_candidates(messages, verified_tags, suggested_cutoff)
    
    # 6. Записываем результат с партицией date
    candidates \
        .withColumn("date", F.lit(date)) \
        .write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(base_output_path)


if __name__ == "__main__":
    main()