"""Заготовка решения. Замените TODO своим кодом перед отправкой."""

import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window


def compare_df(left, right):
    """
    Сравнивает два датафрейма без учёта порядка строк.
    Возвращает True, если они совпадают, иначе False.
    """
    # 1. Порядок колонок может отличаться — приводим к одинаковому
    left_cols = sorted(left.columns)
    right_cols = sorted(right.columns)

    if left_cols != right_cols:
        return False

    left = left.select(*left_cols)
    right = right.select(*right_cols)

    # 2. Сравнение как множеств: влево минус вправо и вправо минус влево
    #    exceptAll корректно обрабатывает дубликаты и NULL
    diff1 = left.exceptAll(right)
    diff2 = right.exceptAll(left)

    # 3. Если обе разницы пусты — датафреймы равны
    return diff1.limit(1).count() == 0 and diff2.limit(1).count() == 0
