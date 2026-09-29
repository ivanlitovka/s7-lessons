#!/bin/sh
# TODO: добавьте команду запуска connection_interests.py и шесть аргументов
# в порядке из условия: дата, глубина, события, интересы, справочник, результат.
# Дату, глубину и пути для контрольного запуска возьмите из задания.
# submit.py отправляет этот файл как текст и не запускает команды.


spark-submit --master yarn --deploy-mode cluster connection_interests.py 2022-05-25 7 /user/prod/data/events /user/prod/data/analytics/user_interests_d7 /user/master/data/snapshots/tags_verified/actual /user/prod/data/analytics/connection_interests_d7