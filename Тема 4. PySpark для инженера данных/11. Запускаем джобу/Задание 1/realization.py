# Замените всё содержимое файла командой запуска из задания.
# Несмотря на расширение .py, сюда нужен текст команды оболочки, а не Python-код.
# Отправка: python submit.py (саму команду клиент не выполняет).
spark-submit --master yarn --num-executors 10 --deploy-mode cluster python_scripts.zip 2022-05-31
