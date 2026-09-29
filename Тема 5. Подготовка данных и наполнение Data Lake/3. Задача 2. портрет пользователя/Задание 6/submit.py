"""Отправка задания de07050306 в сервис проверок."""
from submit_client import submit
from pathlib import Path
import sys

# Корень s7-lessons; работает и при запуске из другого рабочего каталога.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

TASK_ID = 'de07050306'
ENDPOINT = '/api/v1/checks/de07050306_airflow_dag/'
DEFAULT_SOLUTION = 'dag.py'

if __name__ == '__main__':
    raise SystemExit(submit(TASK_ID, ENDPOINT, __file__, DEFAULT_SOLUTION))
