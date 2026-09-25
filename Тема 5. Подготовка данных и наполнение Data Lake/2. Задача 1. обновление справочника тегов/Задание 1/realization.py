import datetime


def input_paths(date, depth):
    """
    Возвращает список путей к датасетам с сообщениями за последние depth дней,
    начиная с даты date (включительно).
    
    :param date: строка с датой в формате 'yyyy-MM-dd'
    :param depth: глубина в днях (целое число)
    :return: список путей
    """
    # Парсим входную дату
    end_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    
    paths = []
    for i in range(depth):
        # Сдвигаемся назад на i дней
        current_date = end_date - datetime.timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        path = f"/user/s1414928/data/events/date={date_str}/event_type=message"
        paths.append(path)
    
    return paths
