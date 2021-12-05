from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""

# Вывод списка обработанных файлов с сортировкой по дате
# def select_all_from_source_files(connector: StoreConnector):
#     connector.start_transaction()  # начинаем выполнение запросов
#     # query = f'SELECT * FROM source_files ORDER BY processed'
#     query = f'SELECT * FROM processed_data ORDER BY LKG'
#     result = connector.execute(query).fetchall()
#     connector.end_transaction()  # завершаем выполнение запросов
#     return result

def select_all_from_suicides_country(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sc.id, country, ins.sex, gdp_per_capita, ing.level_gdp, suicides_no, population, ' \
            f'suicides_on_100KPopulation, source_file FROM suicides_country sc, index_gdp ing, index_sex ins ' \
            f'WHERE sc.Level_gdp = ing.id AND ins.id = sc.sex'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

def select_all_from_suicides_year(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sy.id, sy."year", ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  ' \
            f'FROM suicides_year sy, index_sex ins WHERE ins.id = sy.sex'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

def select_all_from_suicides_age(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT sa.id, sa.age, ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  ' \
            f'FROM suicides_age sa, index_sex ins WHERE ins.id = sa.sex '
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result
