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
def select_all_from_source_files(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT * FROM source_files ORDER BY processed'
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result


# Вставка в таблицу обработанных файлов
def insert_into_source_files(connector: StoreConnector, filename: str):
    now = datetime.now()  # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")  # преобразуем в формат SQL
    connector.start_transaction()
    query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    connector.end_transaction()
    return result


# Вставка строк из DataFrame в БД
def insert_rows_into_processed_data(connector: StoreConnector, list_result: list, filename: str):
    connector.start_transaction()
    foreign_key = connector.execute(f'SELECT MAX(id) FROM source_files').lastrowid  # Костыль ссаный
    row_country = list_result[0].to_dict('records')
    row_year = list_result[1].to_dict('records')
    row_age = list_result[2].to_dict('records')
    for row in row_country:
        connector.execute(f'INSERT INTO suicides_country (country, sex, gdp_per_capita, suicides_no, population, '
                          f'Suicides_on_100KPopulation, Level_gdp, source_file) VALUES (\'{row["country"]}\', '
                          f'\'{row["sex"]}\', \'{row["gdp_per_capita"]}\', \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{row["Level_gdp"]}\', '
                          f'\'{foreign_key}\')')
    for row in row_year:
        connector.execute(f'INSERT INTO suicides_year (year, sex, suicides_no, population, Suicides_on_100KPopulation, '
                          f'source_file) VALUES (\'{row["year"]}\', \'{row["sex"]}\',  \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{foreign_key}\')')
    for row in row_age:
        connector.execute(f'INSERT INTO suicides_age (age, sex, suicides_no, population, Suicides_on_100KPopulation, '
                          f'source_file) VALUES (\'{row["age"]}\', \'{row["sex"]}\',  \'{row["suicides_no"]}\','
                          f'\'{row["population"]}\', \'{row["Suicides/100KPopulation"]}\', \'{foreign_key}\')')
    connector.end_transaction()
